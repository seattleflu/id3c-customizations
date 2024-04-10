"""
Deprecated, data collection ended

Process REDCap DETs that are specific to the Kiosk Enrollment Project.
"""
import logging
import re
from uuid import uuid4
from datetime import datetime
from typing import Any, List, Optional, Tuple, Dict
from cachetools import TTLCache
from id3c.db.session import DatabaseSession
from id3c.cli.redcap import Record as REDCapRecord
from id3c.cli.redcap import Project as REDCapProject
from id3c.cli.command.de_identify import generate_hash
from id3c.cli.command.geocode import get_geocoded_address
from id3c.cli.command.location import location_lookup
from id3c.cli.command.etl import redcap_det, UnknownSiteError
from seattleflu.id3c.cli.command import age_ceiling
from .redcap_map import *
from .fhir import *
from . import race, first_record_instance, required_instruments

LOG = logging.getLogger(__name__)

SFS = 'https://seattleflu.org'

REDCAP_URL = 'https://redcap.iths.org/'

PROJECT_ID = 16691

REQUIRED_INSTRUMENTS = [
    'screening',
    'main_consent_form',
    'enrollment_questionnaire'
]

# This revision number is stored in the processing_log of each REDCap DET
# record when the REDCap DET record is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# REDCap DET records lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all REDCap DET records,
# this revision number should be incremented.
REVISION = 6


@redcap_det.command_for_project(
    "kiosk",
    redcap_url = REDCAP_URL,
    project_id = PROJECT_ID,
    revision = REVISION,
    help = __doc__)

@first_record_instance
@required_instruments(REQUIRED_INSTRUMENTS)
def redcap_det_kiosk(*, db: DatabaseSession, cache: TTLCache, det: dict, redcap_record: REDCapRecord) -> Optional[dict]:
    if redcap_record['staff_name_uw'] == 'DEMO MODE' or redcap_record['staff_name_sch'] == 'DEMO MODE':
        LOG.warning("Skipping enrollment with staff name equal to `DEMO MODE`.")
        return None

    if redcap_record['language_questions'] == 'Spanish':
        LOG.info("Processing Spanish enrollment")
        redcap_record = overwrite_english_questionnaire_responses(redcap_record)

    patient_entry, patient_reference = create_patient(redcap_record)

    if not patient_entry:
        LOG.warning("Skipping enrollment with insufficient information to construct a patient")
        return None

    specimen_resource_entry, specimen_reference = create_specimen(redcap_record, patient_reference)

    if not specimen_resource_entry:
        LOG.warning("Skipping enrollment with insufficient information to construct a specimen")
        return None

    # Create diagnostic report resource if the participant agrees
    # to do the rapid flu test on site
    diagnostic_report_resource_entry = None
    if redcap_record['poc_yesno'] == 'Yes':

        diagnostic_code = create_codeable_concept(
            system = 'http://loinc.org',
            code = '85476-0',
            display = 'FLUAV and FLUBV and RSV pnl NAA+probe (Upper resp)'
        )

        diagnostic_report_resource_entry = create_diagnostic_report(
            redcap_record,
            patient_reference,
            specimen_reference,
            diagnostic_code,
            create_cepheid_result_observation_resource
        )

    encounter_locations = determine_encounter_locations(db, cache, redcap_record)
    location_resource_entries, location_references = create_locations(encounter_locations)

    symptom_resources, symptom_references = create_symptoms(
        redcap_record,
        patient_reference
    )

    encounter_resource_entry, encounter_reference = create_encounter(
        redcap_record,
        patient_reference,
        location_references,
        symptom_resources,
        symptom_references
    )

    if not encounter_resource_entry:
        LOG.warning("Skipping enrollment with insufficient information to construct an encounter")
        return None

    questionnaire_response_resource_entry = create_questionnaire_response_entry(
        redcap_record,
        patient_reference,
        encounter_reference
    )

    specimen_observation_resource_entry = create_specimen_observation_entry(
        specimen_reference,
        patient_reference,
        encounter_reference
    )

    all_resource_entries = [
        patient_entry,
        *location_resource_entries,
        encounter_resource_entry,
        questionnaire_response_resource_entry,
        specimen_resource_entry,
        specimen_observation_resource_entry,
    ]

    if diagnostic_report_resource_entry:
        all_resource_entries.append(diagnostic_report_resource_entry)

    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{REDCAP_URL}{PROJECT_ID}/{redcap_record['record_id']}",
        entries = list(filter(None, all_resource_entries))
    )


# FUNCTIONS SPECIFIC TO SFS KIOSK ENROLLMENT PROJECT
def overwrite_english_questionnaire_responses(redcap_record: Dict[str, Any]):
    overwritten_data = {k: v for k, v in redcap_record.items()}

    for key in redcap_record.keys():
        if key.startswith("s_"):
            overwritten_data[key.lstrip("s_")] = redcap_record[key]

    return REDCapRecord(project=REDCapProject(url=REDCAP_URL, project_id=PROJECT_ID), data=overwritten_data)


def create_patient(record: dict) -> tuple:
    """ Returns a FHIR Patient resource entry and reference. """
    gender = map_sex(record["sex_new"] or record["sex"])

    patient_id = generate_patient_hash(
        names       = participant_names(record),
        gender      = gender,
        birth_date  = record['birthday'],
        postal_code = participant_zipcode(record))

    if not patient_id:
        # Some piece of information was missing, so we couldn't generate a
        # hash.  Fallback to treating this individual as always unique by using
        # the REDCap record id.
        patient_id = generate_hash(f"{REDCAP_URL}{PROJECT_ID}/{record['record_id']}")

    LOG.debug(f"Generated individual identifier {patient_id}")

    patient_identifier = create_identifier(f"{SFS}/individual",patient_id)
    patient_resource = create_patient_resource([patient_identifier], gender)

    return create_entry_and_reference(patient_resource, "Patient")


def participant_names(redcap_record: dict) -> Tuple[str, ...]:
    """
    Extracts a tuple of names for the participant from the given
    *redcap_record*.
    """
    if redcap_record['participant_first_name']:
        return (redcap_record['participant_first_name'], redcap_record['participant_last_name'])
    else:
        return (redcap_record['part_name_sp'],)


def participant_zipcode(redcap_record: dict) -> str:
    """
    Extract the home zipcode for the participant from the given
    *redcap_record*.

    If no zipcode could be found for the participant, then it returns
    «{PROJECT_ID}-{record_id}»
    """
    if redcap_record.get('home_zipcode'):
        return redcap_record['home_zipcode']

    elif redcap_record.get('home_zipcode_notus'):
        return redcap_record['home_zipcode_notus']

    elif redcap_record.get('shelter_name') and redcap_record['shelter_name'] != 'Other/none of the above':
        address = determine_shelter_address(redcap_record['shelter_name'])
        return address['zipcode']

    elif redcap_record.get('uw_dorm') and redcap_record['uw_dorm'] != 'Other':
        address = determine_dorm_address(redcap_record['uw_dorm'])
        return address['zipcode']

    return None


def determine_vaccine_date(vaccine_year: str, vaccine_month: str) -> Optional[str]:
    """
    Determine date of vaccination and return in datetime format as YYYY or
    YYYY-MM
    """
    if vaccine_year == '' or vaccine_year == 'Do not know':
        return None

    if vaccine_month == '' or vaccine_month == 'Do not know':
        return datetime.strptime(vaccine_year, '%Y').strftime('%Y')

    return datetime.strptime(f'{vaccine_month} {vaccine_year}', '%B %Y').strftime('%Y-%m')


def create_specimen(redcap_record: dict, patient_reference: dict) -> tuple:
    """
    Create FHIR specimen resource entry and reference from given *redcap_record*
    """
    sfs_sample_barcode = get_sfs_barcode(redcap_record)

    if not sfs_sample_barcode:
        return None, None

    specimen_identifier = create_identifier(f"{SFS}/sample", sfs_sample_barcode)

    specimen_type = 'NSECR'  # Nasal swab.  TODO we may want shared mapping function
    specimen_resource = create_specimen_resource(
        [specimen_identifier], patient_reference, specimen_type
    )

    return create_entry_and_reference(specimen_resource, "Specimen")


def get_sfs_barcode(redcap_record: dict) -> str:
    """
    Find SFS barcode within *redcap_record*.

    SFS barcode should be scanned into `sfs_barcode`, but if the scanner isn't
    working then barcode will be manually entered into `sfs_barcode_manual`
    """
    barcode = redcap_record['sfs_barcode']

    if barcode == '':
        barcode = redcap_record['sfs_barcode_manual']

    return barcode


def create_cepheid_result_observation_resource(redcap_record: dict) -> List[Observation]:
    """
    Determine the cepheid results based on responses in *redcap_record* and
    create observation resources for each result following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/observation.html)
    """
    code_map = {
        'Influenza A +': {
            'system': 'http://snomed.info/sct',
            'code': '181000124108',
            'display': 'Influenza A virus present'
        },
        'Influenza B +': {
            'system': 'http://snomed.info/sct',
            'code': '441345003',
            'display': 'Influenza B virus present'
        },
        'RSV +': {
            'system': 'http://snomed.info/sct',
            'code': '441278007',
            'display': 'Respiratory syncytial virus untyped strain present'
        },
        'Inconclusive': {
            'system': 'http://snomed.info/sct',
            'code': '911000124104',
            'display': 'Virus inconclusive'
        }
    }

    cepheid_results = find_selected_options('cepheid_results___', redcap_record)

    # Create observation resources for all potential results in Cepheid test
    diagnostic_results: dict[str, Observation] = {}
    for index, result in enumerate(code_map):
        new_observation = observation_resource('Cepheid')
        new_observation['id'] = 'result-' + str(index+1)
        new_observation['code']['coding'] = [code_map[result]]
        diagnostic_results[result] = (new_observation)

    # Mark all results as False if not positive for anything
    if "Not positive for anything" in cepheid_results:
        for result in diagnostic_results:
            diagnostic_results[result]['valueBoolean'] = False

    # Mark inconclusive as True and all other results as False if inconclusive
    elif "Inconclusive" in cepheid_results:
        for result in diagnostic_results:
            if result == 'Inconclusive':
                diagnostic_results[result]['valueBoolean'] = True
            else:
                diagnostic_results[result]['valueBoolean'] = False

    else:
        for result in diagnostic_results:
            if result in cepheid_results:
                diagnostic_results[result]['valueBoolean'] = True
                cepheid_results.remove(result)
            else:
                diagnostic_results[result]['valueBoolean'] = False

        if len(cepheid_results) != 0:
            raise UnknownCepheidResultError(f"Unknown Cepheid result «{cepheid_results}»")

    return list(diagnostic_results.values())


def create_locations(encounter_locations: dict) -> tuple:
    """
    Create FHIR location resources and reference from a given *redcap_record*
    """
    location_resource_entries = []
    location_references = []
    for location in encounter_locations:
        # Locations related to encounter site only needs a logical reference
        # since we expect site to already exist within ID3C warehouse.site
        if location == 'site':
            if not encounter_locations['site']:
                return [], None

            location_reference = create_reference(
                reference_type = 'Location',
                identifier = {
                    'system': f'{SFS}/site',
                    'value': encounter_locations['site']
                }
            )

        else:
            location_fullUrl = encounter_locations[location]['fullUrl']
            location_id = encounter_locations[location]['value']
            scale = 'tract' if location.endswith('-tract') else 'address'
            location_identifier = create_identifier(
                system = f'{SFS}/location/{scale}',
                value = location_id
            )

            # Only create partOf if location is an address to reference the
            # related tract Location resource.
            part_of = None

            # Only create a literal location reference for the Encounter if
            # the location is an address
            location_reference = None

            if scale == 'address':
                address_tract = f'{location}-tract'
                # Check that the address has corresponding census tract
                assert address_tract in encounter_locations, \
                    f'Found address without census-tract for {location}'

                tract_fullUrl = encounter_locations[address_tract]['fullUrl']
                part_of = create_reference(
                    reference_type = 'Location',
                    reference = tract_fullUrl
                )
                location_reference = create_reference(
                    reference_type = 'Location',
                    reference = location_fullUrl
                )

            location_resource = create_location_resource(
                location_type = [determine_location_type_code(location)],
                location_identifier = [location_identifier],
                location_partOf = part_of
            )

            location_resource_entries.append(create_resource_entry(
                resource = location_resource,
                full_url = location_fullUrl
            ))

        if location_reference:
            location_references.append({'location': location_reference})

    return location_resource_entries, location_references


def determine_encounter_locations(db: DatabaseSession, cache: TTLCache, redcap_record: dict) -> dict:
    """
    Find all locations within a *redcap_record* that are relevant to
    an encounter
    """
    locations = {
        'site': determine_site_name(redcap_record)
    }

    def construct_location(db: DatabaseSession, cache: TTLCache, lat_lng: Tuple[int, int],
        canonicalized_address: Any, location_type: str) -> dict:
        return ({
            f'{location_type}-tract': {
                'value': location_lookup(db, lat_lng, 'tract').identifier,  # TODO what if null?
                'fullUrl': generate_full_url_uuid()
            },
            location_type: {
                'value': generate_hash(canonicalized_address),
                'fullUrl': generate_full_url_uuid()
            }
        })

    address: Dict[str, str] = {}
    if redcap_record['shelter_name'] and redcap_record['shelter_name'] != 'Other/none of the above':
        address = determine_shelter_address(redcap_record['shelter_name'])
        housing_type = 'lodging'

    elif redcap_record['uw_dorm'] and redcap_record['uw_dorm'] != 'Other':
        address = determine_dorm_address(redcap_record['uw_dorm'])
        housing_type = 'residence'

    elif redcap_record['home_street'] or redcap_record['home_street_optional']:
        address = determine_home_address(redcap_record)
        housing_type = 'residence'

    if address:
        lat, lng, canonicalized_address = get_geocoded_address(address, cache)

        if canonicalized_address:
            locations.update(construct_location(db, cache, (lat, lng), canonicalized_address, housing_type))

    return locations


def determine_site_name(redcap_record: dict) -> Optional[str]:
    """
    Given a *redcap_record*, determine the site name for the encounter.

    Will error if there is more than one site name found or if the site
    name is not in expected values.
    """
    potential_site_names = find_selected_options('site_identifier_', redcap_record)
    if not potential_site_names:
        return None

    # Check only one site identifier is selected
    assert len(potential_site_names) == 1, \
        f"More than one site name found: «{potential_site_names}»"

    site = potential_site_names[0]

    site_name_map = {
        'UW HUB': 'HUB',
        'UW Suzzallo Library': 'UWSuzzalloLibrary',
        'SeaMar': 'UWSeaMar',
        'UW Hall Health': 'UWHallHealth',
        "Seattle Children's: Seattle Children's campus site": 'ChildrensHospitalSeattle',
        "Seattle Children's: Seattle outpatient clinic": 'ChildrensHospitalSeattleOutpatientClinic',
        'Fred Hutch': 'FredHutchLobby',
        'Harborview Lobby': 'HarborviewLobby',
        'Columbia Center': 'ColumbiaCenter',
        'Seattle Center': 'SeattleCenter',
        'Westlake Center': 'WestlakeCenter',
        'King Street Station': 'KingStreetStation',
        'Westlake Light Rail Station': 'WestlakeLightRailStation',
        'CapitolHillLightRailStation': 'CapitolHillLightRailStation',
        'Capitol Hill Light Rail Station': 'CapitolHillLightRailStation',
        "St. Martin's": 'StMartins',
    }

    if site not in site_name_map:
        raise UnknownSiteError(f"Unknown site name «{site}»")

    return site_name_map[site]


def determine_shelter_address(shelter_name: str) -> dict:
    """
    Return address for a *shelter_name*
    """
    shelter_map = {
        "Aloha Inn": "1911 Aurora Ave N,98109",
        "Blaine Center Homeless Ministry": "150 Denny Way,98109",
        "Bread of Life Mission": "97 S Main St,98104",
        "Compass Housing Alliance":	"77 S Washington St,98104",
        "DESC (Downtown Emergency Service Center)":	"515 3rd Ave,98104",
        "Elizabeth Gregory House": "1604 NE 50th St,98105",
        "Hammond House Women's Shelter": "302 N 78th St,98103",
        "Jubilee Women's Center": "620 18th Ave E,98112",
        "King County Men's Winter Shelter":	"500 4th Avenue,98104",
        "Mary's Place": "1155 N 130th St,98133",
        "Mary's Place North Seattle": "1155 N 130th St,98133",
        "Mary's Place White Center": "10821 8th Ave SW,98146",
        "Mary's Place Burien": "12845 Ambaum Blvd. SW,Burien,WA,98146",
        "Noel House Women's Referral Center": "118 Bell St,98121",
        "Pike Market Senior Center": "85 Pike St #200,98101",
        "Roots Young Adult Shelter": "1415 NE 43rd St,98105",
        "Sacred Heart Shelter": "232 Warren Ave N,98109",
        "Saint Martin de Porres Shelter": "1516 Alaskan Way S,98134",
        "Salvation Army Women's Shelter": "1101 Pike St,98101",
        "Seattle City Hall Shelter": "600 4th Ave,98104",
        "Seattle Union Gospel Mission for Men": "318 2nd Ave Ext S,98104",
        "YMCA Emergency Shelter": "1025 E Fir St,98122"
    }

    if shelter_name not in shelter_map:
        raise UnknownShelterError(f"Unknown shelter name «{shelter_name}»")

    if shelter_name == "Mary's Place Burien":
        street, city, state, zipcode = shelter_map[shelter_name].split(',')
        return construct_address_dict(street, city, state, zipcode)

    street, zipcode = shelter_map[shelter_name].split(',')

    return construct_address_dict(street, 'Seattle', 'WA', zipcode)


def determine_dorm_address(dorm_name: str) -> dict:
    """
    Return address for a *dorm_name*
    """
    dorm_map = {
        "Alder Hall": "1315 NE Campus Parkway,98105",
        "Cedar Apartments":	"1128 NE 41st St,98105",
        "Elm Hall":	"1218 NE Campus Parkway,98105",
        "Haggett Hall":	"4290 Whitman Ct NE,98195",
        "Hansee Hall": "4294 Whitman Ln NE,98195",
        "Lander Hall": "1201 NE Campus Parkway,98105",
        "Madrona Hall":	"4320 Whitman Ln NE,98195",
        "Maple Hall": "1135 NE Campus Parkway,98105",
        "McCarty Hall":	"2100 NE Whitman Ln,98195",
        "McMahon Hall":	"4200 Whitman Ct. NE,98195",
        "Mercer Court Apartments": "3925 Adams Ln NE,98105",
        "Poplar Hall":	"1302 NE Campus Parkway,98105",
        "Stevens Court Apartments": "3801 Brooklyn Ave NE,98105",
        "Terry Hall": "1035 NE Campus Parkway,98105",
        "Willow Hall": "4294 Whitman Ln NE,98195",
    }

    if dorm_name not in dorm_map:
        raise UnknownDormError(f"Unknown dorm name «{dorm_name}»")

    street, zipcode = dorm_map[dorm_name].split(',')

    return construct_address_dict(street, 'Seattle', 'WA', zipcode)


def construct_address_dict(street: str,
                           city: str,
                           state: str,
                           zipcode: str) -> dict:
    """
    Construct an address dict for Seatte, WA specific addresses with provided
    *street* and *zipcode*
    """
    return ({
        'street': street,
        'secondary': None,
        'city': city,
        'state': state,
        'zipcode': zipcode
    })


def determine_home_address(redcap_record: dict) -> dict:
    """
    Parse a home address from a given REDCap *redcap_record* and return as a dict
    with each address field.
    """
    if redcap_record['home_street'] != '':
        street = redcap_record['home_street']
    else:
        street = redcap_record['home_street_optional']

    # City and State
    if redcap_record['seattle_home'] == 'Seattle':
        city = 'Seattle'
        state = 'WA'
    else:
        city = redcap_record['homecity_other']
        state = redcap_record['home_state']

    # Zip Code
    zipcode = redcap_record['home_zipcode']

    return construct_address_dict(street, city, state, zipcode)


def determine_location_type_code(location_type: str) -> dict:
    """
    Given an ID3C *location_type*, return the location type codeable concept
    using FHIR codes
    (http://www.hl7.org/implement/standards/fhir/v3/ServiceDeliveryLocationRoleType/vs.html)
    """
    location_type_system = 'http://terminology.hl7.org/CodeSystem/v3-RoleCode'

    type_map = {
        'site': 'HUSCS',
        'work': 'WORK',
        'residence': 'PTRES',
        'residence-tract': 'PTRES',
        'lodging': 'PTLDG',
        'lodging-tract': 'PTLDG',
        'school': 'SCHOOL'
    }

    return create_codeable_concept(location_type_system, type_map[location_type])


def create_symptoms(redcap_record: dict, patient_reference: dict) -> tuple:
    """
    Create FHIR condition resources and references for symptoms selected in
    given *redcap_record*
    """
    symptom_codes = determine_symptoms_codes(redcap_record)

    if not symptom_codes:
        return None, None

    # YYYY-MM-DD in REDCap
    symptom_onset = redcap_record.get('symptom_duration')

    symptom_resources = []
    symptom_references = []
    for symptom in symptom_codes:
        condition_resource = create_condition_resource(
            condition_id = symptom,
            patient_reference = patient_reference,
            onset_datetime = symptom_onset,
            condition_code = symptom_codes[symptom]['code'],
            severity = symptom_codes[symptom]['severity_code']
        )
        condition_reference = create_reference(
            reference_type = 'Condition',
            reference = '#' + symptom
        )
        symptom_resources.append(condition_resource)
        symptom_references.append({
            'condition': condition_reference
        })

    return symptom_resources, symptom_references


def determine_symptoms_codes(redcap_record: dict) -> Optional[dict]:
    """
    Given a *redcap_record*, determine the symptoms of the encounter
    """
    symptom_responses = find_selected_options('symptoms___', redcap_record)

    severity_map = {
        'Feeling feverish': 'fever_severity',
        'Cough': 'cough_severity',
        'Muscle or body aches': 'ache_severity',
        'Feeling more tired than usual': 'fatigue_severity',
        'Sore throat or itchy/scratchy throat': 'sorethroat_severity',
        'Headaches': 'headache_severity',
        'Chills or shivering': 'chills_severity',
        'Sweats': 'sweats_severity',
        'Nausea or vomiting': 'nausea_severity',
        'Runny / stuffy nose': 'nose_severity',
        'Increased trouble with breathing': 'breathing_severity',
        'Diarrhea': 'diarrhea_severity',
        'Ear pain or ear discharge': 'ear_severity',
        'Rash': 'rash_severity'
    }

    symptom_codes = {}

    for response in symptom_responses:
        symptom = map_symptom(response)

        if not symptom:
            return None

        symptom_code = create_codeable_concept(
            system = f'{SFS}/symptom',
            code = symptom,
            display = symptom
        )

        symptom_codes[symptom] = {
            'code': symptom_code,
            'severity_code': None
        }

        if severity_map.get(response) and redcap_record.get(severity_map.get(response)):
            severity = redcap_record[severity_map.get(response)]
            symptom_codes[symptom]['severity_code'] = create_condition_severity_code(
                condition_severity = severity
            )

    return symptom_codes


def create_encounter(redcap_record: REDCapRecord,
                     patient_reference: dict,
                     location_references: List[dict],
                     symptom_resources: Optional[List[Condition]],
                     symptom_references: Optional[List[dict]]) -> tuple:
    """
    Create FHIR encounter resource and encounter reference from given
    *redcap_record*.
    """
    encounter_id = f"{REDCAP_URL}{PROJECT_ID}/{redcap_record['record_id']}"
    enrollment_date = redcap_record.get('enrollment_date')

    if not enrollment_date:
        return None, None

    # YYYY-MM-DD HH:MM in REDCap
    encounter_date = enrollment_date.split()[0]

    encounter_identifier = create_identifier(
        system = f'{SFS}/encounter',
        value = encounter_id
    )

    encounter_class = create_coding(
        system = 'http://terminology.hl7.org/CodeSystem/v3-ActCode',
        code = 'FLD'
    )

    encounter_resource = create_encounter_resource(
        encounter_source = create_redcap_uri(redcap_record),
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class,
        encounter_date = encounter_date,
        patient_reference = patient_reference,
        location_references = location_references,
        diagnosis = symptom_references,
        contained = symptom_resources
    )

    return create_entry_and_reference(encounter_resource, "Encounter")


def determine_all_questionnaire_items(redcap_record: dict) -> List[dict]:
    """
    Given a *redcap_record*, determine answers for all core questions
    """
    items: Dict[str, Any] = {}

    if redcap_record['age']:
        items['age'] = [{ 'valueInteger': age_ceiling(int(redcap_record['age'])) }]
        items['age_months'] = [{ 'valueInteger': int(age_ceiling(float(redcap_record['age_months']) / 12) * 12) }]

    # Spanish language answers may exist anywhere below here.

    items['travel_countries'] = [{'valueBoolean': (redcap_record['travel_countries'] == 'Yes' or redcap_record['travel_countries'] == 'Sí')}]
    items['travel_states'] = [{'valueBoolean': (redcap_record['travel_states'] == 'Yes' or redcap_record['travel_states'] == 'Sí')}]

    if redcap_record['acute_symptom_onset']:
        items['acute_symptom_onset'] = [{ 'valueString': spanish_to_english_mapper(redcap_record['acute_symptom_onset'], 'acute_symptom_onset')}]

    # Participant can select multiple insurance types, so create
    # a separate answer for each selection
    insurance_responses = find_selected_options('insurance___', redcap_record)
    if insurance_responses:
        items['insurance'] = [{'valueString': spanish_to_english_mapper(insurance, 'insurance')} for insurance in insurance_responses]

    # Participant can select multiple races, so create
    # a separate answer for each selection
    race_responses = find_selected_options('race___', redcap_record)
    if 'Prefer not to say' not in race_responses:
        race_responses = [spanish_to_english_mapper(race, 'race') for race in race_responses]
        races = race(race_responses)
        items['race'] = [{'valueString': race} for race in races]

    if redcap_record['hispanic'] != 'Prefer not to say':
        items['ethnicity'] = [{'valueBoolean': (redcap_record['hispanic'] == 'Yes' or redcap_record['hispanic'] == "Sí")}]

    if redcap_record['education']:
        items['education'] = [{'valueString': spanish_to_english_mapper(redcap_record['education'], 'education')}]

    if redcap_record['income_levels']:
        items['income'] = [{'valueString': spanish_to_english_mapper(redcap_record['income_levels'], 'income_levels')}]

    if redcap_record['housing_type']:
        items['housing_type'] = [{'valueString': spanish_to_english_mapper(redcap_record['housing_type'], 'housing_type')}]

    if redcap_record['house_members']:
        items['house_members'] = [{'valueString': spanish_to_english_mapper(redcap_record['house_members'], 'house_members')}]

    if redcap_record['antiviral_1']:
        items['antiviral_1'] = [{'valueString': spanish_to_english_mapper(redcap_record['antiviral_1'], 'antiviral_1')}]

    child_age_responses = find_selected_options("age_children___", redcap_record)
    if child_age_responses:
        items['age_children'] = [{'valueString': spanish_to_english_mapper(age_response, 'age_children')} for age_response in child_age_responses]

    if redcap_record['regular_activities_0']:
        items['regular_activities_0'] = [{'valueString': spanish_to_english_mapper(redcap_record['regular_activities_0'], 'regular_activities_0')}]

    school_interference_responses = find_selected_options("school_interference_0___", redcap_record)
    if school_interference_responses:
        items['school_interference_0'] = [{'valueString': spanish_to_english_mapper(interference, 'school_interference_0')} for interference in school_interference_responses]

    items['child_dayvare'] = [{'valueBoolean': (redcap_record['child_daycare'] == 'Yes' or redcap_record['child_daycare'] == 'Sí')}]

    # Participant can select multiple smoking types, so create
    # a separate answer for each selection
    smoke_responses = find_selected_options('smoke___', redcap_record)
    if smoke_responses:
        items['smoke'] = [{'valueString': spanish_to_english_mapper(smoke, 'smoke')} for smoke in smoke_responses]

    # Participant can select multiple chronic illnesses, so create
    # a separate answer for each selection
    chronic_illness = find_selected_options('chronic_illness___', redcap_record)
    if smoke_responses:
        items['chronic_illness'] = [{'valueString': map_chronic_illness(spanish_to_english_mapper(illness, 'illness'))} for illness in chronic_illness]

    # Only include vaccine status if known
    vaccine_status = map_vaccine(spanish_to_english_mapper(redcap_record['vaccine'], 'vaccine'))
    # Only include vaccine status if known
    if vaccine_status is not None:
        items['vaccine'] = [{ 'valueBoolean': vaccine_status }]
        immunization_date = determine_vaccine_date(
            vaccine_year = spanish_to_english_mapper(redcap_record['vaccine_year'], 'vaccine_year'),
            vaccine_month = spanish_to_english_mapper(redcap_record['vaccine_month'], 'vaccine_month')
        )
        if vaccine_status and immunization_date:
            items['vaccine'].append({ 'valueDate': immunization_date })

    response_items = []
    for item in items:
        response_items.append(create_questionnaire_response_item(
            question_id = item,
            answers = items[item]
        ))

    # fu
    if redcap_record['doctor_1week']:
        items['doctor_1week'] = [{'valueString': spanish_to_english_mapper(redcap_record['doctor_1week'], 'doctor_1week')}]

    activity_responses = find_selected_options("activities_impacted_poc___", redcap_record)
    if activity_responses:
        items['activities_impacted_poc'] = [{'valueString': spanish_to_english_mapper(activity, 'activities_impacted_poc')} for activity in activity_responses]

    work_impact_responses = find_selected_options('work_impact_2poc___', redcap_record)
    if work_impact_responses:
        items['work_impact_2poc'] = [{'valueString': spanish_to_english_mapper(impact, 'work_impact_2poc')} for impact in work_impact_responses]

    return response_items


def determine_insurance_type(insurance_reseponse: str) -> str:
    """
    Determine the insurance type based on a given *insurance_response*
    """
    insurance_map = {
        'Private (provided by employer and/or purchased)': 'privateInsurance',
        'Government (Medicare/Medicaid)': 'government',
        'Other': 'other',
        'None': 'none',
        'Prefer not to say': 'preferNotToSay'
    }

    def standardize_insurance(insurance):
        try:
            return insurance_map[insurance]
        except KeyError:
            raise UnknownInsuranceError(f'Unknown insurance response «{insurance}»') from None

    return standardize_insurance(insurance_reseponse)


def create_questionnaire_response_entry(redcap_record: dict,
                                        patient_reference: dict,
                                        encounter_reference: dict) -> Optional[dict]:
    """
    Ceeate a questionnaire response entry based on given *redcap_record* and
    link to *patient_refernece* and *encounter_reference*
    """
    questionnaire_items = determine_all_questionnaire_items(redcap_record)

    if not questionnaire_items:
        return None

    questionnaire_response_resource = create_questionnaire_response_resource(
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        items = questionnaire_items
    )

    return (create_resource_entry(
        resource = questionnaire_response_resource,
        full_url = generate_full_url_uuid()
    ))


def find_selected_options(option_prefix: str, redcap_record:dict) -> list:
    """
    Find all choosen options within *redcap_record* where option begins with
    provided *option_prefix*.

    Note: Values of options not choosen are empty strings.
    """
    return [
        value
        for key, value
        in redcap_record.items()
        if key.startswith(option_prefix) and value
    ]


# XXX A quick and dirty mapping set to handle potentially spanish language values in variables.
def spanish_to_english_mapper(value: str, field: str):

    if field == 'acute_symptom_onset':
        return map_spanish_symptom_onset_to_english(value)
    elif field == 'insurance':
        return map_spanish_insurance_to_english(value)
    elif field == 'race':
        return map_spanish_race_to_english(value)
    elif field == 'education':
        return map_spanish_education_to_english(value)
    elif field == 'income_levels':
        return map_spanish_income_levels_to_english(value)
    elif field == 'housing_type':
        return map_spanish_housing_type_to_english(value)
    elif field == 'house_members':
        return map_spanish_house_members_to_english(value)
    elif field == 'antiviral_1':
        return map_spanish_antiviral_1_to_english(value)
    elif field == 'smoke':
        return map_spanish_smoke_to_english(value)
    elif field == 'illness':
        return map_spanish_illness_to_english(value)
    elif field == 'vaccine':
        return map_spanish_vaccine_to_english(value)
    elif field == 'vaccine_year':
        return map_spanish_vaccine_year_to_english(value)
    elif field == 'vaccine_month':
        return map_spanish_vaccine_month_to_english(value)
    elif field == 'age_children':
        return map_spanish_age_children_to_english(value)
    elif field == 'regular_activities_0':
        return map_spanish_regular_activity_to_english(value)
    elif field == 'school_interference_0':
        return map_spanish_school_interference_to_english(value)

    # fu
    elif field == 'activities_impacted_poc':
        return map_spanish_impacted_activity_to_english(value)
    elif field == 'work_impact_2poc':
        return map_spanish_work_impact_to_english(value)
    elif field == 'doctor_1week':
        return map_spanish_doctor_follow_up_to_english(value)
    else:
        raise UnknownMappedField(f"Unknown Spanish to English mapped field name «{field}»")


def map_spanish_symptom_onset_to_english(to_map):
    mapper = {
        'Medio día': 'Half a day',
        'Medio día a 1 día': 'Half a day - 1 day',
        '1 a 1.5 días': '1 - 1.5 days',
        '1.5 a 2 días': '1.5 - 2 days',
        '3 días': '3 days',
        '4 días': '4 days',
        '5 o más días': '5 or more days',
    }

    def standardize_symptom_onset(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownSymptomOnsetError(f'Unknown acute symptom onset response «{to_map}»') from None

    return standardize_symptom_onset(to_map)


def map_spanish_insurance_to_english(to_map):
    mapper = {
        'Privado (proporcionado por el empleador y/o comprado)': 'Private (provided by employer and/or purchased)',
        'Gubernamental (Medicare/Medicaid)': 'Government (Medicare/Medicaid)',
        'Otra': 'Other',
        'Ninguno': 'None',
        'Prefiero no decir': 'Prefer not to say',
    }

    def standardize_insurance(to_map):
        if to_map in mapper.values():
            return determine_insurance_type(to_map)

        try:
            return determine_insurance_type(mapper[to_map])
        except KeyError:
            raise UnknownInsuranceValueError(f'Unknown insurance response «{to_map}»') from None

    return standardize_insurance(to_map)


def map_spanish_race_to_english(to_map):
    mapper = {
        'Indio americano o nativo de Alaska': 'American Indian or Alaska Native',
        'Asiático': 'Asian',
        'Nativo de Hawái o de otra isla del Pacífico': 'Native Hawaiian or other Pacific Islander',
        'Negro o afroamericano': 'Black or African American',
        'Blanco': 'White',
        'Otra': 'Other',
        'Prefiero no decir': 'Prefer not to say',
    }

    def standardize_race(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownRaceError(f'Unknown race response «{to_map}»') from None

    return standardize_race(to_map)


def map_spanish_education_to_english(to_map):
    mapper = {
        "No acabé la preparatoria (high school)": "Less than high school graduate",
        "Terminé la preparatoria (high school) / Obtuve mi GED": "Graduated high school/obtained GED",
        "Algunos estudios universitarios (incluida la formación vocacional, título de dos años)": "Some college (including vocational training, associate's degree)",
        "Licenciatura": "Bachelor's degree",
        "Título avanzado": "Advanced degree",
        "Prefiero no decir": "Prefer not to say",
    }

    def standardize_education(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownEducationError(f'Unknown education response «{to_map}»') from None

    return standardize_education(to_map)


def map_spanish_income_levels_to_english(to_map):
    mapper = {
        'Menos que o igual a $25,000': 'Less than or equal to $25,000',
        'Entre veinticinco y cincuenta mil dolares ($25,001 a $50,000)': 'Between $25 and 50 thousand ($25,001 to $50,000)',
        'Entre cincuenta y setenta y cinco mil dolares ($50,001 a $75,000)': 'Between $50 and 75 thousand ($50,001 to $75,000)',
        'Entre setenta y cinco y cien mil dolares ($75,001 a $100,000)': 'Between $75 and 100 thousand ($75,001 to $100,000)',
        'Entre cien mil y cien y veinticinco mil dolares ($100,001 a $125,000)': 'Between $100 and 125 thousand ($100,001 to $125,000)',
        'Entre cien y veinticinco mil y cien y cincuenta mil dolares ($125,001 a $150,000)': 'Between $125 and 150 thousand ($125,001 to $150,000)',
        'Más que $150,000': 'Over $150,000',
        'No lo sé': "Don't know",
        'Prefiero no decir': 'Prefer not to say',
    }

    def standardize_income_level(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownIncomeError(f'Unknown income levels response «{to_map}»') from None

    return standardize_income_level(to_map)


def map_spanish_housing_type_to_english(to_map):
    mapper = {
        'Casa/condominio/casa adosada': 'House/condo/townhouse',
        'Refugio': 'Shelter',
        'Apartamento': 'Apartment',
        'Dormitorio': 'Dormitory',
        'Centro de vivienda asistida': 'Assisted living facility',
        'Centro de enfermería especializada': 'Skilled nursing center',
        'Sin residencia principal regular': 'No consistent primary residence',
        'Otra': 'Other',
    }

    def standardize_housing_type(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownHousingTypeError(f'Unknown housing type response «{to_map}»') from None

    return standardize_housing_type(to_map)


def map_spanish_house_members_to_english(to_map):
    mapper = {
        'Vivo solo': 'I live by myself',
        '2 personas': '2 people',
        '3 personas': '3 people',
        '4 personas': '4 people',
        '5 personas': '5 people',
        '6 o más personas': '6 or more people',
    }

    def standardize_house_members(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownHousingMembersError(f'Unknown house members response «{to_map}»') from None

    return standardize_house_members(to_map)


def map_spanish_antiviral_1_to_english(to_map):
    mapper = {
        'No': 'No',
        'Sí; Oseltamivir (Tamiflu)': 'Yes; Oseltamivir (Tamiflu)',
        'Sí; Zanamivir (Relenza)': 'Yes; Zanamivir (Relenza)',
        'Sí; Peramivir (Rapivab)': 'Yes; Peramivir (Rapivab)',
        'Sí; Baloxavir (Xofluza)': 'Yes; Baloxavir (Xofluza)',
        'Sí, pero no sé qué medicamento': "Yes, but I don't know which medication",
        'No lo sé': 'Do not know',
    }

    def standardize_antiviral(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownAntiviralError(f'Unknown antiviral response «{to_map}»') from None

    return standardize_antiviral(to_map)


def map_spanish_smoke_to_english(to_map):
    mapper = {
        'Productos de tabaco (p. ej. cigarrillos, puros, pipas)': 'Tobacco products (e.g. cigarettes, cigars, pipes)',
        'Cigarrillos electrónicos/bolígrafos de vapor': 'Electronic cigarettes/vapor pens',
        'Ninguna de las anteriores respuestas': 'None of the above',
        'Prefiero no decir': 'Prefer not to say',
    }

    def standardize_smoke(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownSmokeError(f'Unknown smoke response «{to_map}»') from None

    return standardize_smoke(to_map)


def map_spanish_illness_to_english(to_map):
    mapper = {
        "Asma o enfermedad reactiva de las vías respiratorias": "Asthma or reactive airway disease",
        "COPD/enfisema": "COPD/emphysema",
        "Bronquitis crónica": "Chronic bronchitis",
        "Cáncer": "Cancer",
        "Diabetes": "Diabetes",
        "Enfermedad cardíaca (insuficiencia cardíaca o ataque cardíaco)": "Heart disease (heart failure or heart attack)",
        "Ninguna de estas afecciones": "None of these conditions",
        "No lo sé": "Do not know",
        "Prefiero no decir": "Prefer not to say",
    }

    def standardize_illness(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownIllnessError(f'Unknown illness response «{to_map}»') from None

    return standardize_illness(to_map)


def map_spanish_vaccine_to_english(to_map):
    mapper = {
        'Sí': 'Yes',
        'No': 'No',
        'No lo sé': 'Do not know',
    }

    def standardize_vaccine(to_map):
        if not to_map:
            return ''

        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownVaccineError(f'Unknown vaccine response «{to_map}»') from None

    return standardize_vaccine(to_map)


def map_spanish_vaccine_year_to_english(to_map):
    mapper = {
        '2019': '2019',
        '2020': '2020',
        'No lo sé': 'Do not know',
    }

    def standardize_vaccine_year(to_map):
        if not to_map:
            return ''

        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownVaccineYearError(f'Unknown vaccine year response «{to_map}»') from None

    return standardize_vaccine_year(to_map)


def map_spanish_vaccine_month_to_english(to_map):
    mapper = {
        'Enero': 'January',
        'Febrero': 'February',
        'Marzo': 'March',
        'Abril': 'April',
        'Mayo': 'May',
        'Junio': 'June',
        'Julio': 'July',
        'Agosto': 'August',
        'Septiembre': 'September',
        'Octubre': 'October',
        'Noviembre': 'November',
        'Diciembre': 'December',
        'No lo sé': 'Do not know',
    }

    def standardize_vaccine_month(to_map):
        if not to_map:
            return ''

        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownVaccineMonthError(f'Unknown vaccine month response «{to_map}»') from None

    return standardize_vaccine_month(to_map)


def map_spanish_age_children_to_english(to_map):
    mapper = {
        'No hay niños': 'No children',
        '0-5 años': 'Age 0-5 years',
        '6-12 años': 'Age 6-12 years',
        '13-18 años': 'Age 13-18 years',
    }

    def standardize_age_children(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownChildAgeError(f'Unknown child age response «{to_map}»') from None

    return standardize_age_children(to_map)


def map_spanish_doctor_follow_up_to_english(to_map):
    mapper = {
        "Sí - Consultorio médico o atención de urgencia": "Yes - Doctor's office or Urgent Care",
        "Sí - Farmacia": "Yes - Pharmacy (drugstore)",
        "Sí - Hospital o departamento de emergencias": "Yes - Hospital or Emergency Department",
        "Si - Otro": "Yes - Other", # sic
        "No": "No",
    }

    def standardize_doctor_follow_up(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownDoctorFollowUpError(f'Unknown doctor follow up response «{to_map}»') from None

    return standardize_doctor_follow_up(to_map)


def map_spanish_regular_activity_to_english(to_map):
    mapper = {
        "Nada": "Not at all",
        "Un poco": "A little bit",
        "Algo": "Somewhat",
        "Bastante": "Quite a bit",
        "Mucho": "Very much",
    }

    def standardize_regular_activity(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownRegularActivityError(f'Unknown regular activity response «{to_map}»') from None

    return standardize_regular_activity(to_map)


def map_spanish_impacted_activity_to_english(to_map):
    mapper = {
        "Escuela": "School",
        "Trabajo": "Work",
        "Hacer mandados": "Running errands",
        "Hacer ejercicio": "Exercising",
        "Socializar": "Socializing",
        "Trabajar como voluntario": "Volunteering",
        "Capacidad para cuidarme o cuidar a mi familia": "Ability to take care of myself and/or family",
        "Ninguna de las anteriores/ mis actividades no se han visto afectadas": "None of the above / my activities have not been impacted",
    }

    def standardize_impacted_activity(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownImpactedActivityError(f'Unknown impacted activity response «{to_map}»') from None

    return standardize_impacted_activity(to_map)


def map_spanish_school_interference_to_english(to_map):
    mapper = {
        "Asistir a clases": "Attending class",
        "Ir a trabajar": "Going to work",
        "Estudiar": "Studying",
        "Sacar buenas calificaciones en un examen o tarea de redacción": "Performing well on an exam or written assignment",
        "Ninguna de las anteriores/ mis actividades no se han visto afectadas": "None of the above/ my activities have not been impacted",
    }

    def standardize_school_interference(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownSchoolInterferenceError(f'Unknown school interference response «{to_map}»') from None

    return standardize_school_interference(to_map)


def map_spanish_work_impact_to_english(to_map):
    mapper = {
        "Falté al trabajo": "I missed work",
        "Trabajé desde casa": "I worked from home",
        "Trabajé menos horas de lo habitual": "I worked fewer hours than usual",
        "Ninguna de las anteriores respuestas": "None of the above",
    }

    def standardize_work_impact(to_map):
        if to_map in mapper.values():
            return to_map

        try:
            return mapper[to_map]
        except KeyError:
            raise UnknownWorkImpactError(f'Unknown work impact response «{to_map}»') from None

    return standardize_work_impact(to_map)


class UnknownMappedField(ValueError):
    """
    Raised by :function: `spanish_to_english_mapper` if a provided
    *field* is not among a set of expected values
    """
    pass


class UnknownSymptomOnsetError(ValueError):
    """
    Raised by :function: `map_spanish_symptom_onset_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownInsuranceValueError(ValueError):
    """
    Raised by :function: `map_spanish_insurance_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownRaceError(ValueError):
    """
    Raised by :function: `map_spanish_race_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownEducationError(ValueError):
    """
    Raised by :function: `map_spanish_education_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownIncomeError(ValueError):
    """
    Raised by :function: `map_spanish_income_levels_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownHousingTypeError(ValueError):
    """
    Raised by :function: `map_spanish_housing_type_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownHousingMembersError(ValueError):
    """
    Raised by :function: `map_spanish_house_members_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownAntiviralError(ValueError):
    """
    Raised by :function: `map_spanish_antiviral_1_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownSmokeError(ValueError):
    """
    Raised by :function: `map_spanish_smoke_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownIllnessError(ValueError):
    """
    Raised by :function: `map_spanish_illness_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownVaccineError(ValueError):
    """
    Raised by :function: `map_spanish_vaccine_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownVaccineYearError(ValueError):
    """
    Raised by :function: `map_spanish_vaccine_year_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownVaccineMonthError(ValueError):
    """
    Raised by :function: `map_spanish_vaccine_month_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownChildAgeError(ValueError):
    """
    Raised by :function: `map_spanish_age_children_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownDoctorFollowUpError(ValueError):
    """
    Raised by :function: `map_spanish_doctor_follow_up_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownRegularActivityError(ValueError):
    """
    Raised by :function: `map_spanish_regular_activity_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownImpactedActivityError(ValueError):
    """
    Raised by :function: `map_spanish_impacted_activity_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownSchoolInterferenceError(ValueError):
    """
    Raised by :function: `map_spanish_school_interference_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownWorkImpactError(ValueError):
    """
    Raised by :function: `map_spanish_work_impact_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownChildDaycareError(ValueError):
    """
    Raised by :function: `map_spanish_child_daycare_to_english` if a provided
    *to_map* field is not among a set of expected values
    """
    pass


class UnknownInsuranceError(ValueError):
    """
    Raised by :function: `determine_insurance_type` if a provided
    *insurance_response* is not among a set of expected values
    """
    pass


class UnknownCepheidResultError(ValueError):
    """
    Raised by :function: `determine_cepheid_results` if a provided
    result response is not among a set of expected values
    """
    pass


class UnknownShelterError(ValueError):
    """
    Raised by :function: `determine_shelter_address` if a provided shelter name
    is not among a set of expected values
    """
    pass


class UnknownDormError(ValueError):
    """
    Raised by :function: `determine_dorm_address` if a provided dorm name is
    not among a set of expected values
    """
    pass
