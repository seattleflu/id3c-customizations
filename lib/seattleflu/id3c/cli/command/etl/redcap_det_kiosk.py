"""
Process REDCap DETs that are specific to the Kiosk Enrollment Project.
"""
import click
import logging
import re
import requests
from uuid import uuid4
from datetime import datetime, timezone
from typing import Any, List, Optional
from copy import deepcopy
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from id3c.cli.command import with_database_session
from id3c.cli.command.clinical import generate_hash
from id3c.cli.command.etl import race, UnknownSiteError
from id3c.cli.command.etl.redcap_det import (
    redcap_det,
    mark_skipped,
    mark_loaded,
    get_redcap_record,
    insert_fhir_bundle,
    is_complete
)

LOG = logging.getLogger(__name__)

SFS = 'https://seattleflu.org'

REDCAP_URL = 'redcap.iths.org'

PROJECT_ID = "16691"

INSTRUMENTS_COMPLETE = [
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
REVISION = 1
ETL_NAME = 'redcap-det kiosk'
ETL_ID = {
    'revision': REVISION,
    'etl': ETL_NAME
}


@redcap_det.command("kiosk", help = __doc__)
@with_database_session


def redcap_det_kisok(*, db: DatabaseSession):
    LOG.debug(f"Starting the REDCap DET ETL routine for kiosks, revision {REVISION}")

    redcap_det = db.cursor("REDCap DET")
    redcap_det.execute("""
        select redcap_det_id as id, document
          from receiving.redcap_det
         where not processing_log @> %s
          and document->>'project_id' = %s
         order by id
           for update
        """, (Json([{ "revision": REVISION }]), PROJECT_ID))


    for det in redcap_det:
        with db.savepoint(f"redcap_det {det.id}"):
            LOG.info(f"Processing REDCap DET {det.id}")

            instrument = det.document['instrument']
            # Only pull REDCap record if the instrument is marked complete
            if not is_complete(instrument, det.document):
                LOG.debug(f"Skipping incomplete or unverified REDCap DET {det.id}")
                mark_skipped(db, det.id, ETL_ID)
                continue

            redcap_record = get_redcap_record(det.document['record'])

            # Only process REDCap record if all required instruments are marked complete
            if any(not is_complete(instrument, redcap_record) for instrument in INSTRUMENTS_COMPLETE):
                LOG.debug(f"One of the required instruments «{INSTRUMENTS_COMPLETE}» has not been completed. " + \
                          f"Skipping REDCap DET {det.id}")
                mark_skipped(db, det.id, ETL_ID)
                continue

            patient_resource_entry, patient_reference = create_patient(redcap_record)

            immunization_resource_entry = create_immunization(redcap_record, patient_reference)

            specimen_reference = create_specimen(redcap_record)

            # Create diagnostic report resource if the participant agrees
            # to do the rapid flu test on site
            diagnostic_report_resource_entry = None
            if redcap_record['poc_yesno'] == 'Yes':
                diagnostic_report_resource_entry = create_diagnostic_report(
                    redcap_record,
                    patient_reference,
                    specimen_reference
                )

            encounter_locations = determine_encounter_locations(redcap_record)
            location_resource_entries, location_references = create_locations(encounter_locations)

            symptom_resources, symptom_references = create_symptoms(
                redcap_record,
                patient_reference
            )

            encounter_id = '/'.join([REDCAP_URL, PROJECT_ID, redcap_record['record_id']])
            encounter_resource_entry, encounter_reference = create_encounter(
                encounter_id,
                redcap_record,
                patient_reference,
                location_references,
                symptom_resources,
                symptom_references
            )

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
                patient_resource_entry,
                immunization_resource_entry,
                *location_resource_entries,
                encounter_resource_entry,
                questionnaire_response_resource_entry,
                specimen_observation_resource_entry
            ]

            if diagnostic_report_resource_entry:
                all_resource_entries.append(diagnostic_report_resource_entry)

            bundle = create_bundle_resource(
                bundle_id = str(uuid4()),
                timestamp = datetime.now().astimezone().isoformat(),
                entries = all_resource_entries)

            insert_fhir_bundle(db, bundle)

            mark_loaded(db, det.id, ETL_ID)


# CREATE FHIR RESOURCES
def create_reference(reference_type: str,
                     reference = None,
                     identifier = None) -> dict:
    """
    Create a reference resource following the FHIR format
    (https://www.hl7.org/fhir/references.html)
    """
    reference_resource = {
        'type': reference_type
    }

    if reference:
        reference_resource['reference'] = reference

    if identifier:
        reference_resource['identifier'] = identifier

    return reference_resource


def create_patient_resource(patient_identifier: List[dict], gender: str) -> dict:
    """
    Create patient resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/patient.html)
    """
    return ({
        'resourceType': 'Patient',
        'identifier': patient_identifier,
        'gender': gender
    })


def create_immunization_resource(vaccine_code: dict,
                                 patient_reference: dict,
                                 status: str,
                                 occurrence: dict) -> dict:
    """
    Create immunization resource following the FHIR format
    (https://www.hl7.org/fhir/immunization.html)
    """
    return ({
        'resourceType': 'Immunization',
        'vaccineCode': vaccine_code,
        'patient': patient_reference,
        'status': status,
        **occurrence
    })


def create_diagnostic_report_resource(datetime: str,
                                      diagnostic_code: dict,
                                      patient_reference: dict,
                                      specimen_reference: dict,
                                      result: list,
                                      contained = None) -> dict:
    """
    Create diagnostic report resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/diagnosticreport.html)
    """
    diagnostic_report = {
        'resourceType': 'DiagnosticReport',
        'status': 'final',
        'effectiveDateTime': datetime,
        'specimen': [ specimen_reference ],
        'code': diagnostic_code,
        'subject': patient_reference,
        'result': result,
    }

    if contained:
        diagnostic_report['contained'] = contained

    return diagnostic_report


def create_condition_resource(condition_id: str,
                              patient_reference: dict,
                              onset_datetime: str,
                              condition_code: dict,
                              severity = None) -> dict:
    """
    Create condition resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/condition.html)
    """
    condition_resource = {
        'resourceType': 'Condition',
        'id': condition_id,
        'subject': patient_reference,
        'onsetDateTime': onset_datetime,
        'code': condition_code,
    }

    if severity:
        condition_resource['severity'] = severity

    return condition_resource


def create_condition_severity_code(condition_severity: str) -> dict:
    """
    Create a condition severity codeable concept following the FHIR format
    and FHIR value set
    (http://www.hl7.org/implement/standards/fhir/valueset-condition-severity.html)
    """
    severity_code_system = 'http://snomed.info/sct'
    severity = {
        'Mild': '255604002',
        'Moderate': '6736007',
        'Severe': '24484000'
    }

    return (
        create_codeable_concept(
            system = severity_code_system,
            code = severity[condition_severity],
            display = condition_severity
        )
    )


def create_location_resource(location_type: List[dict],
                             location_identifier: List[dict],
                             location_partOf: Optional[dict]) -> dict:
    """
    Create location resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/location.html)
    """
    location_resource = {
        'resourceType': 'Location',
        'mode': 'instance',
        'type': location_type,
        'identifier': location_identifier,
    }

    if location_partOf:
        location_resource['partOf'] = location_partOf

    return location_resource


def create_encounter_resource(encounter_identifier: List[dict],
                              encounter_class: dict,
                              start_timestamp: str,
                              patient_reference: dict,
                              location_references: List[dict],
                              diagnosis: Optional[List[dict]],
                              contained: Optional[List[dict]]) -> dict:
    """
    Create encounter resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/encounter.html)
    """
    encounter_resource = {
        'resourceType': 'Encounter',
        'class': encounter_class,
        'identifier': encounter_identifier,
        'status': 'finished',
        'period': {
            'start': start_timestamp
        },
        'subject': patient_reference,
        'location': location_references
    }

    if diagnosis:
        encounter_resource['diagnosis'] = diagnosis
    if contained:
        encounter_resource['contained'] = contained

    return encounter_resource


def create_specimen_observation(specimen_reference: dict,
                                patient_reference: dict,
                                encounter_reference: dict) -> dict:
    """
    Create an observation resource that is a links a specimen, a patient, and
    an encounter. Follows the FHIR format
    (http://www.hl7.org/implement/standards/fhir/observation.html)
    """
    return ({
        'resourceType': 'Observation',
        'status': 'final',
        'code': {
            'coding': [
                {
                    'system': 'http://loinc.org',
                    'code': '89873-4',
                    'display': 'Unique ID Initial sample'
                }
            ]
        },
        'encounter': encounter_reference,
        'subject': patient_reference,
        'specimen': specimen_reference
    })


def create_questionnaire_response_resource(patient_reference: dict,
                                           encounter_reference: dict,
                                           items: List[dict]) -> dict:
    """
    Create a questionnaire response resource following the FHIR format
    (https://www.hl7.org/fhir/questionnaireresponse.html)
    """
    return ({
        'resourceType': 'QuestionnaireResponse',
        'status': 'completed',
        'subject': patient_reference,
        'encounter': encounter_reference,
        'item': items
    })


def create_questionnaire_response_item(question_id: str,
                                       answers: List[dict]) -> dict:
    """
    Create a questionnaire response answer item following the FHIR format
    (https://www.hl7.org/fhir/questionnaireresponse-definitions.html#QuestionnaireResponse.item)
    """
    return ({
        'linkId': question_id,
        'answer': answers
    })


def create_bundle_resource(bundle_id: str,
                           timestamp: str,
                           entries: List[dict]) -> dict:
    """
    Create bundle resource containing other resources following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/bundle.html)
    """
    return ({
        'resourceType': 'Bundle',
        'type': 'collection',
        'id': bundle_id,
        'timestamp': timestamp,
        'entry': entries
    })


def create_resource_entry(resource: dict, full_url: str) -> dict:
    """
    Create bundle entry that contains a *resource* and a *full_url*.
    """
    return ({
        'resource': resource,
        'fullUrl': full_url
    })


# CREATE FHIR DATA TYPES
def create_codeable_concept(system: str, code: str, display = None) -> dict:
    """
    Create codeable concept following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/datatypes.html#CodeableConcept)
    """
    coding = {
        'system': system,
        'code': code
    }

    if display:
        coding['display'] = display

    return ({
        'coding': [coding]
    })


def create_identifier(system: str, value: str) -> dict:
    """
    Create an identifier data type following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/datatypes.html#Identifier)
    """
    return ({
        'system': system,
        'value': value
    })


def generate_full_url_uuid() -> str:
    """
    Create a fullUrl following FHIR format that represents a UUID.
    (http://www.hl7.org/implement/standards/fhir/bundle-definitions.html#Bundle.entry.fullUrl)
    """
    return f'urn:uuid:{uuid4()}'


# FUNCTIONS SPECIFIC TO SFS KIOSK ENROLLMENT PROJECT
def create_patient(redcap_record: dict) -> tuple:
    """
    Create FHIR patient resource and reference from *redcap_record*
    """
    gender = determine_gender(redcap_record['sex'])

    patient_id = generate_patient_id(redcap_record, gender)

    patient_identifier = create_identifier(
        system = f'{SFS}/individual',
        value = patient_id
    )

    patient_resource = create_patient_resource([patient_identifier], gender)

    patient_resource_entry = create_resource_entry(
        resource = patient_resource,
        full_url = generate_full_url_uuid()
    )

    patient_reference = create_reference(
        reference_type = 'Patient',
        identifier = patient_identifier
    )

    return patient_resource_entry, patient_reference


def determine_gender(sex_response: str) -> Optional[str]:
    """
    Determine the gender based on a give *sex_response*
    """
    if sex_response == '':
        return None

    gender_map = {
        'Male': 'male',
        'Female': 'female',
        'Indeterminate/other': 'other',
        'Prefer not to say': None
    }

    if sex_response not in gender_map:
        raise UnknownSexError(f'Unknown sex response «{sex_response}»')

    return gender_map[sex_response]


def generate_patient_id(redcap_record: dict, gender: str) -> str:
    """
    Create a hashed patient id for a given *redcap_record*
    """
    patient = {
        'gender': gender
    }

    full_name = None

    if redcap_record['participant_first_name'] != '':
        full_name = ' '.join([
            redcap_record['participant_first_name'],
            redcap_record['participant_last_name']
        ])
    else:
        full_name = redcap_record['part_name_sp']

    if full_name:
        patient['name'] = canonicalize_name(full_name)

    if redcap_record['birthday']:
        patient['birthday'] = datetime \
            .strptime(redcap_record['birthday'], '%Y-%m-%d') \
            .isoformat()

    if redcap_record.get('home_zipcode'):
        patient['zipcode'] = redcap_record['home_zipcode']
    elif redcap_record.get('home_zipcode_notus'):
        patient['zipcode'] = redcap_record['home_zipcode_notus']
    elif redcap_record.get('shelter_name') and redcap_record['shelter_name'] != 'Other/none of the above':
        address = determine_shelter_address(redcap_record['shelter_name'])
        patient['zipcode'] = address['zipcode']
    elif redcap_record.get('uw_dorm') and redcap_record['uw_dorm'] != 'Other':
        address = determine_dorm_address(redcap_record['uw_dorm'])
        patient['zipcode'] = address['zipcode']

    return generate_hash(''.join(sorted(patient.values())))


def canonicalize_name(full_name: str) -> str:
    """ """
    return re.sub(r'\s*[\d\W]+\s*', ' ', full_name).upper()


def create_immunization(redcap_record: dict, patient_reference: dict) -> dict:
    """
    Create FHIR immunization resource from *redcap_record* and link to a
    specific *patient_reference*
    """
    immunization_status = determine_vaccine_status(redcap_record['vaccine'])

    immunization_date = determine_vaccine_date(
        vaccine_year = redcap_record['vaccine_year'],
        vaccine_month = redcap_record['vaccine_month']
    )

    if immunization_date:
        immunization_occurrence = {
            'occurrenceDateTime': immunization_date
        }
    else:
        immunization_occurrence = {
            'occurrenceString': 'Unknown'
        }

    vaccine_code = create_codeable_concept(
        system = 'http://snomed.info/sct',
        code = '46233009',
        display = 'Influenza virus vaccine'
    )

    immunization_resource = create_immunization_resource(
        vaccine_code = vaccine_code,
        patient_reference = patient_reference,
        status = immunization_status,
        occurrence = immunization_occurrence
    )

    return (create_resource_entry(
        resource = immunization_resource,
        full_url = generate_full_url_uuid()
    ))


def determine_vaccine_status(vaccine_response: str) -> Optional[str]:
    """
    Determine the vaccine status based on provided *vaccine_response*
    """
    if vaccine_response == '':
        return None

    vaccine_map = {
        'Yes': 'completed',
        'No': 'not-done',
        'Do not know': None
    }

    if vaccine_response not in vaccine_map:
        raise UnknownVaccineResponse(f"Unknown vaccine response «{vaccine_response}»")

    return vaccine_map[vaccine_response]


def determine_vaccine_date(vaccine_year: str, vaccine_month: str) -> Optional[str]:
    """
    Determine date of vaccination and return in datetime format as YYYY or
    YYYY-MM
    """
    if vaccine_year == '' or vaccine_year == 'Do not know':
        return None

    if vaccine_month == 'Do not know':
        return datetime.strptime(vaccine_year, '%Y').strftime('%Y')

    return datetime.strptime(f'{vaccine_month} {vaccine_year}', '%B %Y').strftime('%Y-%m')


def create_specimen(redcap_record: dict) -> dict:
    """
    Create FHIR specimen reference from given *redcap_record*
    """
    sfs_sample_barcode = get_sfs_barcode(redcap_record)

    return (create_reference(
        reference_type = 'Specimen',
        identifier = create_identifier(SFS, sfs_sample_barcode)
    ))


def get_sfs_barcode(redcap_record: dict):
    """
    Find SFS barcode within *redcap_record*.

    SFS barcode should be scanned into `sfs_barcode`, but if the scanner isn't
    working then barcode will be manually entered into `sfs_barcode_manual`
    """
    barcode = redcap_record['sfs_barcode']

    if barcode == '':
        barcode = redcap_record['sfs_barcode_manual']

    return barcode


def create_diagnostic_report(redcap_record:dict,
                             patient_reference: dict,
                             specimen_reference: dict) -> dict:
    """
    Create FHIR diagnostic report from given *redcap_record* and link to
    specific *patient_reference* and *specimen_reference*
    """
    cepheid_results = create_cepheid_result_observation_resource(redcap_record)

    diagnostic_result_references = []

    for result in cepheid_results:
        reference = create_reference(
            reference_type = 'Observation',
            reference = '#' + result['id']
        )
        diagnostic_result_references.append(reference)

    collection_datetime = datetime\
        .strptime(redcap_record['collection_date'], '%Y-%m-%d %H:%M:%S')\
        .strftime('%Y-%m-%dT%H:%M:%S')

    diagnostic_code = create_codeable_concept(
        system = 'http://loinc.org',
        code = '85476-0',
        display = 'FLUAV and FLUBV and RSV pnl NAA+probe (Upper resp)'
    )

    diagnostic_report_resource = create_diagnostic_report_resource(
        datetime = collection_datetime,
        diagnostic_code = diagnostic_code,
        patient_reference  = patient_reference,
        specimen_reference = specimen_reference,
        result = diagnostic_result_references,
        contained = cepheid_results
    )

    return (create_resource_entry(
        resource = diagnostic_report_resource,
        full_url = generate_full_url_uuid()
    ))


def create_cepheid_result_observation_resource(redcap_record: dict) -> List[dict]:
    """
    Determine the cepheid results based on responses in *redcap_record* and
    create observation resources for each result following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/observation.html)
    """
    # XXX TODO: Define this as a TypedDict when we upgrade from Python 3.6 to
    # 3.8.  Until then, there's no reasonable way to type this data structure
    # better than Any.
    #   -trs, 24 Oct 2019
    observation_resource: Any = {
        'resourceType': 'Observation',
        'id': '',
        'status': 'final',
        'code': {
            'coding': []
        },
        'valueBoolean': None,
        'device': create_reference(
            reference_type = 'Device',
            identifier = create_identifier(
                system = f'{SFS}/device',
                value = 'Cepheid'
            )
        )
    }

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
    diagnostic_results = {}
    for index, result in enumerate(code_map):
        new_observation = deepcopy(observation_resource)
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
                diagnostic_results[result]['valueBoolean'] == True
                cepheid_results.remove(result)
            else:
                diagnostic_results[result]['valueBoolean'] == False

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
        # Only create location resources for locations not related
        # to site of encounter since the site can just be a location
        # reference
        if location != 'site':
            location_fullUrl = encounter_locations[location]['fullUrl']
            location_identifier = encounter_locations[location]['value']
            scale = 'tract' if location.endswith('-tract') else 'address'
            part_of = None

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

            location_identifier = create_identifier(
                system = f'{SFS}/location/{scale}',
                value = location_identifier
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

            location_reference = create_reference(
                reference_type = 'Location',
                reference = location_fullUrl
            )

        else:
            location_reference = create_reference(
                reference_type = 'Location',
                identifier = {
                    'system': f'{SFS}/site',
                    'value': encounter_locations['site']
                }
            )
        location_references.append({'location': location_reference})

    return location_resource_entries, location_references


def determine_encounter_locations(redcap_record: dict) -> dict:
    """
    Find all locations within a *redcap_record* that are relevant to
    an encounter
    """
    locations = {
        'site': determine_site_name(redcap_record)
    }

    def construct_location(address: dict, location_type: str) -> dict:
        return ({
            f'{location_type}-tract': {
                'value': determine_census_tract(address),
                'fullUrl': generate_full_url_uuid()
            },
            location_type: {
                'value': 'address_hash', #TODO: hash address
                'fullUrl': generate_full_url_uuid()
            }
        })

    if redcap_record['shelter_name'] and redcap_record['shelter_name'] != 'Other/none of the above':
        shelter_address = determine_shelter_address(redcap_record['shelter_name'])
        locations.update(construct_location(shelter_address, 'lodging'))

    elif redcap_record['uw_dorm'] and redcap_record['uw_dorm'] != 'Other':
        dorm_address = determine_dorm_address(redcap_record['uw_dorm'])
        locations.update(construct_location(dorm_address, 'residence'))

    elif redcap_record['home_street'] or redcap_record['home_street_optional']:
        home_address = determine_home_address(redcap_record)
        locations.update(construct_location(home_address, 'residence'))

    return locations


def determine_site_name(redcap_record: dict) -> str:
    """
    Given a *redcap_record*, determine the site name for the encounter.

    Will error if there is more than one site name found or if the site
    name is not in expected values.
    """
    potential_site_names = find_selected_options('site_identifier_', redcap_record)

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
        "Seattle Children's: Seattle outpatient clinic": 'ChildrensHospitalSeattle',
        'Fred Hutch': 'FredHutchLobby',
        'Harborview Lobby': 'HarborviewLobby',
        'Columbia Center': 'ColumbiaCenter',
        'Seattle Center': 'SeattleCenter',
        'Westlake Center': 'WestlakeCenter',
        'King Street Station': 'KingStreetStation',
        'Westlake Light Rail Station': 'WestlakeLightRailStation',
        'CapitolHillLightRailStation': 'CapitolHillLightRailStation'
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
        'city': city,
        'state': state,
        'zipcode': zipcode
    })


def determine_home_address(redcap_record: dict) -> dict:
    """
    Parse a home address from a given REDCap *redcap_record* and return as a dict
    with each address field.
    """
    address = {}
    # Street
    if redcap_record['home_street'] != '':
        address['street'] = redcap_record['home_street']
    else:
        address['street'] = redcap_record['home_street_optional']

    # City and State
    if redcap_record['seattle_home'] == 'Seattle':
        address['city'] = 'Seattle'
        address['state'] = 'WA'
    else:
        address['city'] = redcap_record['homecity_other']
        address['state'] = redcap_record['home_state']

    # Zip Code
    address['zipcode'] = redcap_record['home_zipcode']

    return address


def determine_census_tract(address: dict) -> str:
    """
    Given an *address* return census tract for address
    """
    # TODO: geocoding *address*
    return 'census_tract'


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

    symptom_onset = datetime \
        .strptime(redcap_record['symptom_duration'], "%Y-%m-%d") \
        .strftime('%Y-%m-%d')

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


def determine_symptoms_codes(redcap_record: dict) -> Optional[list]:
    """
    Given a *redcap_record*, determine the symptoms of the encounter
    """
    symptom_responses = find_selected_options('symptoms___', redcap_record)

    if 'None of the above' in symptom_responses:
        return None

    symptom_map = {
        'Feeling feverish':                     'feelingFeverish',
        'Headache':                             'headaches',
        'Headaches':                             'headaches',
        'Cough':                                'cough',
        'Chills or shivering':                  'chillsOrShivering',
        'Sweats':                               'sweats',
        'Sore throat or itchy/scratchy throat': 'soreThroat',
        'Nausea or vomiting':                   'nauseaOrVomiting',
        'Runny or stuffy nose':                 'runnyOrStuffyNose',
        'Runny / stuffy nose':                 'runnyOrStuffyNose',
        'Feeling more tired than usual':        'fatigue',
        'Muscle or body aches':                 'muscleOrBodyAches',
        'Diarrhea':                             'diarrhea',
        'Ear pain or ear discharge':                   'earPainOrDischarge',
        'Rash':                                 'rash',
        'Increased trouble with breathing':     'increasedTroubleBreathing'
    }

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
        symptom = symptom_map.get(response)

        if not symptom:
            raise UnknownSymptomResponseError(f"Unknown symptom response «{response}»")

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


def create_encounter(encounter_id: str,
                     redcap_record: dict,
                     patient_reference: dict,
                     location_references: List[dict],
                     symptom_resources: Optional[List[dict]],
                     symptom_references: Optional[List[dict]]) -> tuple:
    """
    Create FHIR encounter resource and encounter reference from given
    *redcap_record*.
    """
    enrollment_date = redcap_record.get('enrollment_date') \
                   or redcap_record.get('enrollment_date_time')

    encounter_date = datetime\
        .strptime(enrollment_date, '%Y-%m-%d %H:%M')\
        .astimezone().isoformat()

    encounter_identifier = create_identifier(
        system = f'{SFS}/encounter',
        value = encounter_id
    )

    encounter_class_codeable = create_codeable_concept(
        system = 'http://terminology.hl7.org/CodeSystem/v3-ActCode',
        code = 'FLD'
    )

    encounter_class = encounter_class_codeable['coding'][0]

    encounter_resource = create_encounter_resource(
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class,
        start_timestamp = encounter_date,
        patient_reference = patient_reference,
        location_references = location_references,
        diagnosis = symptom_references,
        contained = symptom_resources
    )

    encounter_resource_entry = create_resource_entry(
        resource = encounter_resource,
        full_url = generate_full_url_uuid()
    )

    encounter_reference = create_reference(
        reference_type = 'Encounter',
        identifier = encounter_identifier
    )

    return encounter_resource_entry, encounter_reference


def determine_all_questionnaire_items(redcap_record: dict) -> List[dict]:
    """
    Given a *redcap_record*, determine answers for all core questions
    """
    items = {}

    if redcap_record['age']:
        items['age'] = [{ 'valueInteger' : int(redcap_record['age']) }]
        items['age_months'] = [{ 'valueInteger' : int(redcap_record['age_months']) }]

    # Participant can select multiple insurance types, so create
    # a separate answer for each selection
    insurance_responses = find_selected_options('insurance___', redcap_record)
    insurances = determine_insurance_type(insurance_responses)
    items['insurance'] = [{'valueString': insurance} for insurance in insurances]

    # Participant can select multiple races, so create
    # a separate answer for each selection
    race_responses = find_selected_options('race___', redcap_record)
    if 'Prefer not to say' not in race_responses:
        races = race(race_responses)
        items['race'] = [{'valueString': race} for race in races]

    if redcap_record['hispanic'] != 'Prefer not to say':
        items['ethnicity'] = [{'valueBoolean': redcap_record['hispanic'] == 'Yes'}]

    items['travel_countries'] = [{ 'valueBoolean': redcap_record['travel_countries'] == 'Yes'}]
    items['travel_states'] = [{'valueBoolean': redcap_record['travel_states'] == 'Yes'}]

    response_items = []
    for item in items:
        response_items.append(create_questionnaire_response_item(
            question_id = item,
            answers = items[item]
        ))

    return response_items


def determine_insurance_type(insurance_reseponses: list) -> Optional[list]:
    """
    Determine the insurance type based on a given *insurance_response*
    """
    if len(insurance_reseponses) == 0:
        return None

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

    return list(map(standardize_insurance, insurance_reseponses))


def create_questionnaire_response_entry(redcap_record: dict,
                                        patient_reference: dict,
                                        encounter_reference: dict) -> dict:
    """
    Ceeate a questionnaire response entry based on given *redcap_record* and
    link to *patient_refernece* and *encounter_reference*
    """
    questionnaire_items = determine_all_questionnaire_items(redcap_record)

    questionnaire_response_resource = create_questionnaire_response_resource(
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        items = questionnaire_items
    )

    return (create_resource_entry(
        resource = questionnaire_response_resource,
        full_url = generate_full_url_uuid()
    ))


def create_specimen_observation_entry(specimen_reference: dict,
                                patient_reference: dict,
                                encounter_reference: dict) -> dict:
    """
    Create a speciment observation entry for the bundle that connects given
    *specimen_reference*, *patient_reference*, and *encounter_reference*.
    """
    specimen_observation_resource = create_specimen_observation(
       specimen_reference  = specimen_reference,
       patient_reference   = patient_reference,
       encounter_reference = encounter_reference
    )

    return (create_resource_entry(
       resource = specimen_observation_resource,
       full_url = generate_full_url_uuid()
    ))


def find_selected_options(option_prefix: str, redcap_record:dict) -> list:
    """
    Find all choosen options within *redcap_record* where option begins with
    provided *option_prefix*.

    Note: Values of options not choosen are empty strings.
    """
    selected = []

    for key in redcap_record:
        if key.startswith(option_prefix) and redcap_record[key]:
            selected.append(redcap_record[key])

    return selected


class UnknownSexError(ValueError):
    """
    Raised by :function: `determine_gender` if a provided *sex_response*
    is not among a set of expected values
    """
    pass


class UnknownInsuranceError(ValueError):
    """
    Raised by :function: `determine_insurance_type` if a provided
    *insurance_response* is not among a set of expected values
    """
    pass


class UnknownVaccineResponse(ValueError):
    """
    Raised by :function: `determine_vaccine_status` if a provided
    *vaccine_response* is not among a set of expected values
    """
    pass


class UnknownCepheidResultError(ValueError):
    """
    Raised by :function: `determine_cepheid_results` if a provided
    result response is not among a set of expected values
    """
    pass


class UnknownSymptomResponseError(ValueError):
    """
    Raised by :function: `determine_symptoms` if a provided
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
