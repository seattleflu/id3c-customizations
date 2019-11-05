"""
Process REDCap DETs that are specific to the Shelters Project
"""
import logging
from uuid import uuid4
from typing import Any, List
from datetime import datetime
from copy import deepcopy
from id3c.cli.command.etl import redcap_det, UnknownSiteError
from id3c.cli.command.clinical import generate_hash
from .redcap_map import *
from .fhir import *
from .redcap_det_kiosk import (
    determine_shelter_address,
    determine_dorm_address,
    determine_home_address,
    determine_census_tract,
    determine_location_type_code,
    determine_vaccine_date,
    create_locations,
    create_symptoms,
    create_encounter,
    create_questionnaire_response_entry,
    create_specimen_observation_entry,
    find_selected_options,
)

LOG = logging.getLogger(__name__)

REDCAP_URL = 'https://redcap.iths.org/'
SFS = 'https://seattleflu.org'
PROJECT_ID = 17542

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
REVISION = 1


@redcap_det.command_for_project(
    "shelters",
    redcap_url = REDCAP_URL,
    project_id = PROJECT_ID,
    required_instruments = REQUIRED_INSTRUMENTS,
    revision = REVISION,
    help = __doc__)

def redcap_det_shelters(*, det: dict, redcap_record: dict):
    patient_entry, patient_reference = create_patient(redcap_record)

    specimen_resource_entry, specimen_reference = create_specimen(redcap_record, patient_reference)

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

    encounter_id = '/'.join([REDCAP_URL, str(PROJECT_ID), redcap_record['record_id']])
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

    specimen_observation_entry = create_specimen_observation_entry(
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
        specimen_observation_entry
    ]

    if diagnostic_report_resource_entry:
        all_resource_entries.append(diagnostic_report_resource_entry)

    bundle = create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        entries = all_resource_entries
    )

    return bundle

# FUNCTIONS SPECIFIC TO SFS SHELTERS PROJECT
def create_patient(record: dict) -> tuple:
    """ Returns a FHIR Patietn resource entry and reference. """
    gender = map_sex(record["sex"])
    patient_id = generate_patient_hash(record, gender)
    patient_identifier = create_identifier(f"{SFS}/individual",patient_id)
    patient_resource = create_patient_resource([patient_identifier], gender)

    return create_entry_and_reference(patient_resource, "Patient")


def generate_patient_hash(redcap_record: dict, gender: str) -> str:
    """
    Create a hashed patient id for a given *redcap_record*
    """
    full_name = ' '.join([
        redcap_record['participant_first_name'],
        redcap_record['participant_last_name']
    ])

    patient = {
        'gender': gender,
        'name': canonicalize_name(full_name)
    }

    if redcap_record['birthday']:
        patient['birthday'] = convert_to_iso(redcap_record['birthday'], '%Y-%m-%d')

    if redcap_record.get('home_zipcode'):
        patient['zipcode'] = redcap_record['home_zipcode']
    elif redcap_record.get('shelter_name') and redcap_record['shelter_name'] != 'Other/none of the above':
        address = determine_shelter_address(redcap_record['shelter_name'])
        patient['zipcode'] = address['zipcode']
    elif redcap_record.get('uw_dorm') and redcap_record['uw_dorm'] != 'Other':
        address = determine_dorm_address(redcap_record['uw_dorm'])
        patient['zipcode'] = address['zipcode']

    return generate_hash(str(sorted(patient.values())))



def create_specimen(redcap_record: dict, patient_reference: dict) -> dict:
    """
    Create FHIR specimen reference from given *redcap_record*
    """
    sfs_sample_barcode = get_sfs_barcode(redcap_record)
    specimen_identifier = create_identifier(SFS, sfs_sample_barcode)

    specimen_resource = create_specimen_resource(
        [specimen_identifier], patient_reference
    )
    full_url = generate_full_url_uuid()
    specimen_entry = create_resource_entry(specimen_resource, full_url)
    specimen_reference = create_reference(
        reference_type = "Specimen",
        reference = full_url
    )

    return specimen_entry, specimen_reference


def get_sfs_barcode(redcap_record: dict) -> str:
    """
    Find SFS barcode within *redcap_record*.

    SFS barcode should be scanned into `sfs_barcode_0`, but if the scanner isn't
    working then barcode will be manually entered into `sfs_barcode_manual`
    """
    barcode = redcap_record['sfs_barcode_0']

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
    abbott_results = create_abbott_result_observation_resource(redcap_record)

    diagnostic_result_references = []

    for result in abbott_results:
        reference = create_reference(
            reference_type = 'Observation',
            reference = '#' + result['id']
        )
        diagnostic_result_references.append(reference)

    collection_datetime = datetime\
        .strptime(redcap_record['collection_date'], '%Y-%m-%d %H:%M:%S')\
        .strftime('%Y-%m-%dT%H:%M:%S')

    diagnostic_code = create_codeable_concept(
        system = "http://loinc.org",
        code = "54244-9",
        display = "Influenza virus identified in Unspecified specimen"
    )

    diagnostic_report_resource = create_diagnostic_report_resource(
        datetime = collection_datetime,
        diagnostic_code = diagnostic_code,
        patient_reference  = patient_reference,
        specimen_reference = specimen_reference,
        result = diagnostic_result_references,
        contained = abbott_results
    )

    return (create_resource_entry(
        resource = diagnostic_report_resource,
        full_url = generate_full_url_uuid()
    ))


def create_abbott_result_observation_resource(redcap_record: dict) -> List[dict]:
    """
    Determine the Abbott results based on responses in *redcap_record* and
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
                value = 'Abbott'
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
        'Invalid': {
            'system': 'http://snomed.info/sct',
            'code': '455371000124106',
            'display': 'Invalid result (qualifier value)'
        }
    }

    abbott_results = find_selected_options('abbott_results___', redcap_record)

    # Create observation resources for all potential results in Cepheid test
    diagnostic_results = {}
    for index, result in enumerate(code_map):
        new_observation = deepcopy(observation_resource)
        new_observation['id'] = 'result-' + str(index+1)
        new_observation['code']['coding'] = [code_map[result]]
        diagnostic_results[result] = (new_observation)

    # Mark all results as False if not positive for anything
    if "Not positive for anything" in abbott_results:
        for result in diagnostic_results:
            diagnostic_results[result]['valueBoolean'] = False

    # Mark Invalid as True and all other results as False if inconclusive
    elif "Invalid" in abbott_results:
        for result in diagnostic_results:
            if result == 'Inconclusive':
                diagnostic_results[result]['valueBoolean'] = True
            else:
                diagnostic_results[result]['valueBoolean'] = False

    else:
        for result in diagnostic_results:
            if result in abbott_results:
                diagnostic_results[result]['valueBoolean'] == True
                abbott_results.remove(result)
            else:
                diagnostic_results[result]['valueBoolean'] == False

        if len(abbott_results) != 0:
            raise UnknownAbbottResultError(f"Unknown Cepheid result «{abbott_results}»")

    return list(diagnostic_results.values())


def determine_encounter_locations(redcap_record: dict) -> dict:
    """
    Find all locations within a *redcap_record* that are relevant to
    an encounter
    """
    locations = {
        'site': determine_site_name(redcap_record),
    }

    def construct_location(address: dict, location_type: str) -> dict:
        return ({
            f'{location_type}-tract': {
                'value': determine_census_tract(address),
                'fullUrl': f'urn:uuid:{uuid4()}'
            },
            location_type: {
                'value': 'address_hash', #TODO: hash address
                'fullUrl': f'urn:uuid:{uuid4()}'
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
    Given a *redcap_record* find the site name which is listed as `site_type`
    for the Shelters project 2019-2020
    """
    site = redcap_record['site_type']

    site_name_map = {
        "St. Martin's": "StMartins",
        "DESC": "DESC",
        "Mary's Place - Burien": "MarysPlaceBurien",
        "Mary's Place - White Center": "MarysPlaceWhiteCenter",
        "Mary's Place - North Seattle": "MarysPlaceNorthSeattle",
        "ROOTS": "Roots",
        "Compass at First Presbyterian": "CompassFirstPresbyterian",
        "Jan and Peter's Place Women's Shelter": "JanAndPetersPlaceWomensShelter",
        "Blaine Center Men's Shelter": "BlainceCenterMensShelter"
    }

    if site not in site_name_map:
        raise UnknownSiteError(f"Unknown site name «{site}»")

    return site_name_map[site]


class UnknownAbbottResultError(ValueError):
    """
    Raised by :func: `create_abbott_result_observation_resource` if a provided
    result response is not among a set of expected values
    """
    pass
