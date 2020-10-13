"""
Process DETs for the UW Reopening (Husky Coronavirus Testing) REDCap projects.
"""
import re
import click
import json
import logging
from uuid import uuid4
from typing import Any, Callable, Dict, List, Mapping, Match, NamedTuple, Optional, Union, Tuple
from cachetools import TTLCache
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from enum import Enum
from id3c.db.session import DatabaseSession
from id3c.cli.command.etl import redcap_det
from id3c.cli.command.geocode import get_response_from_cache_or_geocoding
from id3c.cli.command.location import location_lookup
from id3c.cli.command.de_identify import generate_hash
from id3c.cli.redcap import is_complete, Record as REDCapRecord
from seattleflu.id3c.cli.command import age_ceiling
from .redcap import normalize_net_id
from .redcap_map import map_sex, map_symptom, UnknownVaccineResponseError
from .fhir import *
from . import race, first_record_instance, required_instruments


LOG = logging.getLogger(__name__)


class HuskyProject():
    id: int
    lang: str
    api_token_env_var: str
    command_name: str


    def __init__(self, project_id: int, lang: str, api_token_env_var: str, command_name: str) -> None:
        self.id = project_id
        self.lang = lang
        self.api_token_env_var = api_token_env_var
        self.command_name = command_name

PROJECTS = [
        HuskyProject(23854, "en", "REDCAP_API_TOKEN", "uw-reopening")
    ]

LANGUAGE_CODE = {
project.id: project.lang
    for project in PROJECTS }

class CollectionMethod(Enum):
    SWAB_AND_SEND = 'swab_and_send'
    KIOSK = 'kiosk'

class EventType(Enum):
    ENROLLMENT = 'enrollment'
    ENCOUNTER = 'encounter'

REVISION = 1

REDCAP_URL = 'https://redcap.iths.org/'
INTERNAL_SYSTEM = "https://seattleflu.org"
ENROLLMENT_EVENT_NAME = "enrollment_arm_1"
ENCOUNTER_EVENT_NAME = "encounter_arm_1"

REQUIRED_ENROLLMENT_INSTRUMENTS = [
    'eligibility_screening',
    'consent_form',
    'enrollment_questionnaire'
]


def command_for_each_project(function):
    """
    A decorator to register one redcap-det subcommand per REDCap project, each
    calling the same base *function*.

    Used for side-effects only; the original *function* is unmodified.
    """
    for project in PROJECTS:
        help_message = "Process REDCap DETs for Husky Coronavirus Testing (UW reopening)"

        redcap_det.command_for_project(
            name = project.command_name,
            redcap_url = REDCAP_URL,
            project_id = project.id,
            raw_coded_values = True,
            revision = REVISION,
            help = help_message,
            include_incomplete = True)(function)

    return function

@command_for_each_project
def redcap_det_uw_reopening(*, db: DatabaseSession, cache: TTLCache, det: dict,
    redcap_record_instances: List[REDCapRecord]) -> Optional[dict]:

    assert redcap_record_instances is not None and len(redcap_record_instances) > 0, \
        "The redcap_record_instances list was not populated."

    record_id = redcap_record_instances[0]['record_id']
    project_id = redcap_record_instances[0].project.id

    enrollments = [record for record in redcap_record_instances if record["redcap_event_name"] == ENROLLMENT_EVENT_NAME]
    assert len(enrollments) == 1, \
        f"Record had {len(enrollments)} enrollments."

    enrollment = enrollments[0]

    incomplete_enrollment_instruments = {
                instrument
                    for instrument
                    in REQUIRED_ENROLLMENT_INSTRUMENTS
                    if not is_complete(instrument, enrollment)
            }

    if incomplete_enrollment_instruments:
        LOG.debug(f"The following required enrollment instruments «{incomplete_enrollment_instruments}» are not yet marked complete.")
        return None

    # If the participant's age < 18 ensure we have parental consent.
    if (enrollment['core_age_years'] == "" or int(enrollment['core_age_years']) < 18) and \
            (is_complete('parental_consent_form', enrollment) == False or enrollment['signature_parent'] == ''):
        LOG.debug("The participant is < 18 years old and we do not have parental consent. Skipping record.")
        return None

    patient_entry, patient_reference = create_patient(enrollment)
    birthdate = parse_birth_date(enrollment)

    if not patient_entry:
        LOG.warning("Skipping record with insufficient information to construct patient")
        return None

    location_resource_entries = locations(db, cache, enrollment)
    persisted_resource_entries = [patient_entry, *location_resource_entries]

    for redcap_record_instance in redcap_record_instances:

        event_type = None
        collection_method = None

        if redcap_record_instance["redcap_event_name"] == ENROLLMENT_EVENT_NAME:
            event_type = EventType.ENROLLMENT
        elif redcap_record_instance["redcap_event_name"] == ENCOUNTER_EVENT_NAME:
            event_type = EventType.ENCOUNTER
            if is_complete('kiosk_registration_4c7f', redcap_record_instance):
                collection_method = CollectionMethod.KIOSK
            elif is_complete('test_order_survey', redcap_record_instance):
                collection_method = CollectionMethod.SWAB_AND_SEND
        else:
            LOG.error(f"The record instance has an unexpected event name: {redcap_record_instance['redcap_event_name']}")
            continue

        # Skip an ENCOUNTER instance if we don't have the data we need to
        # create an encounter.
        if event_type == EventType.ENCOUNTER \
            and not is_complete('daily_attestation', redcap_record_instance) \
                and not collection_method  \
                and not redcap_record_instance['testing_date']: # from the 'Testing Determination - Internal' instrument
                    LOG.debug("Skipping record instance with insufficient information to construct the initial encounter")
                    continue

        # site_reference refers to where the sample was collected
        site_reference = create_site_reference(redcap_record_instance, collection_method, event_type)

        initial_encounter_entry, initial_encounter_reference = create_encounter(
            redcap_record_instance, patient_reference, site_reference,
            location_resource_entries, event_type, collection_method)

        # Skip the entire record if we can't create the enrollment encounter.
        # Otherwise, just skip the record instance.
        if not initial_encounter_entry:
            if event_type == EventType.ENROLLMENT:
                LOG.warning("Skipping record because we could not create the enrollment encounter")
                return None
            else:
                LOG.warning("Skipping record instance with insufficient information to construct the initial encounter")
                continue

        specimen_entry = None
        specimen_observation_entry = None
        specimen_received = (collection_method == CollectionMethod.SWAB_AND_SEND and \
            is_complete('post_collection_data_entry_qc', redcap_record_instance)) or \
            (collection_method == CollectionMethod.KIOSK and \
            is_complete('kiosk_registration_4c7f', redcap_record_instance))

        if specimen_received:
            specimen_entry, specimen_reference = create_specimen(redcap_record_instance, patient_reference, collection_method)
            specimen_observation_entry = create_specimen_observation_entry(
                specimen_reference, patient_reference, initial_encounter_reference)
        else:
            LOG.info("Creating encounter for record instance without sample")

        if specimen_received and not specimen_entry:
            LOG.warning("Skipping record instance with insufficent information to construct a specimen")
            continue

        computed_questionnaire_entry = None
        enrollment_questionnaire_entry = None
        daily_questionnaire_entry = None
        testing_determination_internal_questionnaire_entry = None
        follow_up_encounter_entry = None
        follow_up_questionnaire_entry = None
        follow_up_computed_questionnaire_entry = None

        computed_questionnaire_entry = create_computed_questionnaire_response(
            redcap_record_instance, patient_reference, initial_encounter_reference,
            birthdate, datetime.strptime(initial_encounter_entry['resource']['period']['start'], '%Y-%m-%d'))

        if event_type == EventType.ENROLLMENT:
            enrollment_questionnaire_entry = create_enrollment_questionnaire_response(
            enrollment, patient_reference, initial_encounter_reference)
        else:
            testing_determination_internal_questionnaire_entry = \
                create_testing_determination_internal_questionnaire_response(
                redcap_record_instance, patient_reference, initial_encounter_reference)

            daily_questionnaire_entry = \
                create_daily_questionnaire_response(
                redcap_record_instance, patient_reference, initial_encounter_reference)

            if is_complete('week_followup', redcap_record_instance):
                follow_up_encounter_entry, follow_up_encounter_reference = create_follow_up_encounter(
                    redcap_record_instance, patient_reference, site_reference, initial_encounter_reference)
                follow_up_questionnaire_entry = create_follow_up_questionnaire_response(
                redcap_record_instance, patient_reference, follow_up_encounter_reference)
                follow_up_computed_questionnaire_entry = create_computed_questionnaire_response(
                redcap_record_instance, patient_reference, follow_up_encounter_reference,
                birthdate, datetime.strptime(follow_up_encounter_entry['resource']['period']['start'], '%Y-%m-%d'))


        current_instance_entries = [
            initial_encounter_entry,
            computed_questionnaire_entry,
            enrollment_questionnaire_entry,
            testing_determination_internal_questionnaire_entry,
            daily_questionnaire_entry,
            specimen_entry,
            specimen_observation_entry,
            follow_up_encounter_entry,
            follow_up_questionnaire_entry,
            follow_up_computed_questionnaire_entry
        ]

        persisted_resource_entries.extend(list(filter(None, current_instance_entries)))


    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{REDCAP_URL}{project_id}/{record_id}",
        entries = list(filter(None, persisted_resource_entries))
    )


def parse_birth_date(record: dict) -> Optional[datetime]:
    """ Returns a participant's birth date from a given *record* as a datetime
    object if it can be parsed. Otherwise, emits a warning and returns None. """
    try:
        birth_date = datetime.strptime(record['core_birthdate'], '%Y-%m-%d')
    except ValueError:
        LOG.warning("Invalid `core_birthdate`.")
        birth_date = None

    return birth_date

def create_site_reference(record: dict, collection_method: CollectionMethod, event_type: EventType) -> Optional[Dict[str,dict]]:
    """
    Create a Location reference for site of the sample collection encounter based
    on how the sample was collected.
    """
    if collection_method == CollectionMethod.KIOSK:
        record_location = record.get('location_type')
        if record_location:
            site = site_map(record_location)
    else:
        site = 'UWReopeningSwabNSend'

    return {
        "location": create_reference(
            reference_type = "Location",
            identifier = create_identifier(f"{INTERNAL_SYSTEM}/site", site)
        )
    }


def site_map(record_location: str) -> str:
    """
    Maps *record_location* to the corresponding site name.
    """
    location_site_map = {
        'bothell':  'UWBothell',
        'odegaard': 'UWOdegaardLibrary',
        'slu':      'UWSouthLakeUnion',
        'tacoma':   'UWTacoma',
        'uw_club':  'UWClub'
    }

    if record_location not in location_site_map:
        raise UnknownRedcapRecordLocation(f"Found unknown location type «{record_location}»")

    return location_site_map[record_location]


def locations(db: DatabaseSession, cache: TTLCache, record: dict) -> list:
    """ Creates a list of Location resource entries from a REDCap record. """
    lodging_options = [
        'shelter',
        'afl',
        'snf',
        'ltc',
        'be',
        'pst',
        'cf',
        'none'
    ]

    if record.get('core_housing_type') in lodging_options:
        housing_type = 'lodging'
    else:
        housing_type = 'residence'

    address = {
        'street': record['core_home_street'],
        'secondary': record['core_apartment_number'],
        'city': record['core_home_city'],
        'state': record['core_home_state'],
        'zipcode': record['core_zipcode'],
    }

    lat, lng, canonicalized_address = get_response_from_cache_or_geocoding(address, cache)
    if not canonicalized_address:
        return []  # TODO

    tract_location = residence_census_tract(db, (lat, lng), housing_type)
    # TODO what if tract_location is null?
    tract_full_url = generate_full_url_uuid()
    tract_entry = create_resource_entry(tract_location, tract_full_url)

    address_hash = generate_hash(canonicalized_address)
    address_location = create_location(
        f"{INTERNAL_SYSTEM}/location/address",
        address_hash,
        housing_type,
        tract_full_url
    )
    address_entry = create_resource_entry(address_location, generate_full_url_uuid())

    return [tract_entry, address_entry]


def residence_census_tract(db: DatabaseSession, lat_lng: Tuple[float, float],
    housing_type: str) -> Optional[dict]:
    """
    Creates a new Location Resource for the census tract containing the given
    *lat_lng* coordintes and associates it with the given *housing_type*.
    """
    location = location_lookup(db, lat_lng, 'tract')

    if location and location.identifier:
        return create_location(
            f"{INTERNAL_SYSTEM}/location/tract", location.identifier, housing_type
        )
    else:
        LOG.warning("No census tract found for given location.")
        return None


def create_location(system: str, value: str, location_type: str, parent: str=None) -> dict:
    """ Returns a FHIR Location resource. """
    location_type_system = "http://terminology.hl7.org/CodeSystem/v3-RoleCode"
    location_type_map = {
        "residence": "PTRES",
        "school": "SCHOOL",
        "work": "WORK",
        "site": "HUSCS",
        "lodging": "PTLDG",
    }

    location_type_cc = create_codeable_concept(location_type_system,
                                            location_type_map[location_type])
    location_identifier = create_identifier(system, value)
    part_of = None
    if parent:
        part_of = create_reference(reference_type="Location", reference=parent)

    return create_location_resource([location_type_cc], [location_identifier], part_of)


def create_patient(record: REDCapRecord) -> tuple:
    """ Returns a FHIR Patient resource entry and reference. """
    gender = map_sex(record['core_sex'])

    language_codeable_concept = create_codeable_concept(
        system = 'urn:ietf:bcp:47',
        code = LANGUAGE_CODE[record.project.id]
    )
    communication = [{
        'language' : language_codeable_concept,
        'preferred': True # Assumes that the project language is the patient's preferred language
    }]

    net_id = normalize_net_id(record.get('netid'))
    if net_id:
        patient_id = generate_hash(net_id)
    else:
        patient_id = generate_patient_hash(
            names       = (record['core_participant_first_name'], record['core_participant_last_name']),
            gender      = gender,
            birth_date  = record['core_birthdate'],
            postal_code = record['core_zipcode'])

    if not patient_id:
        # Some piece of information was missing, so we couldn't generate a
        # hash.  Fallback to treating this individual as always unique by using
        # the REDCap record id.
        patient_id = generate_hash(f"{REDCAP_URL}{record.project.id}/{record['record_id']}")

    LOG.debug(f"Generated individual identifier {patient_id}")

    patient_identifier = create_identifier(f"{INTERNAL_SYSTEM}/individual", patient_id)
    patient_resource = create_patient_resource([patient_identifier], gender, communication)

    return create_entry_and_reference(patient_resource, "Patient")


def create_encounter(record: REDCapRecord, patient_reference: dict,
    site_reference: dict, locations: list, event_type: EventType,
    collection_method: CollectionMethod) -> tuple:
    """
    Returns a FHIR Encounter resource entry and reference for the encounter in the study.
    """

    def grab_symptom_key(key: str, variable_name: str) -> Optional[Match[str]]:
        if record[key] == '1':
            return re.match(f"{variable_name}___[a-z_]+", key)
        else:
            return None

    def build_condition(symptom: str, onset_date: str) -> dict:
        return create_resource_condition(record, symptom, patient_reference, onset_date)

    def build_diagnosis(symptom: str) -> Optional[dict]:
        mapped_symptom = map_symptom(symptom)
        if not mapped_symptom:
            return None

        return { "condition": { "reference": f"#{mapped_symptom}" } }

    def build_locations_list(location: dict) -> dict:
        return {
            "location": create_reference(
                reference_type = "Location",
                reference = location["fullUrl"]
            )
        }

    def non_tract_locations(resource: dict):
        return bool(resource) \
            and resource['resource']['identifier'][0]['system'] != f"{INTERNAL_SYSTEM}/location/tract"

    # Map the various symptoms variables to their onset date.
    # For daily_symptoms_covid_like we don't know the actual onset date. The questions asks
    # "in the past 24 hours"
    if event_type == EventType.ENCOUNTER:
        symptom_onset_map = {
            'daily_symptoms_covid_like': None,
            'symptoms': record['symptom_onset'],
            'symptoms_kiosk': record['symptom_duration_kiosk'],
            'symptoms_swabsend': record['symptom_duration_swabsend']
        }
    elif event_type == EventType.ENROLLMENT:
        symptom_onset_map = {'symptoms_base': record['symptom_onset_base']}

    contained = []
    diagnosis = []

    for symptom_variable in symptom_onset_map:
        symptom_keys = []

        for redcap_key in record.keys():
            symptom_key = grab_symptom_key(redcap_key, symptom_variable)
            if symptom_key:
                symptom_keys.append(symptom_key.string)

        symptoms = list(map(lambda x: re.sub('[a-z_]+___', '', x), symptom_keys))
        for symptom in symptoms:
            contained.append(build_condition(symptom, symptom_onset_map[symptom_variable]))
            diagnosis.append(build_diagnosis(symptom))

    encounter_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/encounter",
        value = f"{REDCAP_URL}{record.project.id}/{record['record_id']}/{record['redcap_event_name']}/{record['redcap_repeat_instance']}"
    )

    collection_code = None

    # See https://terminology.hl7.org/1.0.0/CodeSystem-v3-ActCode.html for
    # possible collection codes.
    # HH = 'home health'
    # FLD = 'field'
    if event_type == EventType.ENROLLMENT or collection_method == CollectionMethod.SWAB_AND_SEND:
        collection_code = "HH"
    elif collection_method == CollectionMethod.KIOSK:
        collection_code = "FLD"

    # For the encounter_date for an ENCOUNTER, first try the attestation_date
    # from the daily attestation survey then try nasal_swab_timestamp from
    # the kiosk registration and finally the swab-and-send order date.
    encounter_date = None

    if event_type == EventType.ENCOUNTER:
        if record.get('attestation_date'):
            encounter_date = record.get('attestation_date')
        elif record.get('nasal_swab_timestamp'):
            encounter_date = datetime.strptime(record.get('nasal_swab_timestamp'),
                '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        elif record.get('time_test_order'):
            encounter_date = datetime.strptime(record.get('time_test_order'),
                '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        elif record.get('testing_date'): # from the 'Testing Determination - Internal' instrument
            encounter_date = record.get('testing_date')

    elif event_type == EventType.ENROLLMENT:
        encounter_date = record.get('enrollment_date')

    if not encounter_date:
        return None, None

    encounter_class_coding = create_coding(
        system = "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code = collection_code
    )

    non_tracts = list(filter(non_tract_locations, locations))
    non_tract_references = list(map(build_locations_list, non_tracts))
    # Add hard-coded site Location reference
    if site_reference:
        non_tract_references.append(site_reference)

    encounter_resource = create_encounter_resource(
        encounter_source = create_redcap_uri(record),
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class_coding,
        encounter_date = encounter_date,
        patient_reference = patient_reference,
        location_references = non_tract_references,
        diagnosis = diagnosis,
        contained = contained
    )

    return create_entry_and_reference(encounter_resource, "Encounter")


def create_resource_condition(record: dict, symptom_name: str, patient_reference: dict, onset_date:str) -> Optional[dict]:
    """ Returns a FHIR Condition resource. """
    mapped_symptom_name = map_symptom(symptom_name)
    if not mapped_symptom_name:
        return None

    # XXX TODO: Define this as a TypedDict when we upgrade from Python 3.6 to
    # 3.8.  Until then, there's no reasonable way to type this data structure
    # better than Any.
    #   -trs, 24 Oct 2019
    condition: Any = {
        "resourceType": "Condition",
        "id": f'{mapped_symptom_name}',
        "code": {
            "coding": [
                {
                    "system": f"{INTERNAL_SYSTEM}/symptom",
                    "code": mapped_symptom_name
                }
            ]
        },
        "subject": patient_reference
    }

    if onset_date:
        condition["onsetDateTime"] = onset_date

    return condition


def create_specimen(record: dict, patient_reference: dict, collection_method: CollectionMethod) -> tuple:
    """ Returns a FHIR Specimen resource entry and reference """

    def specimen_barcode() -> Optional[str]:
        """
        Return specimen barcode from REDCap record.
        """
         # Normalize all barcode fields upfront.
        barcode_fields = {
            # Kiosk related
            "collect_barcode_kiosk", # from	kiosk_registration_4c7f

            # Swab and Send related
            "pre_scan_barcode", # from test_fulfillment_form
            "barcode_swabsend",	# from husky_test_kit_registration
            "return_utm_barcode", # from post_collection_data_entry_qc
        }

        for barcode_field in barcode_fields:
            record[barcode_field] = record[barcode_field].strip().lower()

        return record["collect_barcode_kiosk"] or record["return_utm_barcode"] or \
            record["pre_scan_barcode"] or None


    barcode = specimen_barcode()

    if not barcode:
        LOG.warning("Could not create Specimen Resource due to lack of barcode.")
        return None, None

    specimen_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/sample",
        value = barcode
    )

    # YYYY-MM-DD HH:MM:SS in REDCap
    received_time = record['samp_process_date'].split()[0] if record.get('samp_process_date') else None

    note = None

    if record['able_to_test'] == 'no':
        note = 'never-tested'
    else:
        note = 'can-test'

    specimen_type = 'NSECR'  # Nasal swab.
    specimen_resource = create_specimen_resource(
        [specimen_identifier], patient_reference, specimen_type, received_time,
        collection_date(record, collection_method), note
    )

    return create_entry_and_reference(specimen_resource, "Specimen")


def collection_date(record: dict, collection_method: CollectionMethod) -> Optional[str]:
    """
    Determine sample/specimen collection date from the given REDCap *record*.
    """
    if collection_method == CollectionMethod.KIOSK:
        return record["nasal_swab_q"]
    elif collection_method == CollectionMethod.SWAB_AND_SEND:
        return record["date_on_tube"] or record["kit_reg_date"] or record["back_end_scan_date"]
    else:
        return None

def combine_multiple_fields(record: Dict[Any, Any], field_prefix: str, field_suffix: str = "") -> Optional[List]:
        """
        Handles the combining of multiple fields asking the same question such
        as country and state traveled.
        """
        regex = rf'^{re.escape(field_prefix)}[0-9]+{re.escape(field_suffix)}$'
        empty_value = ''
        answered_fields = list(filter(lambda f: filter_fields(f, record[f], regex, empty_value), record))

        if not answered_fields:
            return None

        return list(map(lambda x: record[x], answered_fields))



def create_enrollment_questionnaire_response(record: dict, patient_reference: dict,
                                            encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the enrollment
    encounter (i.e. encounter of enrollment into the study)
    """

    # Do not include `core_age_years` because we calculate the age ourselves in the computed questionnaire.
    integer_questions = [
        'weight',
        'height_total',
        'tier'
    ]

    string_questions = [
        'text_or_email',
        'text_or_email_attestation',
        'campus_location',
        'affiliation',
        'student_level',
        'sea_employee_type',
        'core_house_members',
        'core_education',
        'core_income',
        'wfh_base',
        'core_housing_type',
        'core_health_risk',
        'core_tobacco_use',
        'sought_care_base',
        'hospital_where_base',
        'hospital_ed_base',
        'prior_test_positive_base',
        'prior_test_type_base',
        'prior_test_result_base',
        'contact_base',
        'wash_hands_base',
        'clean_surfaces_base',
        'hide_cough_base',
        'mask_base',
        'distance_base',
        'novax_reason',
        'covid_vaccine',
        'covid_novax_reason',
        'countries_visited_base',
        'states_visited_base',
        'alerts_off',
        'pronouns',
        'on_campus_freq',
        'vaccine_method',
        'vaccine_where'
    ]

    date_questions = [
        'today_consent',
        'enrollment_date_time',
        'hospital_arrive_base',
        'hospital_leave_base',
        'prior_test_positive_date_base'
    ]

    boolean_questions = [
        'study_area',
        'attend_uw',
        'english_speaking',
        'athlete',
        'uw_medicine_yesno',
        'inperson_classes',
        'uw_job',
        'uw_greek_member',
        'live_other_uw',
        'uw_apt_yesno',
        'core_pregnant',
        'core_latinx',
        'mobility',
        'vaccine_hx',
        'hall_health',
        'prior_test_base',
        'travel_countries_phs_base',
        'travel_states_phs_base',
        'swab_and_send_calc',
        'kiosk_calc',
        'covid_test_week_base'
    ]

    decimal_questions = [
        'bmi'
    ]

    coding_questions = [
        'core_race'
    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'core_race',
        'core_health_risk',
        'core_tobacco_use',
        'sought_care_base',
        'prior_test_type_base',
        'prior_test_positive_base',
        'contact_base'
    ]

    question_categories = {
        'valueCoding': coding_questions,
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueString': string_questions,
        'valueDate': date_questions,
        'valueDecimal': decimal_questions
    }

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    # Combine all fields answering the same question
    record['countries_visited_base'] = combine_multiple_fields(record, 'country', '_base')
    record['states_visited_base'] = combine_multiple_fields(record, 'state', '_base')

    # Age Ceiling
    try:
        record['core_age_years'] = age_ceiling(int(record['core_age_years']))
    except ValueError:
        record['core_age_years'] = record['core_age_years'] = None

    # Set the study tier
    tier = None
    if record['tier_1'] == '1':
        tier = 1
    elif record['tier_2'] == '1':
        tier = 2
    elif record['tier_3'] == '1':
        tier = 3
    record['tier'] = tier

    return questionnaire_response(record, question_categories, patient_reference, encounter_reference, True)


def filter_fields(field: str, field_value: str, regex: str, empty_value: str) -> bool:
    """
    Function that filters for *field* matching given *regex* and the
    *field_value* must not equal the expected *empty_value.
    """
    if re.match(regex, field) and field_value != empty_value:
        return True

    return False


def combine_checkbox_answers(record: dict, coded_question: str) -> Optional[List]:
    """
    Handles the combining "select all that apply"-type checkbox
    responses into one list.

    Uses our in-house mapping for race and symptoms
    """
    regex = rf'{re.escape(coded_question)}___[\w]*$'
    empty_value = '0'
    answered_checkboxes = list(filter(lambda f: filter_fields(f, record[f], regex, empty_value), record))
    # REDCap checkbox fields have format of {question}___{answer}
    answers = list(map(lambda k: k.replace(f"{coded_question}___", ""), answered_checkboxes))

    if coded_question == 'race':
        return race(answers)

    if re.match(r'fu_[1-4]_symptoms$', coded_question):
        return list(map(lambda a: map_symptom(a), answers))

    return answers

def map_vaccine(vaccine_response: str) -> Optional[bool]:
    """
    Maps a vaccine response to FHIR immunization status codes
    (https://www.hl7.org/fhir/valueset-immunization-status.html)
    """
    vaccine_map = {
        'yes': True,
        'no': False,
        'dont_know': None,
        '': None
    }

    if vaccine_response not in vaccine_map:
        raise UnknownVaccineResponseError(f"Unknown vaccine response «{vaccine_response}»")

    return vaccine_map[vaccine_response]

def vaccine(record: Any) -> Optional[dict]:
    """
    For a given *record*, return a questionnaire response item with the vaccine
    response(s) encoded.
    """
    vaccine_status = map_vaccine(record["vaccine"])
    if vaccine_status is None:
        return None

    answers: List[Dict[str, Any]] = [{ 'valueBoolean': vaccine_status }]

    date = vaccine_date(record)
    if vaccine_status and date:
        answers.append({ 'valueDate': date })

    return create_questionnaire_response_item('vaccine', answers)


def vaccine_date(record: dict) -> Optional[str]:
    """ Converts a vaccination date to 'YYYY' or 'YYYY-MM' format. """
    year = record['vaccine_year']
    month = record['vaccine_month']

    if year == '' or year == 'dont_know':
        return None

    if month == 'dont_know':
        return datetime.strptime(year, '%Y').strftime('%Y')

    return datetime.strptime(f'{month} {year}', '%B %Y').strftime('%Y-%m')


def questionnaire_response(record: dict,
                           question_categories: Dict[str, list],
                           patient_reference: dict,
                           encounter_reference: dict,
                           include_vaccine_item: bool) -> Optional[dict]:
    """
    Provided a dictionary of *question_categories* with the key being the value
    type and the value being a list of field names, return a FHIR
    Questionnaire Response resource entry.
    """
    def build_questionnaire_items(question: str) -> Optional[dict]:
        return questionnaire_item(record, question, category)

    items: List[dict] = []
    for category in question_categories:
        category_items = list(map(build_questionnaire_items, question_categories[category]))
        for item in category_items:
            if item:
                items.append(item)

    if include_vaccine_item:
        vaccine_item = vaccine(record)
        if vaccine_item:
            items.append(vaccine_item)

    if items:
        questionnaire_reseponse_resource = create_questionnaire_response_resource(
            patient_reference, encounter_reference, items
        )
        full_url = generate_full_url_uuid()
        return create_resource_entry(questionnaire_reseponse_resource, full_url)

    return None


def questionnaire_item(record: dict, question_id: str, response_type: str) -> Optional[dict]:
    """ Creates a QuestionnaireResponse internal item from a REDCap record.
    """
    response = record.get(question_id)
    if not response:
        return None

    def cast_to_coding(string: str):
        """ Currently the only QuestionnaireItem we code is race. """
        return create_coding(
            system = f"{INTERNAL_SYSTEM}/race",
            code = string,
        )

    def cast_to_string(string: str) -> Optional[str]:
        if string != '':
            return string.strip()
        return None

    def cast_to_integer(string: str) -> Optional[int]:
        try:
            return int(string)
        except ValueError:
            return None

    def cast_to_float(string: str) -> Optional[float]:
        try:
            return float(string)
        except ValueError:
            return None

    def cast_to_boolean(string: str) -> Optional[bool]:
        if (string and string.lower() == 'yes') or string == '1':
            return True
        elif (string and string.lower() == 'no') or string == '0':
            return False
        return None

    def build_response_answers(response: Union[str, List]) -> List:
        answers = []
        if not isinstance(response, list):
            response = [response]

        for item in response:
            type_casted_item = casting_functions[response_type](item)

            # cast_to_boolean can return False, so must be `is not None`
            if type_casted_item is not None:
                answers.append({ response_type: type_casted_item })

        return answers

    casting_functions: Mapping[str, Callable[[str], Any]] = {
        'valueCoding': cast_to_coding,
        'valueInteger': cast_to_integer,
        'valueBoolean': cast_to_boolean,
        'valueString': cast_to_string,
        'valueDate': cast_to_string,
        'valueDecimal': cast_to_float
    }

    answers = build_response_answers(response)
    if answers:
        return create_questionnaire_response_item(question_id, answers)

    return None


def create_follow_up_encounter(record: REDCapRecord,
                               patient_reference: dict,
                               site_reference: dict,
                               initial_encounter_reference: dict) -> tuple:
    """
    Returns a FHIR Encounter resource entry and reference for a follow-up
    encounter
    """
    if not record.get('fu_timestamp'):
        return None, None

    encounter_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/encounter",
        value = f"{REDCAP_URL}{record.project.id}/{record['record_id']}/{record['redcap_event_name']}/{record['redcap_repeat_instance']}_follow_up"
    )
    encounter_class_coding = create_coding(
        system = "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code = "HH"
    )
    encounter_reason_code = create_codeable_concept(
        system = "http://snomed.info/sct",
        code = "390906007",
        display = "Follow-up encounter"
    )

    # YYYY-MM-DD HH:MM in REDCap
    encounter_date = record['fu_timestamp'].split()[0]
    encounter_resource = create_encounter_resource(
        encounter_source = create_redcap_uri(record),
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class_coding,
        encounter_date = encounter_date,
        patient_reference = patient_reference,
        location_references = [site_reference],
        reason_code = [encounter_reason_code],
        part_of = initial_encounter_reference
    )

    return create_entry_and_reference(encounter_resource, "Encounter")


def create_follow_up_questionnaire_response(record: dict, patient_reference: dict,
                                            encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the follow-up
    encounter.

    Note: `fu_which_activites` is misspelled on
    purpose to match the misspelling of the fields in the REDCap project.
    """
    boolean_questions = [
        'fu_illness',
        'fu_change',
        'result_changes',
        'fu_household_sick'
    ]

    integer_questions = [
        'fu_days_illness',
        'fu_number_sick'
    ]

    string_questions = [
        'fu_fever',
        'fu_headache',
        'fu_cough',
        'fu_chills',
        'fu_sweat',
        'fu_throat',
        'fu_nausea',
        'fu_nose',
        'fu_tired',
        'fu_ache',
        'fu_breathe',
        'fu_diarrhea',
        'fu_rash',
        'fu_ear',
        'fu_eye',
        'fu_smell_taste',
        'fu_feel_normal',
        'fu_care',
        'fu_hospital_ed',
        'fu_work_school',
        'fu_activities',
        'fu_which_activites',
        'fu_test_result',
        'fu_behaviors_no',
        'fu_behaviors_inconclusive',
        'fu_behaviors',
        'fu_1_symptoms',
        'fu_2_symptoms',
        'fu_3_symptoms',
        'fu_4_symptoms',
        'fu_1_test',
        'fu_2_test',
        'fu_3_test',
        'fu_4_test',
        'fu_1_result',
        'fu_2_result',
        'fu_3_result',
        'fu_4_result',
        'fu_healthy_test',
        'fu_healthy_result'
    ]

    date_questions = [
        'fu_timestamp',
        'fu_symptom_duration',
        'fu_date_care',
        'fu_1_date',
        'fu_2_date',
        'fu_3_date',
        'fu_4_date',
        'followup_date'
    ]

    question_categories = {
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueString': string_questions,
        'valueDate': date_questions
    }

    # Combine checkbox answers into one list
    checkbox_fields = [
        'fu_care',
        'fu_which_activites',
        'fu_behaviors_no',
        'fu_behaviors_inconclusive',
        'fu_behaviors',
        'fu_1_symptoms',
        'fu_2_symptoms',
        'fu_3_symptoms',
        'fu_4_symptoms'
    ]

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    return questionnaire_response(record, question_categories, patient_reference, encounter_reference, False)

def create_testing_determination_internal_questionnaire_response(record: dict, patient_reference: dict,
                                            encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the
    testing_determination_internal instrument. This instrument is used
    to communicate with the Priority Queue application. Note that for this
    questionnaire we do want to return answers for all questions even if they are
    not answered.
    """

    string_questions = [
        'testing_trigger',
        'testing_type',
    ]

    question_categories = {
        'valueString': string_questions
    }

    return questionnaire_response(record, question_categories, patient_reference, encounter_reference, False)

def create_computed_questionnaire_response(record: dict, patient_reference: dict,
                                            encounter_reference: dict,
                                            birthdate: datetime, encounter_date: datetime) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for a
    computed questionnaire. This "computed questionnaire" produces
    answers that do not appear directly in an actual REDCap survey.
    For example, a computed question captures the participant's age
    on the date of the encounter.
    """
    # A birthdate of None will return a falsy relativedelta() object
    delta = relativedelta(encounter_date, birthdate)
    if not delta:
        age = None
    else:
        age = delta.years
    record['age'] = age

    integer_questions = [
        'age'
    ]

    question_categories = {
        'valueInteger': integer_questions
    }

    return questionnaire_response(record, question_categories, patient_reference, encounter_reference, False)

def create_daily_questionnaire_response(record: dict, patient_reference: dict,
                                            encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the daily
    attestation questionnaire.
    """
    boolean_questions = [
        'daily_symptoms',
        'prev_pos',
        'hall_health_2',
        'prior_test',
        'travel_countries_phs',
        'travel_states_phs',
        'screen_positive',
        'attend_event',
    ]

    string_questions = [
        'daily_exposure',
        'daily_exposure_known_pos',
        'sought_care',
        'hospital_where',
        'hospital_ed',
        'prior_test_positive',
        'prior_test_type',
        'prior_test_result',
        'contact',
        'wash_hands',
        'clean_surfaces',
        'hide_cough',
        'mask',
        'distance',
        'wfh',
        'countries_visited',
        'states_visited'
    ]

    date_questions = [
        'attestation_timestamp',
        'hospital_arrive',
        'hospital_leave',
        'prior_test_positive_date'
    ]

    question_categories = {
        'valueBoolean': boolean_questions,
        'valueString': string_questions,
        'valueDate': date_questions
    }

    # Combine checkbox answers into one list
    checkbox_fields = [
        'sought_care',
        'prior_test_type',
        'contact'
    ]

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    # Combine all fields answering the same question
    record['countries_visited'] = combine_multiple_fields(record, 'country')
    record['states_visited'] = combine_multiple_fields(record, 'state')

    return questionnaire_response(record, question_categories, patient_reference, encounter_reference, False)


class UnknownRedcapZipCode(ValueError):
    """
    Raised by :function: `zipcode_map` if a provided *redcap_code* is not
    among a set of expected values.
    """
    pass


class UnknownRedcapRecordLocation(ValueError):
    """
    Raised by :function: `site_map` if a provided *redcap_location* is not
    among a set of expected values.
    """
    pass
