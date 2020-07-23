"""
Process DETs for the greater Seattle Coronavirus Assessment Network (SCAN) REDCap projects.
"""
import re
import click
import json
import logging
from uuid import uuid4
from typing import Any, Callable, Dict, List, Mapping, Match, NamedTuple, Optional, Union, Tuple
from datetime import datetime
from cachetools import TTLCache
from id3c.db.session import DatabaseSession
from id3c.cli.command.etl import redcap_det
from id3c.cli.command.geocode import get_response_from_cache_or_geocoding
from id3c.cli.command.location import location_lookup
from id3c.cli.redcap import is_complete, Record as REDCapRecord
from seattleflu.id3c.cli.command import age_ceiling
from .redcap_map import *
from .fhir import *
from . import race


LOG = logging.getLogger(__name__)


class ScanProject(NamedTuple):
    id: int
    lang: str
    type: Optional[str] = None

PROJECTS = [
    ScanProject(20759, "en"),
    ScanProject(21520, "es"),
    ScanProject(21512, "vi"),
    ScanProject(21514, "zh-Hans"),
    ScanProject(21521, "zh-Hant"),
    ScanProject(21809, "so"),
    ScanProject(21810, "ko"),
    ScanProject(21808, "ru"),
    ScanProject(21953, "tl"),
    ScanProject(21950, "am"),
    ScanProject(21951, "ti"),
    ScanProject(22461, "en", "irb"),
    ScanProject(22475, "es", "irb"),
    ScanProject(22474, "zh-Hant", "irb"),
    ScanProject(22477, "vi", "irb"),
    ScanProject(22472, "ru", "irb"),
    ScanProject(22476, "ko", "irb"),
    ScanProject(22471, "so", "irb"),
    ScanProject(23089, "en", "irb-kiosk"),

]

REVISION = 14

REDCAP_URL = 'https://redcap.iths.org/'
INTERNAL_SYSTEM = "https://seattleflu.org"

LANGUAGE_CODE = {
    project.id: project.lang
        for project in PROJECTS }

REQUIRED_INSTRUMENTS = [
    'consent_form',
]


# A decorator lets us keep command registration up here at the top, instead of
# moving the loop after the definition of redcap_det_scan().
#
def command_for_each_project(function):
    """
    A decorator to register one redcap-det subcommand per SCAN project, each
    calling the same base *function*.

    Used for side-effects only; the original *function* is unmodified.
    """
    for project in PROJECTS:
        help_message = "Process REDCap DETs for SCAN "
        if project.type:
            command_name = f"scan-{project.type}-{project.lang}"
            help_message += f"{project.type.upper()} ({project.lang})"
        else:
            command_name = f"scan-{project.lang}"
            help_message += f"({project.lang})"

        redcap_det.command_for_project(
            name = command_name,
            redcap_url = REDCAP_URL,
            project_id = project.id,
            required_instruments = REQUIRED_INSTRUMENTS,
            raw_coded_values = True,
            revision = REVISION,
            help = help_message)(function)

    return function

@command_for_each_project
def redcap_det_scan(*, db: DatabaseSession, cache: TTLCache, det: dict, redcap_record: REDCapRecord) -> Optional[dict]:
    # Add check for `enrollment_questionnaire` is complete because we cannot
    # include it in the top list of REQUIRED_INSTRUMENTS since the new
    # SCAN In-Person Enrollment project does not have this instrument.
    #   -Jover, 17 July 2020
    if is_complete('enrollment_questionnaire', redcap_record) == False:
        LOG.debug("Skipping enrollment with incomplete `enrollment_questionnaire` instrument")
        return None

    # Skip record if the illness_questionnaire is not complete, because this is
    # a "false" enrollment where the participant was not mailed a swab kit.
    # We must verify illness_questionnaire with the `illness_q_date` field
    # since there is a bug in REDCap that sometimes leaves the questionnaire marked incomplete/unverified.
    # We must have another check of the back_end_mail_scans because sometimes
    # the `illness_q_date` field is not filled in due to a bug in REDCap.
    # By verifying illness_questionnaire is complete first, we minimize the
    # delay in data ingestion since the back_end_mail_scans is completed the day after enrollment.
    #   -Jover, 29 June 2020

    # Add check for `illness_questionnaire` is complete because the new
    # SCAN In-Person Enrollment project does not have the `illness_q_date` field
    # and it does not have the `back_end_mail_scans` instrument.
    #   -Jover, 16 July 2020
    if not (redcap_record.get('illness_q_date') or
            is_complete('illness_questionnaire', redcap_record) or
            is_complete('back_end_mail_scans', redcap_record)):
        LOG.debug("Skipping incomplete enrollment")
        return None

    site_reference = create_site_reference(redcap_record)
    location_resource_entries = locations(db, cache, redcap_record)
    patient_entry, patient_reference = create_patient(redcap_record)

    if not patient_entry:
        LOG.warning("Skipping enrollment with insufficient information to construct patient")
        return None

    initial_encounter_entry, initial_encounter_reference = create_initial_encounter(
        redcap_record, patient_reference,
        site_reference, location_resource_entries)

    if not initial_encounter_entry:
        LOG.warning("Skipping enrollment with insufficient information to construct a initial encounter")
        return None

    initial_questionnaire_entry = create_initial_questionnaire_response(
        redcap_record, patient_reference, initial_encounter_reference)

    specimen_entry = None
    specimen_observation_entry = None
    # Mail in SCAN projects have `post_collection_data_entry_qc` instrument to
    # indicate a specimen is received. The SCAN In-Person Enrollmen project
    # does not have that instrument. So we rely on `nasal_swab_collection`
    # instrument to know that we have sample data to ingest.
    #   -Jover, 16 July 2020
    specimen_received = is_complete('post_collection_data_entry_qc', redcap_record) or \
                        is_complete('nasal_swab_collection', redcap_record)

    if specimen_received:
        specimen_entry, specimen_reference = create_specimen(redcap_record, patient_reference)
        specimen_observation_entry = create_specimen_observation_entry(
            specimen_reference, patient_reference, initial_encounter_reference)
    else:
        LOG.info("Creating encounter for record without sample")

    if specimen_received and not specimen_entry:
        LOG.warning("Skipping enrollment with insufficent information to construct a specimen")
        return None

    follow_up_encounter_entry = None
    follow_up_questionnaire_entry = None

    if is_complete('day_7_follow_up', redcap_record):
        # Follow-up encounter for 7 day follow-up survey
        follow_up_encounter_entry, follow_up_encounter_reference = create_follow_up_encounter(
            redcap_record, patient_reference, site_reference, initial_encounter_reference)
        follow_up_questionnaire_entry = create_follow_up_questionnaire_response(
        redcap_record, patient_reference, follow_up_encounter_reference)

    resource_entries = [
        patient_entry,
        initial_encounter_entry,
        initial_questionnaire_entry,
        specimen_entry,
        *location_resource_entries,
        specimen_observation_entry,
        follow_up_encounter_entry,
        follow_up_questionnaire_entry
    ]

    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{REDCAP_URL}{redcap_record.project.id}/{redcap_record['record_id']}",
        entries = list(filter(None, resource_entries))
    )


def create_site_reference(record: dict) -> Dict[str,dict]:
    """
    Create a Location reference for site of encounter.
    If `location_type` is available in *record*, then return site according
    to the provided location. Site for all other SCAN Encounters is 'SCAN'.
    """
    site = 'SCAN'

    record_location = record.get('location_type')
    if record_location:
        site = site_map(record_location)

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
        'greek': 'UWGreek'
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

    if record['housing_type'] in lodging_options:
        housing_type = 'lodging'
    else:
        housing_type = 'residence'

    address = {
        'street': record['home_street'],
        'secondary': record['apartment_number'],
        'city': record['homecity_other'],
        'state': record['home_state'],
        'zipcode': zipcode_map(record['home_zipcode_2']),
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


def zipcode_map(redcap_code: str) -> str:
    """
    Maps *redcap_code* to corresponding zip code. This is required because the
    branching logic in REDCap cauases major issues if the code is the
    actual zip code.
    """
    zipcode_map = {
        '44': '98101',
        '45': '98102',
        '46': '98103',
        '47': '98104',
        '48': '98105',
        '49': '98106',
        '50': '98107',
        '51': '98108',
        '52': '98109',
        '53': '98112',
        '54': '98115',
        '55': '98116',
        '56': '98117',
        '57': '98118',
        '58': '98119',
        '59': '98121',
        '60': '98122',
        '61': '98125',
        '62': '98126',
        '63': '98133',
        '64': '98134',
        '65': '98136',
        '66': '98144',
        '67': '98146',
        '69': '98154',
        '70': '98155',
        '71': '98164',
        '75': '98177',
        '76': '98178',
        '78': '98195',
        '80': '98199',
        '4':  '98004',
        '5':  '98005',
        '6':  '98006',
        '7':  '98007',
        '8':  '98008',
        '21': '98033',
        '22': '98034',
        '25': '98039',
        '26': '98040',
        '33': '98056',
        '36': '98059',
        '10': '98011',
        '17': '98028',
        '1':  '98001',
        '2':  '98002',
        '3':  '98003',
        '15': '98023',
        '18': '98030',
        '19': '98031',
        '20': '98032',
        '28': '98042',
        '31': '98047',
        '32': '98055',
        '34': '98057',
        '35': '98058',
        '68': '98148',
        '72': '98166',
        '73': '98168',
        '77': '98188',
        '79': '98198',
        '81': '98010',
        '82': '98022',
        '83': '98038',
        '84': '98045',
        '85': '98051',
        '86': '98065',
        '87': '98027',
        '88': '98029',
        '89': '98052',
        '90': '98074',
        '91': '98075',
        '92': '98092',
        '93': '98070',
        '94': '98014',
        '95': '98077',
        '96': '98053',
        '97': '98024',
        '98': '98072',
        '100': '98902',
        '101': '98908',
        '102': '98901',
        '103': '98944',
        '104': '98942',
        '105': '98930',
        '106': '98903',
        '107': '98951',
        '108': '98948',
        '109': '98953',
        '110': '98936',
        '111': '98932',
        '112': '98935',
        '113': '98937',
        '114': '98947',
        '115': '98952',
        '116': '98938',
        '117': '98923',
        '118': '98933',
        '119': '98921',
        '120': '98939',
    }

    if redcap_code not in zipcode_map:
        raise UnknownRedcapZipCode(f"Found unknown zip code REDCap code {redcap_code}")

    return zipcode_map[redcap_code]



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
    gender = map_sex(record['sex_new'])

    language_codeable_concept = create_codeable_concept(
        system = 'urn:ietf:bcp:47',
        code = LANGUAGE_CODE[record.project.id]
    )
    communication = [{
        'language' : language_codeable_concept,
        'preferred': True # Assumes that the project language is the patient's preferred language
    }]

    patient_id = generate_patient_hash(
        names       = (record['participant_first_name'], record['participant_last_name']),
        gender      = gender,
        birth_date  = record['birthday'],
        postal_code = record['home_zipcode_2'])

    if not patient_id:
        # Some piece of information was missing, so we couldn't generate a
        # hash.  Fallback to treating this individual as always unique by using
        # the REDCap record id.
        patient_id = generate_hash(f"{REDCAP_URL}{record.project.id}/{record['record_id']}")

    LOG.debug(f"Generated individual identifier {patient_id}")

    patient_identifier = create_identifier(f"{INTERNAL_SYSTEM}/individual", patient_id)
    patient_resource = create_patient_resource([patient_identifier], gender, communication)

    return create_entry_and_reference(patient_resource, "Patient")


def create_initial_encounter(record: REDCapRecord, patient_reference: dict, site_reference: dict, locations: list) -> tuple:
    """
    Returns a FHIR Encounter resource entry and reference for the initial
    encounter in the study (i.e. encounter of enrollment in the study)
    """

    def grab_symptom_keys(key: str, suffix: str='') -> Optional[Match[str]]:
        if record[key] == '1':
            return re.match(f'symptoms{suffix}___[a-z_]+$', key)
        else:
            return None

    def build_conditions_list(symptom: str, suffix: str='') -> dict:
        return create_resource_condition(record, symptom, patient_reference, suffix)

    def build_diagnosis_list(symptom: str, suffix: str='') -> Optional[dict]:
        mapped_symptom = map_symptom(symptom)
        if not mapped_symptom:
            return None

        return { "condition": { "reference": f"#{mapped_symptom}{suffix}" } }

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

    symptom_keys = list(filter(grab_symptom_keys, record))
    symptoms = list(map(lambda x: x.replace('symptoms___', ''), symptom_keys))
    contained = list(filter(None, map(build_conditions_list, symptoms)))
    diagnosis = list(filter(None, map(build_diagnosis_list, symptoms)))

    # Look for the follow up symptoms questions, labeled with suffix '_2'
    suffix = '_2'
    symptom_keys2 = list(filter(lambda key: grab_symptom_keys(key, suffix), record))
    if symptom_keys2:
        symptoms2 = list(map(lambda x: x.replace(f'symptoms{suffix}___', ''), symptom_keys2))

        contained += list(filter(None, map(lambda x: build_conditions_list(x, suffix), symptoms2)))
        diagnosis += list(filter(None, map(lambda x: build_diagnosis_list(x, suffix), symptoms2)))

    encounter_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/encounter",
        value = f"{REDCAP_URL}{record.project.id}/{record['record_id']}"
    )
    encounter_class_coding = create_coding(
        system = "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code = "HH"
    )

    # YYYY-MM-DD in REDCap
    # SCAN: In-Person Enrollments do not have the `enrollment_date` field.
    # For in person enrollments, the consent date is the same as enrollment date.
    # -Jover, 16 July 2020
    encounter_date = record.get('enrollment_date') or record.get('consent_date')
    if not encounter_date:
        return None, None

    non_tracts = list(filter(non_tract_locations, locations))
    non_tract_references = list(map(build_locations_list, non_tracts))
    # Add hard-coded site Location reference
    non_tract_references.append(site_reference)

    encounter_resource = create_encounter_resource(
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class_coding,
        encounter_date = encounter_date,
        patient_reference = patient_reference,
        location_references = non_tract_references,
        diagnosis = diagnosis,
        contained = contained
    )

    return create_entry_and_reference(encounter_resource, "Encounter")


def create_resource_condition(record: dict, symptom_name: str, patient_reference: dict, suffix: str='') -> Optional[dict]:
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
        "id": f'{mapped_symptom_name}{suffix}',
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

    symptom_duration = record.get(f'symptom_duration{suffix}')

    if symptom_duration:
        condition["onsetDateTime"] = symptom_duration

    return condition


def create_specimen(record: dict, patient_reference: dict) -> tuple:
    """ Returns a FHIR Specimen resource entry and reference """
    def specimen_barcode() -> Optional[str]:
        """
        Return specimen barcode from REDCap record.
        """
         # Normalize all barcode fields upfront.
        barcode_fields = {
            "return_utm_barcode",
            "utm_tube_barcode_2",
            "reenter_barcode",
            "reenter_barcode_2"}

        for barcode_field in barcode_fields:
            if barcode_field in record:
                record[barcode_field] = record[barcode_field].strip().lower()

        # The `return_utm_barcode` field is most reliable in our mail-in kits
        # because this is the barcode that gets scanned during unboxing.
        # If the field exists in the record, then use its value.
        # - Jover, 22 July 2020
        if 'return_utm_barcode' in record:
            return record['return_utm_barcode']

        # SCAN In-Person Enrollments project does not have the `return_utm_barcode`
        # field, so use `utm_tube_barcode_2` instead.
        # -Jover, 16 July 2020
        barcode = record.get('utm_tube_barcode_2')
        manual_barcodes_match = (record['reenter_barcode'] == record['reenter_barcode_2'])

        # If the `utm_tube_barcode_2` field is blank, use the manually
        # entered barcode in `reenter_barcode` field. If this doesn't match
        # the value in `reenter_barcode_2` then return None to err on the
        # side of caution.
        # -Jover, 22 July 2020
        if not barcode:
            if manual_barcodes_match:
                barcode = record['reenter_barcode']
            else:
                barcode = None

        return barcode

    barcode = specimen_barcode()

    if not barcode:
        LOG.warning("Could not create Specimen Resource due to lack of barcode.")
        return None, None

    specimen_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/sample",
        value = barcode
    )

    # YYYY-MM-DD in REDCap
    # The `collection_date` field is being removed from the SCAN REDCap projects
    # on 22 July 2020.
    #   -Jover, 16 July 2020.
    collected_time = record.get('collection_date')

    # YYYY-MM-DD HH:MM:SS in REDCap
    # `samp_process_date`field does not exist in new SCAN In-Person Enrollments
    #   -Jover, 17 July 2020
    received_time = record['samp_process_date'].split()[0] if record.get('samp_process_date') else None

    note = None

    if record['able_to_test'] == 'no':
        note = 'never-tested'
    # Assumes that all samples can be tested unless explicitly marked "no".
    #   - Jover 09 April 2020
    else:
        note = 'can-test'

    specimen_type = 'NSECR'  # Nasal swab.  TODO we may want shared mapping function
    specimen_resource = create_specimen_resource(
        [specimen_identifier], patient_reference, specimen_type, received_time,
        collected_time, note
    )

    return create_entry_and_reference(specimen_resource, "Specimen")


def create_initial_questionnaire_response(record: dict, patient_reference: dict,
                                          encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the initial
    encounter (i.e. encounter of enrollment into the study)
    """
    def combine_multiple_fields(field_prefix: str) -> Optional[List]:
        """
        Handles the combining of multiple fields asking the same question such
        as country and state traveled.
        """
        regex = rf'^{re.escape(field_prefix)}[0-9]+$'
        empty_value = ''
        answered_fields = list(filter(lambda f: filter_fields(f, record[f], regex, empty_value), record))

        if not answered_fields:
            return None

        return list(map(lambda x: record[x], answered_fields))

    coding_questions = [
        'race'
    ]

    boolean_questions = [
        'ethnicity',
        'pregnant_yesno',
        'travel_countries_phs',
        'travel_states_phs',
        'prior_test',
    ]

    integer_questions = [
        'age',
        'age_months',
        'prior_test_number',
    ]

    string_questions = [
        'redcap_event_name',
        'priority_code',
        'income',
        'housing_type',
        'house_members',
        'doctor_3e8fae',
        'hospital_where',
        'hospital_ed',
        'smoke_9a005a',
        'chronic_illness',
        'overall_risk_health',
        'overall_risk_setting',
        'longterm_type',
        'country',
        'state',
        'ace',
        'website_id',
        'prior_test_positive',
        'prior_test_type',
        'prior_test_result',
        'contact',
        'wash_hands',
        'clean_surfaces',
        'hide_cough',
        'mask',
        'distance',
        'attend_event',
        'wfh',
        'industry',
    ]

    date_questions = [
        'illness_q_date',
        'hospital_arrive',
        'hospital_leave',
        'prior_test_positive_date',
    ]

    question_categories = {
        'valueCoding': coding_questions,
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueString': string_questions,
        'valueDate': date_questions,
    }

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'race',
        'doctor_3e8fae',
        'smoke_9a005a',
        'chronic_illness',
        'overall_risk_health',
        'overall_risk_setting',
        'longterm_type',
        'ace',
        'prior_test_positive',
        'prior_test_type',
        'contact',
        'industry',
    ]
    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    # Combine all fields answering the same question
    record['country'] = combine_multiple_fields('country')
    record['state'] = combine_multiple_fields('state')

    # Age Ceiling
    try:
        record['age'] = age_ceiling(int(record['age']))
        record['age_months'] = age_ceiling(int(record['age_months']) / 12) * 12
    except ValueError:
        record['age'] = record['age_months'] = None

    return questionnaire_response(record, question_categories, patient_reference, encounter_reference)


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


def questionnaire_response(record: dict,
                           question_categories: Dict[str, list],
                           patient_reference: dict,
                           encounter_reference: dict) -> Optional[dict]:
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

    if items:
        questionnaire_reseponse_resource = create_questionnaire_response_resource(
            patient_reference, encounter_reference, items
        )
        full_url = generate_full_url_uuid()
        return create_resource_entry(questionnaire_reseponse_resource, full_url)

    return None


def questionnaire_item(record: dict, question_id: str, response_type: str) -> Optional[dict]:
    """ Creates a QuestionnaireResponse internal item from a REDCap record. """
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

    def cast_to_boolean(string: str) -> Optional[bool]:
        if string == 'yes':
            return True
        elif string == 'no':
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
        value = f"{REDCAP_URL}{record.project.id}/{record['record_id']}_follow_up"
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

    Note: `fu_which_activites` and `fu_missed_activites` are misspelled on
    purpose to match the misspelling of the fields in the REDCap project.
    """
    boolean_questions = [
        'fu_illness',
        'fu_change',
        'fu_feel_normal',
        'fu_household_sick',
        'result_changes',
    ]

    integer_questions = [
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
        'fu_care',
        'fu_hospital_where',
        'fu_hospital_ed',
        'fu_work_school',
        'fu_activities',
        'fu_which_activites',
        'fu_missed_activites',
        'fu_test_result',
        'fu_behaviors_no',
        'fu_behaviors_inconclusive',
        'fu_behaviors',
        'fu_1_symptoms',
        'fu_1_test',
        'fu_1_result',
        'fu_2_symptoms',
        'fu_2_test',
        'fu_2_result',
        'fu_3_symptoms',
        'fu_3_test',
        'fu_3_result',
        'fu_4_symptoms',
        'fu_4_test',
        'fu_4_result',
        'fu_healthy_test',
        'fu_healthy_result'
    ]

    date_questions = [
        'fu_symptom_duration',
        'fu_date_care',
        'fu_1_date',
        'fu_2_date',
        'fu_3_date',
        'fu_4_date',
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
        'fu_missed_activites',
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

    return questionnaire_response(record, question_categories, patient_reference, encounter_reference)


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
