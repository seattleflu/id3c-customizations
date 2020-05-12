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

PROJECTS = [
    ScanProject(20759, "en"),
    ScanProject(21520, "es"),
    ScanProject(21512, "vi"),
    ScanProject(21514, "zh-Hans"),
    ScanProject(21521, "zh-Hant"),
    ScanProject(21809, "so"),
    ScanProject(21810, "ko"),
    ScanProject(21808, "ru"),
]

REVISION = 10

REDCAP_URL = 'https://redcap.iths.org/'
INTERNAL_SYSTEM = "https://seattleflu.org"

LANGUAGE_CODE = {
    project.id: project.lang
        for project in PROJECTS }

REQUIRED_INSTRUMENTS = [
    'consent_form',
    'enrollment_questionnaire',
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
        redcap_det.command_for_project(
            f"scan-{project.lang}",
            redcap_url = REDCAP_URL,
            project_id = project.id,
            required_instruments = REQUIRED_INSTRUMENTS,
            raw_coded_values = True,
            revision = REVISION,
            help = f"Process REDCap DETs for SCAN ({project.lang})")(function)

    return function

@command_for_each_project
def redcap_det_scan(*, db: DatabaseSession, cache: TTLCache, det: dict, redcap_record: REDCapRecord) -> Optional[dict]:
    location_resource_entries = locations(db, cache, redcap_record)
    patient_entry, patient_reference = create_patient(redcap_record)

    if not patient_entry:
        LOG.warning("Skipping enrollment with insufficient information to construct patient")
        return None

    encounter_entry, encounter_reference = create_encounter(redcap_record, patient_reference, location_resource_entries)

    if not encounter_entry:
        LOG.warning("Skipping enrollment with insufficient information to construct an encounter")
        return None

    questionnaire_entry = create_questionnaire_response(redcap_record, patient_reference, encounter_reference)

    specimen_entry = None
    specimen_observation_entry = None
    specimen_received = is_complete('post_collection_data_entry_qc', redcap_record)

    if specimen_received:
        specimen_entry, specimen_reference = create_specimen(redcap_record, patient_reference)
        specimen_observation_entry = create_specimen_observation_entry(specimen_reference, patient_reference, encounter_reference)
    else:
        LOG.info("Creating encounter for record without sample")

    if specimen_received and not specimen_entry:
        LOG.warning("Skipping enrollment with insufficent information to construct a specimen")
        return None

    resource_entries = [
        patient_entry,
        encounter_entry,
        questionnaire_entry,
        specimen_entry,
        *location_resource_entries,
        specimen_observation_entry
    ]

    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{REDCAP_URL}{redcap_record.project.id}/{redcap_record['record_id']}",
        entries = list(filter(None, resource_entries))
    )


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
        '93': '98070'
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


def create_encounter(record: REDCapRecord, patient_reference: dict, locations: list) -> tuple:
    """ Returns a FHIR Encounter resource entry and reference """

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
    encounter_date = record['enrollment_date']
    if not encounter_date:
        return None, None

    non_tracts = list(filter(non_tract_locations, locations))
    non_tract_references = list(map(build_locations_list, non_tracts))
    # Site for all SCAN Encounters is 'scan'
    site_reference = {
        "location": create_reference(
            reference_type = "Location",
            identifier = create_identifier(f"{INTERNAL_SYSTEM}/site", 'SCAN')
        )
    }
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
    barcode = record['return_utm_barcode']
    if not barcode:
        LOG.warning("Could not create Specimen Resource due to lack of barcode.")
        return None, None

    specimen_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/sample",
        value = barcode
    )

    # YYYY-MM-DD in REDCap
    collected_time = record['collection_date'] or None

    # YYYY-MM-DD HH:MM:SS in REDCap
    received_time = record['samp_process_date'].split()[0] if record['samp_process_date'] else None

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


def create_questionnaire_response(record: dict, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """ Returns a FHIR Questionnaire Response resource entry. """

    def combine_checkbox_answers(coded_question: str) -> Optional[List]:
        """
        Handles the combining "select all that apply"-type checkbox
        responses into one list.

        Uses our in-house mapping for race.
        """
        regex = rf'{re.escape(coded_question)}___[\w]*$'
        empty_value = '0'
        answered_checkboxes = list(filter(lambda f: filter_fields(f, regex, empty_value), record))
        # REDCap checkbox fields have format of {question}___{answer}
        answers = list(map(lambda k: k.replace(f"{coded_question}___", ""), answered_checkboxes))

        if coded_question == 'race':
            return race(answers)

        return answers


    def filter_fields(field: str, regex: str, empty_value: str) -> bool:
        """
        Function that filters answered fields matching given *regex*
        """
        if re.match(regex, field) and record[field] != empty_value:
            return True

        return False


    def combine_multiple_fields(field_prefix: str) -> Optional[List]:
        """
        Handles the combining of multiple fields asking the same question such
        as country and state traveled.
        """
        regex = rf'^{re.escape(field_prefix)}[0-9]+$'
        empty_value = ''
        answered_fields = list(filter(lambda f: filter_fields(f, regex, empty_value), record))

        if not answered_fields:
            return None

        return list(map(lambda x: record[x], answered_fields))


    def build_questionnaire_items(question: str) -> Optional[dict]:
        return questionnaire_item(record, question, category)

    coding_questions = [
        'race'
    ]

    boolean_questions = [
        'ethnicity',
        'pregnant_yesno',
        'travel_countries_phs',
        'travel_states_phs',
    ]

    integer_questions = [
        'age',
        'age_months',
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
    ]

    date_questions = [
        'hospital_arrive',
        'hospital_leave',
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
    ]
    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(field)

    # Combine all fields answering the same question
    record['country'] = combine_multiple_fields('country')
    record['state'] = combine_multiple_fields('state')

    # Age Ceiling
    record['age'] = age_ceiling(int(record['age']))
    record['age_months'] = age_ceiling(int(record['age_months']) / 12) * 12

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


class UnknownRedcapZipCode(ValueError):
    """
    Raised by :function: `zipcode_map` if a provided *redcap_code* is not
    among a set of expected values.
    """
    pass
