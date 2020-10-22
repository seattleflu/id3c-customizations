"""
Functions shared by REDCap DET ETL
"""
from datetime import datetime
from enum import Enum
import re
from typing import Dict, List, Mapping, Match, Optional, Tuple, Union

from cachetools import TTLCache

from . import race
from .fhir import *
from .redcap_map import map_sex, map_symptom, UnknownVaccineResponseError
from id3c.cli.command.geocode import get_response_from_cache_or_geocoding
from id3c.cli.command.location import location_lookup
from id3c.cli.redcap import Record as REDCapRecord
from id3c.db.session import DatabaseSession
import logging


LOG = logging.getLogger(__name__)

# See https://terminology.hl7.org/1.0.0/CodeSystem-v3-ActCode.html for
    # possible collection codes.
    # HH = 'home health'
    # FLD = 'field'
class CollectionCode(Enum):
    HOME_HEALTH = "HH"
    FIELD = "FLD"


def normalize_net_id(net_id: str=None) -> Optional[str]:
    """
    If netid or UW email provided, return netid@washington.edu

    >>> normalize_net_id('abcd')
    'abcd@washington.edu'

    >>> normalize_net_id('aBcD ')
    'abcd@washington.edu'

    >>> normalize_net_id('abcd@uw.edu')
    'abcd@washington.edu'

    >>> normalize_net_id('aBcD@u.washington.edu')
    'abcd@washington.edu'

    If missing netid or non-uw email provided, it cannot be normalized so return nothing
    >>> normalize_net_id('notuw@gmail.com')
    >>> normalize_net_id('nodomain@')
    >>> normalize_net_id('multiple@at@signs')
    >>> normalize_net_id()
    >>> normalize_net_id('')
    >>> normalize_net_id(' ')
    """

    if not net_id or net_id.isspace():
        return None

    net_id = net_id.strip().lower()

    # if a uw email was entered, drop the domain before normalizing
    if net_id.count('@') == 1:
        net_id, domain = net_id.split('@')
        if domain not in ['u.washington.edu', 'uw.edu']:
            return None
    elif net_id.count('@') > 1:
        return None

    username = f'{net_id}@washington.edu'
    return username


def parse_date_from_string(input_string: str)-> Optional[datetime]:
    """ Returns a date from a given *input_string* as a datetime
    object if the value can be parsed.
    Otherwise, emits a debug log entry and returns None.

    >>> parse_date_from_string('2000-2-12')
    datetime.datetime(2000, 2, 12, 0, 0)

    >>> parse_date_from_string('abc')

    >>> parse_date_from_string(None)
    """
    date = None

    if input_string:
        try:
            date = datetime.strptime(input_string, '%Y-%m-%d')
        except ValueError:
            LOG.debug(f"Invalid date value.")

    return date


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


def census_tract(db: DatabaseSession, lat_lng: Tuple[float, float],
        location_type: str, system_identifier: str) -> Optional[dict]:
    """
    Creates a new Location Resource for the census tract containing the given
    *lat_lng* coordintes and associates it with the given *location_type*.
    """
    location = location_lookup(db, lat_lng, 'tract')

    if location and location.identifier:
        return create_location(
            f"{system_identifier}/location/tract", location.identifier, location_type
        )
    else:
        LOG.debug("No census tract found for given location.")
        return None


def build_residential_location_resources(db: DatabaseSession, cache: TTLCache, housing_type: str,
        primary_street_address: str, secondary_street_address: str, city: str, state: str,
        zipcode: str, system_identifier: str) -> list:
    """ Creates a list of residential Location resource entries. """

    lodging_options = [
        'shelter',
        'afl',
        'snf',
        'ltc',
        'be',
        'pst',
        'cf',
        'none',
    ]

    if housing_type in lodging_options:
        housing_type = 'lodging'
    else:
        housing_type = 'residence'

    address = {
        'street': primary_street_address,
        'secondary': secondary_street_address,
        'city': city,
        'state': state,
        'zipcode': zipcode
    }

    lat, lng, canonicalized_address = get_response_from_cache_or_geocoding(address, cache)
    if not canonicalized_address:
        return []  # TODO

    tract_location = census_tract(db, (lat, lng), housing_type, system_identifier)
    # TODO what if tract_location is null?
    tract_full_url = generate_full_url_uuid()
    tract_entry = create_resource_entry(tract_location, tract_full_url)

    address_hash = generate_hash(canonicalized_address)
    address_location = create_location(
        f"{system_identifier}/location/address",
        address_hash,
        housing_type,
        tract_full_url
    )
    address_entry = create_resource_entry(address_location, generate_full_url_uuid())

    return [tract_entry, address_entry]


def create_site_reference(default_site: str, system_identifier: str,
    location: str = None, site_map: dict = None) -> Optional[Dict[str,dict]]:
    """
    Create a Location reference for site of the sample collection encounter based
    on how the sample was collected.
    """
    class UnknownRedcapRecordLocation(ValueError):
        """
        Raised if a provided *location* is not
        among a set of expected values.
        """
        pass

    if location and site_map:
        if location not in site_map:
            raise UnknownRedcapRecordLocation(f"Found unknown location type «{location}»")
        site = site_map[location]
    else:
        site = default_site

    return {
        "location": create_reference(
            reference_type = "Location",
            identifier = create_identifier(f"{system_identifier}/site", site)
        )
    }


def _create_patient(sex: str, preferred_language: str, record: REDCapRecord,
        system_identifier: str, first_name: str = None, last_name: str = None,
        birth_date: str = None, zipcode: str = None, unique_identifier: str = None) -> tuple:
    """
    Returns a FHIR Patient resource entry and reference.
    Uses demographics to create the patient identifier unless
    a *unique_identifier* is provided.
    """

    if not unique_identifier and (not first_name or not last_name or not birth_date \
        or not zipcode):
        LOG.debug('If you are not providing a `unique_identifier` you should provide' \
            +' `first_name`, `last_name`, `birth_date`, and `zipcode`')

    gender = map_sex(sex)

    if preferred_language:
        language_codeable_concept = create_codeable_concept(
            system = 'urn:ietf:bcp:47',
            code = preferred_language
        )

        communication = [{
            'language' : language_codeable_concept,
            'preferred': True
        }]
    else:
        communication = []

    patient_id = None
    if unique_identifier:
        patient_id = generate_hash(unique_identifier)
    else:
        patient_id = generate_patient_hash(
                names       = (first_name, last_name),
                gender      = gender,
                birth_date  = birth_date,
                postal_code = zipcode)

    if not patient_id:
        # Some piece of information was missing, so we couldn't generate a
        # hash.  Fallback to treating this individual as always unique by using
        # the REDCap record id.
        patient_id = generate_hash(f"{record.project.base_url}{record.project.id}/{record.id}")

    LOG.debug(f"Generated individual identifier {patient_id}")

    patient_identifier = create_identifier(f"{system_identifier}/individual", patient_id)
    patient_resource = create_patient_resource([patient_identifier], gender, communication)

    return create_entry_and_reference(patient_resource, "Patient")


def create_patient_using_demographics(sex: str, preferred_language: str, first_name: str, last_name: str,
        birth_date: str, zipcode: str, record: REDCapRecord, system_identifier: str) -> tuple:
    """
    Returns a FHIR Patient resource entry and reference.
    Uses demographics to create the patient identifier
    """
    return _create_patient(
        sex = sex,
        preferred_language = preferred_language,
        first_name = first_name,
        last_name = last_name,
        birth_date = birth_date,
        zipcode = zipcode,
        record = record,
        system_identifier = system_identifier)


def create_patient_using_unique_identifier(sex: str, preferred_language: str, unique_identifier: str,
    record: REDCapRecord, system_identifier: str) -> tuple:
    """
    Returns a FHIR Patient resource entry and reference.
    Uses a unique identifier to create the patient identifier
    """
    return _create_patient(
        sex = sex,
        preferred_language = preferred_language,
        record = record,
        system_identifier = system_identifier,
        unique_identifier = unique_identifier)


def create_specimen(prioritized_barcodes: List[str], patient_reference: dict, collection_date: str, sample_received_time: str,
    able_to_test: str, system_identifier: str, specimen_type: Optional[str] = None) -> tuple:
    """ Returns a FHIR Specimen resource entry and reference
        Uses the first non-empty barcode from *prioritized_barcodes*
    """

    for barcode in prioritized_barcodes:
        prepared_barcode = barcode.strip().lower()
        if prepared_barcode:
            break

    if not prepared_barcode:
        LOG.debug("Could not create Specimen Resource due to lack of barcode.")
        return None, None

    specimen_identifier = create_identifier(
        system = f"{system_identifier}/sample",
        value = prepared_barcode
    )

    # YYYY-MM-DD HH:MM:SS in REDCap
    received_time = sample_received_time.split()[0] if sample_received_time else None

    note = None

    if able_to_test == 'no':
        note = 'never-tested'
    else:
        note = 'can-test'

    specimen_type = specimen_type or 'NSECR'  # Nasal swab.
    specimen_resource = create_specimen_resource(
        [specimen_identifier], patient_reference, specimen_type, received_time,
        collection_date, note
    )

    return create_entry_and_reference(specimen_resource, "Specimen")


def build_contained_and_diagnosis(patient_reference: dict, record: REDCapRecord,
        symptom_onset_map: dict, system_identifier: str) -> Tuple[list, list]:

    def grab_symptom_key(record: REDCapRecord, key: str, variable_name: str) -> Optional[Match[str]]:
        if record[key] == '1':
            return re.match(f"{variable_name}___[a-z_]+", key)
        else:
            return None


    def build_condition(patient_reference: dict, symptom_name: str, onset_date: str,
        system_identifier: str) -> Optional[dict]:
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
                        "system": f"{system_identifier}/symptom",
                        "code": mapped_symptom_name
                    }
                ]
            },
            "subject": patient_reference
        }

        if onset_date:
            condition["onsetDateTime"] = onset_date

        return condition


    def build_diagnosis(symptom: str) -> Optional[dict]:
        mapped_symptom = map_symptom(symptom)
        if not mapped_symptom:
            return None

        return { "condition": { "reference": f"#{mapped_symptom}" } }


    contained = []
    diagnosis = []

    for symptom_variable in symptom_onset_map:
            symptom_keys = []

            for redcap_key in record.keys():
                symptom_key = grab_symptom_key(record, redcap_key, symptom_variable)
                if symptom_key:
                    symptom_keys.append(symptom_key.string)

            symptoms = list(map(lambda x: re.sub('[a-z_]+___', '', x), symptom_keys))

            for symptom in symptoms:
                contained.append(build_condition(patient_reference, symptom, symptom_onset_map[symptom_variable], system_identifier))
                diagnosis.append(build_diagnosis(symptom))

    return contained, diagnosis


def follow_up_encounter_reason_code() -> dict:
    encounter_reason_code = create_codeable_concept(
        system = "http://snomed.info/sct",
        code = "390906007",
        display = "Follow-up encounter"
    )
    return encounter_reason_code


def create_encounter(encounter_date: str, patient_reference: dict, site_reference: dict,
    collection_code: CollectionCode, encounter_id: str, record: REDCapRecord,
    system_identifier: str, locations: list = None, diagnosis: list = None,
    contained: list = None, parent_encounter_reference: dict = None,
    encounter_reason_code: dict = None, encounter_identifier_suffix: str = None) -> tuple:
    """
    Returns a FHIR Encounter resource entry and reference for the encounter in the study.
    """

    def build_locations_list(location: dict) -> dict:
        return {
            "location": create_reference(
                reference_type = "Location",
                reference = location["fullUrl"]
            )
        }


    def non_tract_locations(resource: dict):
        return bool(resource) \
            and resource['resource']['identifier'][0]['system'] != f"{system_identifier}/location/tract"


    if not encounter_date:
        LOG.debug("Not creating the encounter because there is no encounter_date.")
        return None, None

    if not site_reference:
        LOG.debug("Not creating the encounter because there is no site_reference.")
        return None, None

    encounter_identifier = create_identifier(
        system = f"{system_identifier}/encounter",
        value = encounter_id
    )

    collection_code_value = None
    if collection_code:
        collection_code_value = collection_code.value

    encounter_class_coding = create_coding(
        system = "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code = collection_code_value
    )

    site_reference_list = [site_reference]

    # Add hard-coded site Location reference
    if locations:
        non_tracts = list(filter(non_tract_locations, locations))
        non_tract_references = list(map(build_locations_list, non_tracts))
        site_reference_list.extend(non_tract_references)

    reason_code_list = None
    if encounter_reason_code:
        reason_code_list = [encounter_reason_code]


    encounter_resource = create_encounter_resource(
        encounter_source = create_redcap_uri(record),
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class_coding,
        encounter_date = encounter_date,
        patient_reference = patient_reference,
        location_references = site_reference_list,
        diagnosis = diagnosis,
        contained = contained,
        reason_code = reason_code_list,
        part_of = parent_encounter_reference
    )

    return create_entry_and_reference(encounter_resource, "Encounter")


def filter_fields(field: str, field_value: str, regex: str, empty_value: str) -> bool:
    """
    Function that filters for *field* matching given *regex* and the
    *field_value* must not equal the expected *empty_value.
    """
    if re.match(regex, field) and field_value != empty_value:
        return True

    return False


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
        '1': True,
        'no': False,
        '0': False,
        'dont_know': None,
        '': None
    }

    if vaccine_response.lower() not in vaccine_map:
        raise UnknownVaccineResponseError(f"Unknown vaccine response «{vaccine_response}»")

    return vaccine_map[vaccine_response.lower()]


def create_vaccine_item(vaccine_status: str, vaccine_year: str, vaccine_month: str, dont_know_text: str) -> Optional[dict]:
    """
    Return a questionnaire response item with the vaccine response(s) encoded.
    """
    def vaccine_date(vaccine_year: str, vaccine_month: str, dont_know_text: str) -> Optional[str]:
        """ Converts a vaccination date to 'YYYY' or 'YYYY-MM' format. """
        if vaccine_year == '' or vaccine_year == dont_know_text:
            return None

        if vaccine_month == dont_know_text:
            return datetime.strptime(vaccine_year, '%Y').strftime('%Y')

        return datetime.strptime(f'{vaccine_month} {vaccine_year}', '%B %Y').strftime('%Y-%m')


    vaccine_status_bool = map_vaccine(vaccine_status)
    if vaccine_status_bool is None:
        return None

    answers: List[Dict[str, Any]] = [{ 'valueBoolean': vaccine_status_bool }]

    date = vaccine_date(vaccine_year, vaccine_month, dont_know_text)
    if vaccine_status_bool and date:
        answers.append({ 'valueDate': date })

    return create_questionnaire_response_item('vaccine', answers)


def questionnaire_item(record: REDCapRecord, question_id: str, response_type: str, system_identifier: str) -> Optional[dict]:
    """ Creates a QuestionnaireResponse internal item from a REDCap record.
    """
    response = record.get(question_id)
    if not response:
        return None


    def cast_to_coding(string: str) -> dict:
        """ Currently the only QuestionnaireItem we code is race. """
        return create_coding(
            system = f"{system_identifier}/race",
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


def create_questionnaire_response(record: REDCapRecord, question_categories: Dict[str, list],
    patient_reference: dict, encounter_reference: dict, system_identifier: str,
    additional_items: Optional[List[dict]] = None) -> Optional[dict]:
    """
    Provided a dictionary of *question_categories* with the key being the value
    type and the value being a list of field names, return a FHIR
    Questionnaire Response resource entry. To the list of items built by
    processing the *question_categories*, add *additional_items* if there are any.
    """
    def build_questionnaire_items(question: str) -> Optional[dict]:
        return questionnaire_item(record, question, category, system_identifier)

    items: List[dict] = []
    for category in question_categories:
        category_items = list(map(build_questionnaire_items, question_categories[category]))
        for item in category_items:
            if item:
                items.append(item)

    if additional_items:
        items.extend(additional_items)

    if items:
        questionnaire_reseponse_resource = create_questionnaire_response_resource(
            patient_reference, encounter_reference, items
        )
        full_url = generate_full_url_uuid()
        return create_resource_entry(questionnaire_reseponse_resource, full_url)

    return None
