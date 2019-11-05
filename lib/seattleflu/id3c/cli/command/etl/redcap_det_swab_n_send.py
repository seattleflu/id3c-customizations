"""
Process REDCap DET documents into the relational warehouse.

Contains some hard-coded logic for which project ID correlates to which project
(e.g. 17421 is the PID for Shelters)
"""
import os
import re
import click
import json
import hashlib
import logging
from uuid import uuid4
from typing import Any, Callable, Dict, List, Mapping, Match, Optional, Union
from datetime import datetime
from id3c.cli.command.etl import race, redcap_det
from id3c.cli.command.clinical import generate_hash
from .redcap_map import *
from .fhir import *


LOG = logging.getLogger(__name__)


REVISION = 1

REDCAP_URL = 'https://redcap.iths.org/'
INTERNAL_SYSTEM = "https://seattleflu.org"
UW_CENSUS_TRACT = '53033005302'

PROJECT_ID = 17561  # TODO use '17421' in production
REQUIRED_INSTRUMENTS = [
    # 'consent',   # TODO use in production
    'enrollment_questionnaire',
    'back_end_mail_scans',
    # 'illness_questionnaire_nasal_swab_collection',  # TODO use in production
    'post_collection_data_entry_qc'
]

@redcap_det.command_for_project(
    "swab-n-send",
    redcap_url = REDCAP_URL,
    project_id = PROJECT_ID,
    required_instruments = REQUIRED_INSTRUMENTS,
    revision = REVISION,
    help = __doc__)

def redcap_det_swab_n_send(*, det: dict, redcap_record: dict) -> Optional[dict]:
    location_resource_entries = locations(redcap_record)
    patient_entry,patient_reference = create_patient(redcap_record)
    encounter_entry,encounter_reference = create_encounter(redcap_record, patient_reference, location_resource_entries)
    questionnaire_entry = create_questionnaire_response(redcap_record, patient_reference, encounter_reference)
    specimen_entry, specimen_reference = create_specimen(redcap_record, patient_reference)
    specimen_observation_entry = create_resource_entry(
        resource = create_specimen_observation(specimen_reference,patient_reference,encounter_reference),
        full_url = generate_full_url_uuid()
    )

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
        entries = resource_entries
    )


def locations(record: dict) -> list:
    """ Creates a list of Location resource entries from a REDCap record. """
    def uw_affiliation(record: dict) -> List[Dict[Any, Any]]:
        uw_affiliation = record['uw_affiliation']

        uw_locations = []
        if uw_affiliation in ['1', '2']:
            uw_locations.append(
                create_location(f"{INTERNAL_SYSTEM}/location", UW_CENSUS_TRACT, "school"))

        if uw_affiliation in ['2', '3', '4']:
            uw_locations.append(
                create_location(f"{INTERNAL_SYSTEM}/location", UW_CENSUS_TRACT, "work"))

        return uw_locations

    def housing(record: dict) -> tuple:
        lodging_options = [
            'Shelter',
            'Assisted living facility',
            'Skilled nursing center',
            'No consistent primary residence'
        ]

        if record['housing_type'] in lodging_options:
            housing_type = 'lodging'
        else:
            housing_type = 'residence'

        # TODO census tract
        if record['home_country'] == 'US':
            address = {
                'street1': record['home_street'],
                'city': record['homecity_other'],
                'state': record['home_state_55ec63'],
                'country': record['home_country'],
                'zipcode': record['home_zipcode_2'],
            }

        tract_location = create_location(
            f"{INTERNAL_SYSTEM}/locations/tract", '#TODO CENSUS TRACT', housing_type
        )

        tract_full_url = generate_full_url_uuid()
        tract_entry = create_resource_entry(tract_location, tract_full_url)

        address_location = create_location(
            f"{INTERNAL_SYSTEM}/locations/address",
            '#TODO ADDRESS HASH',
            housing_type,
            tract_full_url
        )

        address_entry = create_resource_entry(address_location, generate_full_url_uuid())

        return tract_entry, address_entry

    locations = [
        create_resource_entry(
            create_location(f"{INTERNAL_SYSTEM}/site", 'self-test', "site"),
            generate_full_url_uuid()
        ),
        *housing(record)
    ]

    uw_location = uw_affiliation(record)
    for location in uw_location:
        if location:
            locations.append(create_resource_entry(location, generate_full_url_uuid()))

    return locations


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

    location_type = create_codeable_concept(location_type_system,
                                            location_type_map[location_type])
    location_identifier = create_identifier(system, value)
    part_of = None
    if parent:
        part_of = create_reference(reference_type="Location", reference=parent)

    return create_location_resource([location_type], [location_identifier], part_of)


def create_patient(record: dict) -> tuple:
    """ Returns a FHIR Patient resource entry and reference. """
    gender = map_sex(record["sex"])
    patient_id = generate_patient_hash(record, gender)
    patient_identifier = create_identifier(f"{INTERNAL_SYSTEM}/individual",patient_id)
    patient_resource = create_patient_resource([patient_identifier], gender)

    return create_entry_and_reference(patient_resource, "Patient")


def generate_patient_hash(record: dict, gender: str) -> dict:
    """ Returns a hash generated from patient information. """
    personal_information = {
        "name": canonicalize_name(f"{record['first_name_1']}{record['last_name_1']}"),
        "gender": gender,
        "birthday": convert_to_iso(record['birthday'], '%Y-%m-%d'),
        "zipcode": record['home_zipcode_2']  # TODO redundant?
    }

    return generate_hash(str(sorted(personal_information.items())))


def create_encounter(record: dict, patient_reference: dict, locations: list) -> tuple:
    """ Returns a FHIR Encounter resource entry and reference """

    def grab_symptom_keys(key: str) -> Optional[Match[str]]:
        if record[key] != '':
            return re.match('symptoms(_child)?___[0-9]{1,3}$', key)
        else:
            return None

    def build_conditions_list(symptom_key: str) -> dict:
        return create_resource_condition(record, record[symptom_key], patient_reference)

    def build_diagnosis_list(symptom_key: str) -> dict:
        return { "condition": { "reference": f"#{map_symptom(record[symptom_key])}" } }

    def build_locations_list(location: dict) -> dict:
        return {
            "location": create_reference(
                reference_type = "Location",
                reference = location["fullUrl"]
            )
        }

    symptom_keys = list(filter(grab_symptom_keys, record))
    contained = list(map(build_conditions_list, symptom_keys))
    diagnosis = list(map(build_diagnosis_list, symptom_keys))
    encounter_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/encounter",
        value = f"{REDCAP_URL}/{PROJECT_ID}/{record['record_id']}"
    )
    encounter_class_coding = create_coding(
        system = "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code = "HH"
    )
    start_time = convert_to_iso(record['enrollment_date_time'], "%Y-%m-%d %H:%M")
    locations = list(map(build_locations_list, locations))

    encounter_resource = create_encounter_resource(
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class_coding,
        start_timestamp = start_time,
        patient_reference = patient_reference,
        location_references = locations,
        diagnosis = diagnosis,
        contained = contained
    )

    full_url = generate_full_url_uuid()
    encounter_resource_entry = create_resource_entry(
        resource = encounter_resource,
        full_url = full_url
    )
    encounter_reference = create_reference(
        reference_type = "Encounter",
        reference = full_url
    )

    return encounter_resource_entry, encounter_reference


def create_resource_condition(record: dict, symptom_name: str, patient_reference: dict) -> dict:
    """ Returns a FHIR Condition resource. """
    def symptom_duration(record: dict) -> str:
        return convert_to_iso(record['symptom_duration'], "%Y-%m-%d")

    def severity(symptom_name: Optional[str]) -> Optional[str]:
        if symptom_name:
            category = re.search('fever|cough|ache|fatigue|sorethroat', symptom_name.lower())
            if category:
                return f"{category[0]}_severity"

        return None

    mapped_symptom_name = map_symptom(symptom_name)

    # XXX TODO: Define this as a TypedDict when we upgrade from Python 3.6 to
    # 3.8.  Until then, there's no reasonable way to type this data structure
    # better than Any.
    #   -trs, 24 Oct 2019
    condition: Any = {
        "resourceType": "Condition",
        "id": mapped_symptom_name,
        "code": {
            "coding": [
                {
                    "system": f"{INTERNAL_SYSTEM}/symptom",
                    "code": mapped_symptom_name
                }
            ]
        },
        "onsetDateTime": symptom_duration(record),
        "subject": patient_reference
    }

    symptom_severity = severity(mapped_symptom_name)
    if symptom_severity:
        condition['severity'] = create_condition_severity_code(record[symptom_severity]) # TODO lowercase?

    return condition


def create_specimen(record: dict, patient_reference: dict) -> tuple:
    """ Returns a FHIR Specimen resource entry and reference """
    # TODO: turn on barcode logic in production and replace the following line:
    barcode = record['utm_tube_barcode']

    # barcode = record['utm_tube_barcode_2']
    # reentered_barcode = record['reenter_barcode']

    # if record['barcode_confirm'] == "No":
    #     barcode = record['corrected_barcode']
    # elif barcode != reentered_barcode:
    #     raise Error # TODO
    # elif barcode != record['return_utm_barcode']:
    #     raise Error # TODO

    # TODO in production, throw error if no barcode (or skip)

    specimen_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/sample",
        value = barcode
    )

    received_time = None
    collected_time = None
    # TODO I believe in production all samples should have a sample process date
    if record['samp_process_date']:
        received_time = convert_to_iso(record['samp_process_date'], "%Y-%m-%d %H:%M")

    # TODO same as above comment
    if record['collection_date']:
        collected_time = convert_to_iso(ecord['collection_date'], "%Y-%m-%d %H:%M")

    specimen_resource = create_specimen_resource(
        [specimen_identifier], patient_reference, received_time, collected_time
    )

    full_url = generate_full_url_uuid()

    specimen_entry = create_resource_entry(specimen_resource, full_url)
    specimen_reference = create_reference(
        reference = full_url,
        reference_type = "Specimen"
    )

    return specimen_entry, specimen_reference


def create_questionnaire_response(record: dict, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """ Returns a FHIR Questionnaire Response resource entry. """

    def create_custom_race_key(record: dict) -> List:
        """
        Handles the 'race' edge case by combining "select all that apply"-type
        responses into one list.
        """
        race_keys = list(filter(grab_race_keys, record))
        race_names = list(map(lambda x: record[x], race_keys))
        return race(race_names)

    def grab_race_keys(key: str) -> Optional[Match[str]]:
        if record[key] != '':
            return re.match('race___[0-9]{1,3}$', key)
        else:
            return None

    def build_questionnaire_items(question: str) -> Optional[dict]:
        return questionnaire_item(record, question, category)

    coding_questions = [
        'race',
    ]

    boolean_questions = [
        'ethnicity',
        'barcode_confirm',
        'travel_states',
        'travel_countries',
    ]

    integer_questions = [
        'age',
        'age_months',
    ]

    string_questions = [
        'education',
        'insurance',
        'doctor_3e8fae',
        'how_hear_sfs',
        'samp_process_date',
        'house_members_d5f2d9',
        'shelter_members',
        'where_sick',
        # 'antiviral_0',    TODO turn on for production
        'acute_symptom_onset',
        'doctor_1week',
        # 'antiviral_1',    TODO turn on for production
        # 'poc_behaviors',  TODO turn on for production
    ]

    question_categories = {
        'valueCoding': coding_questions,
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueString': string_questions,
    }

    record['race'] = create_custom_race_key(record)

    items: List[dict] = []
    for category in question_categories:
        category_items = list(map(build_questionnaire_items, question_categories[category]))
        for item in category_items:
            if item:
                items.append(item)

    # Vaccine is an edge case because it can have two answers of different value types
    vaccine_status = map_vaccine(record["vaccine"])
    if vaccine_status is not None:
        answers = [{ 'valueBoolean': vaccine_status }]
        date = vaccine_date(record)
        if vaccine_status and date:
            answers.append({ 'valueDate': date })
        vaccine_item = create_questionnaire_response_item('vaccine', answers)
        items.append(vaccine_item)

    if items:
        questionnaire_reseponse_resource = create_questionnaire_response_resource(
            patient_reference, encounter_reference, items
        )
        full_url = generate_full_url_uuid()
        return create_resource_entry(questionnaire_reseponse_resource, full_url)

    return None


def questionnaire_item(record: dict, question_id: str, response_type: str) -> Optional[dict]:
    """ Creates a QuestionnaireResponse internal item from a REDCap record. """
    response = record[question_id]

    def cast_to_coding(string: str):
        """ Currently the only QuestionnaireItem we code is race. """
        return create_coding(
            system = f"{INTERNAL_SYSTEM}/race",
            code = string,
        )

    def cast_to_string(string: str) -> Optional[str]:
        if string != '':
            return string
        return None

    def cast_to_integer(string: str) -> Optional[int]:
        try:
            return int(string)
        except ValueError:
            return None

    def cast_to_boolean(string: str) -> Optional[bool]:
        if string == 'Yes':
            return True
        elif re.match(r'^No(?=$|,[\w\s]*)$', string):  # Starts with "No", has optional comma and text
            return False
        return None

    def build_response_answers(response: Union[str, List]) -> List:
        answers = []
        if not isinstance(response, list):
            response = [response]

        for item in response:
            type_casted_item = casting_functions[response_type](item)

            if type_casted_item:
                answers.append({ response_type: type_casted_item })

        return answers

    casting_functions: Mapping[str, Callable[[str], Any]] = {
        'valueCoding': cast_to_coding,
        'valueInteger': cast_to_integer,
        'valueBoolean': cast_to_boolean,
        'valueString': cast_to_string,
    }

    answers = build_response_answers(response)
    if answers:
        return create_questionnaire_response_item(question_id, answers)

    return None


def vaccine_date(record: dict) -> Optional[str]:
    """ Converts a vaccination date to 'YYYY' or 'YYYY-MM' format. """
    year = record['vaccine_year_fc54b4']
    month = record['vaccine_month_dfe1c1']

    if year == '' or year == 'Do not know':
        return None

    if month == 'Do not know':
        return datetime.strptime(year, '%Y').strftime('%Y')

    return datetime.strptime(f'{month} {year}', '%B %Y').strftime('%Y-%m')


def residence_census_tract():
    # TODO
    pass
