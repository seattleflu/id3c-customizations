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
    location_resources = locations(redcap_record)

    patient         = resource(create_resource_patient(redcap_record))
    encounter       = resource(create_resource_encounter(redcap_record, PROJECT_ID, patient, location_resources))
    questionnaire   = resource(create_resource_questionnaire_response(redcap_record, patient, encounter))
    specimen        = resource(create_resource_specimen(redcap_record, patient))
    immunization    = resource(create_resource_immunization(redcap_record, patient))

    other_resources = [
        patient,
        encounter,
        questionnaire,
        specimen,
        immunization,
    ]

    bundle = {
        "resourceType": "Bundle",
        "id": str(uuid4()),
        "type": "collection",
        "timestamp": datetime.now().astimezone().isoformat(),
        "entry": [
            *[ r for r in other_resources if r is not None ],
            *location_resources,
        ]
    }

    return bundle


def resource(resource: dict) -> Optional[dict]:
    """
    Provides a `fullUrl` to a non-empty FHIR Resource. This is made possible by
    wrapping the FHIR Resource within a `resource` key.
    """
    if resource:
        return { "resource": resource, "fullUrl": f"urn:uuid:{uuid4()}" }
    return None


def locations(record: dict) -> list:
    """ Creates a list of Location resources from a REDCap record. """
    def uw_affiliation(record: dict) -> List[Dict[Any, Any]]:
        uw_affiliation = record['uw_affiliation']

        uw_locations = []
        if uw_affiliation in ['1', '2']:
            uw_locations.append(
                create_resource_location(f"{INTERNAL_SYSTEM}/location", UW_CENSUS_TRACT, "school"))

        if uw_affiliation in ['2', '3', '4']:
            uw_locations.append(
                create_resource_location(f"{INTERNAL_SYSTEM}/location", UW_CENSUS_TRACT, "work"))

        return uw_locations

    def housing(record: dict) -> dict:
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

        return create_resource_location(
            f"{INTERNAL_SYSTEM}/location", '#TODO CENSUS TRACT', housing_type)

    locations = [
        resource(create_resource_location(f"{INTERNAL_SYSTEM}/site", 'self-test', "site"))
    ]

    housing_location = housing(record)
    if housing_location:
        locations.append(resource(housing_location))

    uw_location = uw_affiliation(record)
    for location in uw_location:
        if location:
            locations.append(resource(location))

    return locations


def create_resource_location(system: str, value: str, type: str, parent: str=None) -> dict:
    """ Returns a FHIR Location resource. """
    location_type_map = {
        "residence": "PTRES",
        "school": "SCHOOL",
        "work": "WORK",
        "site": "HUSCS",
        "lodging": "PTLDG",
    }

    location = {
        "resourceType": "Location",
        "mode": "instance",
        "identifier": [{
            "system": system,
            "value": value
        }],
        "type": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                "code": location_type_map[type]
            }]
        }]
    }

    if parent:
        location["partOf"] = { "reference": parent }  # TODO we're not really using this key right now

    return location


def create_resource_patient(record: dict) -> dict:
    """ Returns a FHIR Patient resource. """
    return {
        "resourceType": "Patient",
        "identifier": [{
            "system": f"{INTERNAL_SYSTEM}/individual",
            "value": generate_patient_hash(record),
        }],
        "gender": sex(record)
    }


def generate_patient_hash(record: dict) -> dict:
    """ Returns a hash generated from patient information. """
    personal_information = {
        "name": canonicalize_name(f"{record['first_name_1']}{record['last_name_1']}"),
        "gender": sex(record),  # TODO redundant?
        "birthday": convert_to_iso(record['birthday'], '%Y-%m-%d'),
        "zipcode": record['home_zipcode_2']  # TODO redundant?
    }

    return generate_hash(str(sorted(personal_information.items())))


def create_resource_immunization(record: dict, patient: dict) -> Optional[dict]:
    """ Returns a FHIR Immunization resource. """
    vaccine_status = vaccine(record)
    if not vaccine_status:
        return None

    immunization = {
        "resourceType": "Immunization",
        "status": vaccine_status,
        "vaccineCode": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "46233009",
                    "display": "Influenza virus vaccine"
                }
            ]
        },
        "patient": {
            "type": "Patient",
            "reference": patient['fullUrl'],
        }
    }

    date = vaccine_date(record)

    if date:
        immunization["occurrenceDateTime"] = date
    else:
        immunization["occurrenceString"] = "No Vaccine"
    # TODO it seems we have to have occurrenceDate or occurrenceString which seems weird considering
    # 'not-done' is an acceptable status

    return immunization



def create_resource_encounter(record: dict, project_id: int, patient: dict, locations: list) -> dict:
    """ Returns a FHIR Encounter resource. """

    def grab_symptom_keys(key: str) -> Optional[Match[str]]:
        if record[key] != '':
            return re.match('symptoms(_child)?___[0-9]{1,3}$', key)
        else:
            return None

    def build_conditions_list(symptom_key: str) -> dict:
        return create_resource_condition(record, record[symptom_key], patient)

    def build_diagnosis_list(symptom_key: str) -> dict:
        return { "condition": { "reference": f"#{symptom(record[symptom_key])}" } }

    def build_locations_list(location: dict) -> dict:
        return {
            "location": {
                "type": "Location",
                "reference": location['fullUrl']
            }
        }

    symptom_keys = list(filter(grab_symptom_keys, record))

    encounter = {
        "resourceType": "Encounter",
        "identifier": [{
            "system": f"{INTERNAL_SYSTEM}/encounter",
            "value": f"{REDCAP_URL}/{project_id}/{record['record_id']}",
        }],
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "HH"
        },
        "status": "finished",
        "period": {
            "start": convert_to_iso(record['enrollment_date_time'], "%Y-%m-%d %H:%M")
        },
        "subject": {
            "type": "Patient",
            "reference": patient['fullUrl'],
        },
        "location": list(map(build_locations_list, locations)),
    }

    contained = list(map(build_conditions_list, symptom_keys))
    if contained:
        encounter['contained'] = contained

    diagnosis = list(map(build_diagnosis_list, symptom_keys))
    if diagnosis:
        encounter['diagnosis'] = diagnosis

    return encounter


def convert_to_iso(time: str, current_format: str) -> str:
    """
    Converts a *time* to ISO format from the *current_format* specified in C-standard format codes.
    """
    return datetime.strptime(time, current_format).astimezone().isoformat()  # TODO uses locale time zone


def create_resource_condition(record: dict, symptom_name: str, patient: dict) -> dict:
    """ Returns a FHIR Condition resource. """
    def symptom_duration(record: dict) -> str:
        return convert_to_iso(record['symptom_duration'], "%Y-%m-%d")

    def severity(symptom_name: Optional[str]) -> Optional[str]:
        if symptom_name:
            category = re.search('fever|cough|ache|fatigue|sorethroat', symptom_name.lower())
            if category:
                return f"{category[0]}_severity"

        return None

    mapped_symptom_name = symptom(symptom_name)

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
        "subject": {
            "type": "Patient",
            "identifier": patient['resource']['identifier'][0],
        }
    }

    symptom_severity = severity(mapped_symptom_name)
    if symptom_severity:
        condition['severity'] = { "text": record[symptom_severity] }  # TODO lowercase?

    return condition


def create_resource_specimen(record: dict, patient: dict) -> dict:
    """ """
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

    specimen = {
        "resourceType": "Specimen",
        "identifier": [{
            "system": f"{INTERNAL_SYSTEM}/sample",
            "value": barcode,
        }],
        "subject": {
            "type": "Patient",
            "reference": patient['fullUrl'],
        }
    }

    # TODO I believe in production all samples should have a sample process date
    received_time = record['samp_process_date']
    if received_time:
        specimen["receivedTime"] = convert_to_iso(received_time, "%Y-%m-%d %H:%M")

    # TODO same as above comment
    collected_time = record['collection_date']
    if collected_time:
        specimen["collection"] = {
            "collectedDateTime": convert_to_iso(collected_time, "%Y-%m-%d %H:%M")
        }

    return specimen

def create_resource_questionnaire_response(record: dict, patient: dict,
    encounter: dict) -> dict:
    """ Returns a FHIR Specimen resource. """

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

    questionnaire_response = {
        "resourceType": "QuestionnaireResponse",
        "status": "completed",
        "subject": {
            "type": "Patient",
            "reference": patient['fullUrl'],
        },
        "encounter": {
            "type": "Encounter",
            "reference": encounter['fullUrl'],
        },
        "item": items,
    }

    if items:
        return questionnaire_response

    return None


def questionnaire_item(record: dict, question_id: str, response_type: str) -> Optional[dict]:
    """ Creates a QuestionnaireResponse internal item from a REDCap record. """
    response = record[question_id]

    def cast_to_coding(string: str):
        """ Currently the only QuestionnaireItem we code is race. """
        return {
            "system": f"{INTERNAL_SYSTEM}/race",
            "code": string,
        }

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
        return {
            "linkId": question_id,
            "answer": answers,
        }

    return None

def symptom(symptom_name: str) -> Optional[str]:
    """
    Returns a symptom name mapped from the REDCap data dictionary to the Audere
    (ID3C) equivalent name.
    """
    symptom_map = {
        'Feeling feverish':                     'feelingFeverish',
        'Headache':                             'headaches',
        'Cough':                                'cough',
        'Chills or shivering':                  'chillsOrShivering',
        'Sweats':                               'sweats',
        'Sore throat or itchy/scratchy throat': 'soreThroat',
        'Nausea or vomiting':                   'nauseaOrVomiting',
        'Runny or stuffy nose':                 'runnyOrStuffyNose',
        'Feeling more tired than usual':        'fatigue',
        'Muscle or body aches':                 'muscleOrBodyAches',
        'Diarrhea':                             'diarrhea',
        'Ear pain or discharge':                'earPainOrDischarge',
        'Rash':                                 'rash',
        'Increased trouble with breathing':     'increasedTroubleBreathing',
        'None of the above':                    None,
    }

    if symptom_name not in symptom_map:
        raise KeyError(f"Unknown symptom name \"{symptom_name}\"")

    return symptom_map[symptom_name]

def sex(record: dict) -> str:
    """
    Returns a *record* sex value mapped to a FHIR gender value.
    This function uses a map instead of converting to lowercase to guard against
    potential new or unexpected values for 'sex' in REDCap.
    """
    sex_map = {
        'Male': 'male',
        'Female': 'female',
        'Indeterminate/other': 'other',
        '': 'unknown'
    }

    try:
        return sex_map[record['sex']]

    except:
        raise KeyError(f"Unknown sex \"{record['sex']}\"")


def vaccine(record: dict) -> Optional[str]:
    """ Maps a vaccine response to a standardized FHIR value. """
    vaccine_map = {
        'Yes': 'completed',
        'No': 'not-done',
        'Do not know': None,
        '': None,
    }

    try:
        return vaccine_map[record['vaccine']]
    except:
        raise KeyError(f"Unknown vaccine response \"{record['vaccine']}\"")


def vaccine_date(record: dict) -> Optional[str]:
    """ Converts a vaccination date to ISO format. """
    year = record['vaccine_year_fc54b4']
    month = record['vaccine_month_dfe1c1']

    try:
        return convert_to_iso(f'{month} {year}', '%B %Y')
    except ValueError:
        return None


def residence_census_tract():
    # TODO
    pass


def canonicalize_name(name: str) -> str:  # TODO should live in shared module
  return re.sub(r'[\s\d\W]', '', name).upper()

def generate_hash(identifier: str):  # TODO import? currently lives in `clinical`
    """
    Generate hash for *identifier* that is linked to identifiable records.
    Must provide a "PARTICIPANT_DEIDENTIFIER_SECRET" as an OS environment
    variable.
    """
    secret = os.environ["PARTICIPANT_DEIDENTIFIER_SECRET"]
    new_hash = hashlib.sha256()
    new_hash.update(identifier.encode("utf-8"))
    new_hash.update(secret.encode("utf-8"))
    return new_hash.hexdigest()
