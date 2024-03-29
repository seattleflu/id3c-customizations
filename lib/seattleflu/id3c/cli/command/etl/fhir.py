"""
REDCap DET ETL shared functions to create FHIR documents
"""
import logging
import regex
import json
from itertools import filterfalse
from typing import Iterable, NamedTuple, Optional, List, Callable, Union
from typing_extensions import NotRequired, TypedDict
from uuid import uuid4
from datetime import datetime
from urllib.parse import quote
from id3c.cli.redcap import Record as REDCapRecord
from id3c.cli.command.de_identify import generate_hash


LOG = logging.getLogger(__name__)

SFS = "https://seattleflu.org"

class Resource(TypedDict):
    resourceType: str
    id: str
    code: dict

class Condition(Resource):
    subject: dict
    onsetDateTime: NotRequired[str]
    severity: NotRequired[dict]
    encounter: NotRequired[dict]

class Observation(Resource):
    status: str
    valueBoolean: Optional[bool]
    device: dict

# CREATE FHIR RESOURCES
def create_reference(reference_type: str = None,
                     reference: str = None,
                     identifier: dict = None) -> dict:
    """
    Create a reference resource following the FHIR format
    (https://www.hl7.org/fhir/references.html)
    """
    assert reference or identifier, \
        "Provide at least one of reference or identifier to create reference resource!"

    reference_resource = {
        "type": reference_type,
        "reference": reference,
        "identifier": identifier
    }

    return {k:v for k,v in reference_resource.items() if v is not None}


def create_patient_resource(patient_identifier: List[dict],
                            gender: str,
                            communication: Optional[List[dict]] = None) -> dict:
    """
    Create patient resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/patient.html)
    """
    gender_codes = {"male", "female", "other", "unknown"}

    assert gender in gender_codes, \
        f"Gender must be one of these gender codes: {gender_codes}"

    patient_resource = {
        "resourceType": "Patient",
        "identifier": patient_identifier,
        "gender": gender
    }

    if communication:
        patient_resource["communication"] = communication

    return patient_resource


def create_diagnostic_report(record:dict,
                             patient_reference: dict,
                             specimen_reference: dict,
                             diagnostic_code: dict,
                             create_device_result_observation_resources: Callable) -> Optional[dict]:
    """
    Create FHIR diagnostic report from given *record*.

    Links the generated diagnostic report to a specific *patient_reference* and
    *specimen_reference*.

    Device-specific modifications are made with the given *diagnostic_code*
    codeable concept for the diagnostic report and the
    *create_device_result_observation_resource* function which attaches
    observation resources to the diagnostic report.
    """
    clinical_results = create_device_result_observation_resources(record)
    if not clinical_results:
        return None

    diagnostic_result_references = []
    for result in clinical_results:
        reference = create_reference(
            reference_type = 'Observation',
            reference = '#' + result['id']
        )
        diagnostic_result_references.append(reference)

    collection_datetime = record['collection_date']

    diagnostic_report_resource = create_diagnostic_report_resource(
        datetime = collection_datetime,
        diagnostic_code = diagnostic_code,
        patient_reference  = patient_reference,
        specimen_reference = specimen_reference,
        result = diagnostic_result_references,
        contained = clinical_results
    )

    return (create_resource_entry(
        resource = diagnostic_report_resource,
        full_url = generate_full_url_uuid()
    ))


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
        "resourceType": "DiagnosticReport",
        "status": "final",
        "effectiveDateTime": datetime,
        "specimen": [ specimen_reference ],
        "code": diagnostic_code,
        "subject": patient_reference,
        "result": result,
    }

    if contained:
        diagnostic_report["contained"] = contained

    return diagnostic_report


def create_condition_resource(condition_id: str,
                              patient_reference: dict,
                              onset_datetime: str,
                              condition_code: dict,
                              encounter_reference = None,
                              severity = None) -> Condition:
    """
    Create condition resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/condition.html)
    """
    condition_resource: Condition = {
        "resourceType": "Condition",
        "id": condition_id,
        "subject": patient_reference,
        "code": condition_code,
    }

    if encounter_reference:
        condition_resource["encounter"] = encounter_reference

    if severity:
        condition_resource["severity"] = severity

    if onset_datetime:
        condition_resource["onsetDateTime"] = onset_datetime

    return condition_resource


def create_condition_severity_code(condition_severity: str) -> dict:
    """
    Create a condition severity codeable concept following the FHIR format
    and FHIR value set
    (http://www.hl7.org/implement/standards/fhir/valueset-condition-severity.html)
    """
    severity_code_system = "http://snomed.info/sct"
    severity = {
        "Mild": "255604002",
        "Moderate": "6736007",
        "Severe": "24484000"
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
                             location_partOf: dict = None) -> dict:
    """
    Create location resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/location.html)
    """
    location_resource = {
        "resourceType": "Location",
        "mode": "instance",
        "type": location_type,
        "identifier": location_identifier,
        "partOf": {},
    }

    if location_partOf:
        location_resource["partOf"] = location_partOf
    else:
        del location_resource["partOf"]

    return location_resource


def create_encounter_resource(encounter_source: str,
                              encounter_identifier: List[dict],
                              encounter_class: dict,
                              encounter_date: str,
                              patient_reference: dict,
                              location_references: List[dict],
                              diagnosis: List[dict] = None,
                              contained: List[Condition] = None,
                              encounter_status = 'finished',
                              hospitalization: dict = None,
                              reason_code: List[dict] = None,
                              part_of: dict = None) -> dict:
    """
    Create encounter resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/encounter.html)
    """
    # XXX FIXME: We are currently using Encounter.period incorrectly, which
    # became apparently only after we started using hospitalization data (from
    # applicable retrospective data). An Encounter.period's start and end dates
    # should reflect the time period covering the hospital admission and
    # discharge.
    #
    # Currently, the FHIR ETL upserts an ID3C encounter (e.g. time of sample
    # collectoin date) using the Encounter.period.start date. Ideally, the FHIR
    # ETL would pull the encounter date (i.e. sample collection date) from
    # Specimen.collection, but there are some organizational hurdles in the
    # current code we'd have to address before being able to do so. For example,
    # Encounters are processed before Specimens, and the two are only linked by
    # Observations.
    #
    # kfay, 20 March 2020
    period =  {
        "start": encounter_date
    }

    if encounter_status == 'finished' or encounter_status == 'cancelled':
        period['end'] = encounter_date

    encounter_resource = {
        "resourceType": "Encounter",
        "meta": { "source": encounter_source },
        "class": encounter_class,
        "identifier": encounter_identifier,
        "status": encounter_status,
        "period": period,
        "subject": patient_reference,
    }

    if location_references:
        encounter_resource["location"] = location_references
    if diagnosis:
        encounter_resource["diagnosis"] = diagnosis
    if contained:
        encounter_resource["contained"] = contained
    if hospitalization:
        encounter_resource["hospitalization"] = hospitalization
    if reason_code:
        encounter_resource['reasonCode'] = reason_code
    if part_of:
        encounter_resource['partOf'] = part_of

    return encounter_resource


def create_specimen_resource(specimen_identifier: List[dict],
                             patient_reference: dict,
                             specimen_type: str,
                             received_datetime: str = None,
                             collection_datetime: str = None,
                             note: str = None) -> dict:
    """
    Create specimen resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/specimen.html)
    """
    specimen_type_system = 'http://terminology.hl7.org/CodeSystem/v2-0487'
    specimen_resource = {
        "resourceType": "Specimen",
        "identifier": specimen_identifier,
        "subject": patient_reference,
        "type": create_codeable_concept(specimen_type_system, specimen_type)
    }
    if received_datetime:
        specimen_resource["receivedTime"] = received_datetime

    if collection_datetime:
        specimen_resource["collection"] = {
            "collectedDateTime": collection_datetime
        }

    if note:
        specimen_resource["note"] = [{"text": note}]

    return specimen_resource


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


def create_specimen_observation(specimen_reference: dict,
                                patient_reference: dict,
                                encounter_reference: dict) -> Optional[dict]:
    """
    Create an observation resource that is a links a specimen, a patient, and
    an encounter. Follows the FHIR format
    (http://www.hl7.org/implement/standards/fhir/observation.html)
    """
    if not specimen_reference:
        return None

    return {
        "resourceType": "Observation",
        "status": "final",
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "89873-4",
                    "display": "Unique ID Initial sample"
                }
            ]
        },
        "encounter": encounter_reference,
        "subject": patient_reference,
        "specimen": specimen_reference
    }


def create_immunization_resource(patient_reference: dict,
                                 immunization_identifier: List[dict],
                                 immunization_date: str,
                                 immunization_status: str,
                                 vaccine_code: dict) -> dict:
    """
    Create an immunization resource following the FHIR format
    (https://www.hl7.org/fhir/immunization.html)
    """

    return({
        "resourceType": "Immunization",
        "identifier": immunization_identifier,
        "status": immunization_status,
        "patient": patient_reference,
        "occurrenceDateTime": immunization_date,
        "vaccineCode": {
            "coding": [vaccine_code]
        }
    })


def create_questionnaire_response_resource(patient_reference: dict,
                                           encounter_reference: dict,
                                           items: List[dict]) -> dict:
    """
    Create a questionnaire response resource following the FHIR format
    (https://www.hl7.org/fhir/questionnaireresponse.html)
    """
    return ({
        "resourceType": "QuestionnaireResponse",
        "status": "completed",
        "subject": patient_reference,
        "encounter": encounter_reference,
        "item": items
    })


def create_questionnaire_response_item(question_id: str,
                                       answers: List[dict]) -> dict:
    """
    Create a questionnaire response answer item following the FHIR format
    (https://www.hl7.org/fhir/questionnaireresponse-definitions.html#QuestionnaireResponse.item)
    """
    return ({
        "linkId": question_id,
        "answer": answers
    })


def create_bundle_resource(bundle_id: str,
                           timestamp: str,
                           source: str,
                           entries: List[dict]) -> dict:
    """
    Create bundle resource containing other resources following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/bundle.html)
    """
    return ({
        "resourceType": "Bundle",
        "type": "collection",
        "id": bundle_id,
        "meta": { "source": source },
        "timestamp": timestamp,
        "entry": entries
    })


def create_resource_entry(resource: Union[dict, Condition, Observation], full_url: str) -> Optional[dict]:
    """
    Create bundle entry that contains a *resource* and a *full_url*.
    """
    if not resource:
        return None

    return ({
        "resource": resource,
        "fullUrl": full_url
    })


def create_entry_and_reference(resource: dict,
                               reference_type: str = None) -> tuple:
    """
    Create a bundle entry and a reference that refers to the fullUrl of the
    bundle entry.
    """
    full_url = generate_full_url_uuid()
    entry = create_resource_entry(resource, full_url)
    reference = create_reference(
        reference_type = reference_type,
        reference      = full_url
    )

    return entry, reference


# CREATE FHIR DATA TYPES
def create_coding(system: str, code: str, display: str = None) -> dict:
    """
    Create coding concept following the FHIR format
    (https://www.hl7.org/fhir/datatypes.html#codesystem)
    """
    coding = {
        "system": system,
        "code": code
    }

    if display:
        coding["display"] = display

    return coding


def create_codeable_concept(system: str, code: str, display = None) -> dict:
    """
    Create codeable concept following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/datatypes.html#CodeableConcept)
    """
    return ({
        "coding": [create_coding(system, code, display)]
    })


def create_identifier(system: str, value: str) -> dict:
    """
    Create an identifier data type following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/datatypes.html#Identifier)
    """
    return ({
        "system": system,
        "value": value
    })


def generate_full_url_uuid() -> str:
    """
    Create a fullUrl following FHIR format that represents a UUID.
    (http://www.hl7.org/implement/standards/fhir/bundle-definitions.html#Bundle.entry.fullUrl)
    """
    return f"urn:uuid:{uuid4()}"


def generate_patient_hash(names: Iterable[str], gender: str, birth_date: str, postal_code: str) -> str:
    """
    Creates a likely-to-be unique, unreversible hash from the *names*,
    *gender*, *birth_date*, and *postal_code* for an individual.

    Used in FHIR Patient resources as an identifier, which ultimately winds up
    in ID3C's ``warehouse.individual.identifier`` column.
    """
    class PersonalInformation(NamedTuple):
        name: str
        gender: str
        birth_date: str
        postal_code: str

    personal_information = PersonalInformation(
        canonicalize_name(*names),
        gender,
        birth_date,
        postal_code,
    )

    def missing(info):
        return [
            field
            for field, value
            in zip(info._fields, info)
            if not value
        ]

    if missing(personal_information):
        LOG.debug(f"All personal information is required to generate a robust patient hash; missing {missing(personal_information)}")
        return None

    return generate_hash("\N{UNIT SEPARATOR}".join(personal_information))


def canonicalize_name(*parts: Iterable[str]) -> str:
    """
    Takes a list of name *parts* and returns a single, canonicalized string.

    >>> canonicalize_name("`1234567890-=~!@#$%^&*()_+")
    '1234567890'
    >>> canonicalize_name("qwertyuiop[]\\\\QWERTYUIOP{}|")
    'QWERTYUIOPQWERTYUIOP'
    >>> canonicalize_name("asdfghjkl;'ASDFGHJKL:\\"")
    'ASDFGHJKLASDFGHJKL'
    >>> canonicalize_name("zxcvbnm,./ZXCVBNM<>?")
    'ZXCVBNMZXCVBNM'
    >>> canonicalize_name("¿¡Y", "tú", "quién", "te crees!?")
    'Y TÚ QUIÉN TE CREES'
    >>> canonicalize_name("The \\t\\n, quick   brown fox")
    'THE QUICK BROWN FOX'
    >>> canonicalize_name("  jumps\\t\\tover\\n\\n\\nthe   .  ")
    'JUMPS OVER THE'
    >>> canonicalize_name("lazydog")
    'LAZYDOG'
    """
    def remove_non_word_chars(part):
        # Python's core "re" module doesn't support Unicode property classes
        return regex.sub(r'[^\s\p{Alphabetic}\p{Mark}\p{Decimal_Number}\p{Join_Control}]', "", part)

    def collapse_whitespace(part):
        return regex.sub(r'\s+', " ", part)

    def canonicalize(part):
        return collapse_whitespace(remove_non_word_chars(part)).strip().upper()

    return " ".join(map(canonicalize, parts))


def observation_resource(device: str) -> Observation:
    """
    Returns a minimally-filled FHIR Observation Resource with a given
    *device* value.
    """
    return {
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
                value = device
            )
        )
    }


def create_redcap_uri(record: REDCapRecord) -> str:
    """
    Create a JSON data URI representing the source of a given REDCap *record*.

    Used in FHIR Encounter resources as the meta.source, which ultimately winds
    up in ID3C's ``warehouse.encounter.details`` column.
    """
    data_scheme = 'data:application/json'

    redcap_data = {
        'url': record.project.base_url,
        'project_id': record.project.id,
        'record_id': record.id
    }

    if record.get('redcap_event_name'):
        redcap_data['event_name'] = record.get('redcap_event_name')

    if record.get('redcap_repeat_instance'):
        redcap_data['repeat_instance'] = record.get('redcap_repeat_instance')

    data = {'redcap': redcap_data}

    return data_scheme + ',' + quote(json.dumps(data))
