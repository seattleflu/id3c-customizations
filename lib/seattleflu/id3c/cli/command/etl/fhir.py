"""
REDCap DET ETL shared functions to create FHIR documents
"""
import re
from typing import Optional, List
from uuid import uuid4
from datetime import datetime

# CREATE FHIR RESOURCES
def create_reference(reference_type: Optional[str] = None,
                     reference: Optional[str] = None,
                     identifier: Optional[dict] = None) -> dict:
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


def create_patient_resource(patient_identifier: List[dict], gender: str) -> dict:
    """
    Create patient resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/patient.html)
    """
    gender_codes = ("male", "female", "other", "unknown")

    assert gender in gender_codes, \
        f"Gender must be one of these gender codes: {gender_codes}"

    return ({
        "resourceType": "Patient",
        "identifier": patient_identifier,
        "gender": gender
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
        "resourceType": "Immunization",
        "vaccineCode": vaccine_code,
        "patient": patient_reference,
        "status": status,
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
                              severity = None) -> dict:
    """
    Create condition resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/condition.html)
    """
    condition_resource = {
        "resourceType": "Condition",
        "id": condition_id,
        "subject": patient_reference,
        "onsetDateTime": onset_datetime,
        "code": condition_code,
    }

    if severity:
        condition_resource["severity"] = severity

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
                             location_partOf: Optional[dict] = None) -> dict:
    """
    Create location resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/location.html)
    """
    location_resource = {
        "resourceType": "Location",
        "mode": "instance",
        "type": location_type,
        "identifier": location_identifier,
    }

    if location_partOf:
        location_resource["partOf"] = location_partOf

    return location_resource


def create_encounter_resource(encounter_identifier: List[dict],
                              encounter_class: dict,
                              start_timestamp: str,
                              patient_reference: dict,
                              location_references: List[dict],
                              diagnosis: Optional[List[dict]] = None,
                              contained: Optional[List[dict]] = None) -> dict:
    """
    Create encounter resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/encounter.html)
    """
    encounter_resource = {
        "resourceType": "Encounter",
        "class": encounter_class,
        "identifier": encounter_identifier,
        "status": "finished",
        "period": {
            "start": start_timestamp
        },
        "subject": patient_reference,
        "location": location_references
    }

    if diagnosis:
        encounter_resource["diagnosis"] = diagnosis
    if contained:
        encounter_resource["contained"] = contained

    return encounter_resource


def create_specimen_resource(specimen_identifier: List[dict],
                             patient_reference: dict,
                             received_datetime: Optional[str] = None,
                             collection_datetime: Optional[str] = None) -> dict:
    """
    Create specimen resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/specimen.html)
    """
    specimen_resource = {
        "resourceType": "Specimen",
        "identifier": specimen_identifier,
        "subject": patient_reference
    }
    if received_datetime:
        specimen_resource["receivedTime"] = received_datetime

    if collection_datetime:
        specimen_resource["collection"] = {
            "collectedDateTime": collection_datetime
        }

    return specimen_resource


def create_specimen_observation(specimen_reference: dict,
                                patient_reference: dict,
                                encounter_reference: dict) -> dict:
    """
    Create an observation resource that is a links a specimen, a patient, and
    an encounter. Follows the FHIR format
    (http://www.hl7.org/implement/standards/fhir/observation.html)
    """
    return ({
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
                           entries: List[dict]) -> dict:
    """
    Create bundle resource containing other resources following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/bundle.html)
    """
    return ({
        "resourceType": "Bundle",
        "type": "collection",
        "id": bundle_id,
        "timestamp": timestamp,
        "entry": entries
    })


def create_resource_entry(resource: dict, full_url: str) -> dict:
    """
    Create bundle entry that contains a *resource* and a *full_url*.
    """
    return ({
        "resource": resource,
        "fullUrl": full_url
    })


# CREATE FHIR DATA TYPES
def create_coding(system: str, code: str, display: Optional[str] = None) -> dict:
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


def convert_to_iso(time: str, current_format: str) -> str:
    """
    Converts a *time* to ISO format from the *current_format* specified in
    C-standard format codes.
    """
    # TODO uses locale time zone
    return datetime.strptime(time, current_format).astimezone().isoformat()


def canonicalize_name(full_name: str) -> str:
    """ """
    return re.sub(r'\s*[\d\W]+\s*', ' ', full_name).upper()
