"""
Clinical -> FHIR ETL shared functions to process retrospective data
into FHIR bundles
"""
import logging
from typing import Optional
from id3c.db.session import DatabaseSession
from .clinical import standardize_whitespace
from .fhir import *
from .redcap_map import map_sex

LOG = logging.getLogger(__name__)

SFS = "https://seattleflu.org"


def create_specimen(record: dict, patient_reference: dict) -> tuple:
    """ Returns a FHIR Specimen resource entry and reference. """
    barcode = record["barcode"]
    specimen_identifier = create_identifier(f"{SFS}/sample", barcode)
    specimen_type = "NSECR" # Nasal swab.

    specimen_resource = create_specimen_resource(
        [specimen_identifier], patient_reference, specimen_type
    )

    return create_entry_and_reference(specimen_resource, "Specimen")


def find_sample_origin_by_barcode(db: DatabaseSession, barcode: str) -> Optional[str]:
    """
    Given an SFS *barcode* return the `sample_origin` found in sample.details
    """
    sample = db.fetch_row("""
        select details ->> 'sample_origin' as sample_origin
        from warehouse.sample
        join warehouse.identifier on sample.identifier = identifier.uuid::text
        where barcode = %s
    """, (barcode,))

    if not sample:
        LOG.error(f"No sample with barcode «{barcode}» found.")
        return None

    if not sample.sample_origin:
        LOG.warning(f"Sample with barcode «{barcode}» did not have sample_origin in details")
        return None

    return sample.sample_origin


def create_encounter_class(record: dict) -> dict:
    """
    Creates an Encounter.class coding from a given *record*. If no
    encounter class is given, defaults to the coding for `AMB`.

    This attribute is required by FHIR for an Encounter resource.
    (https://www.hl7.org/fhir/encounter-definitions.html#Encounter.class)
    """
    encounter_class = record.get('patient_class', '')

    mapper = {
        "outpatient" : "AMB",
        "hospital outpatient surgery": "AMB",
        "series pt-ot-st": "AMB", # Physical-occupational-speech therapy
        "deceased - organ donor": "AMB",
        "inpatient"  : "IMP",
        "emergency"  : "EMER",
        "op"    : "AMB",
        "ed"    : "EMER",  # can also code as "AMB"
        "ip"    : "IMP",
        "lim"   : "IMP",
        "obs"   : "IMP",
        "obv"   : "IMP",
        "observation" : "IMP",
        "field" : "FLD",
        "surgery overnight stay" : "IMP",
        "surgery admit": "IMP",
    }

    standardized_encounter_class = standardize_whitespace(encounter_class.lower())

    if standardized_encounter_class and standardized_encounter_class not in mapper:
        raise Exception(f"Unknown encounter class «{encounter_class}».")

    # Default to 'AMB' if encounter_class not defined
    return create_coding(
        system = "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code = mapper.get(standardized_encounter_class, 'AMB')
    )


def create_encounter_status(record: dict) -> str:
    """
    Returns an Encounter.status code from a given *record*. Defaults to
    'finished' if no encounter status is found, because we can assume this
    UW Retrospective encounter was an outpatient encounter.

    This attribute is required by FHIR for an Encounter resource.
    (https://www.hl7.org/fhir/encounter-definitions.html#Encounter.status)
    """
    status = record['encounter_status']
    if not status:
        return 'finished'

    mapper = {
        'arrived'   : 'arrived',
        'preadmit'  : 'arrived',
        'lwbs'      : 'cancelled',  # LWBS = left without being seen.
        'canceled'  : 'cancelled',
        'no show'   : 'cancelled',
        'completed' : 'finished',
        'discharged': 'finished',
    }

    standardized_status = standardize_whitespace(status.lower())

    if standardized_status in mapper.values():
        return standardized_status
    elif standardized_status not in mapper:
        raise Exception(f"Unknown encounter status «{standardized_status}».")

    return mapper[standardized_status]


def create_patient(record: dict) -> Optional[tuple]:

    """ Returns a FHIR Patient resource entry and reference. """

    if not record["sex"]:
        return None, None

    gender = map_sex(record["sex"])

    # phskc samples
    if record.get("individual", None):
        patient_id = record["individual"]
    # uw retro samples
    elif record.get("personid", None):
        patient_id = generate_hash(record["personid"].lower())
    else:
        return None, None

    patient_identifier = create_identifier(f"{SFS}/individual", patient_id)
    patient_resource = create_patient_resource([patient_identifier], gender)

    return create_entry_and_reference(patient_resource, "Patient")


class UnknownTestResult(ValueError):
    """
    Raised by :function: `present` if it finds a test result
    that is not among a set of mapped values
    """
    pass

class UnknownSampleOrigin(ValueError):
    """
    Raised by :function: `create_encounter_location_references` if it finds
    a sample_origin that is not among a set of expected values
    """
    pass
