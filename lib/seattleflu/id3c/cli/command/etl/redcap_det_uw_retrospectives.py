"""
Process REDCAP DETs that are specific to UW retrospective samples from
the Clinical Data Pulls Project.
"""
import logging
from collections import defaultdict
from uuid import uuid4
from datetime import datetime
from typing import Optional, List, Dict, Any
from cachetools import TTLCache
from id3c.db.session import DatabaseSession
from id3c.cli.command.etl import redcap_det
from id3c.cli.command.location import location_lookup
from id3c.cli.command.geocode import get_response_from_cache_or_geocoding
from seattleflu.id3c.cli.command import age_ceiling
from .fhir import *
from .redcap_map import *

LOG = logging.getLogger(__name__)

SFS = "https://seattleflu.org"
REDCAP_URL = "https://redcap.iths.org/"
PROJECT_ID = 19915

REVISION = 1

@redcap_det.command_for_project(
    "uw-retrospectives",
    redcap_url = REDCAP_URL,
    project_id = PROJECT_ID,
    revision = REVISION,
    include_incomplete = True,
    help = __doc__)

def redcap_det_uw_retrospectives(*,
                                   db: DatabaseSession,
                                   cache: TTLCache,
                                   det: dict,
                                   redcap_record: dict) -> Optional[dict]:
    patient_entry, patient_reference = create_patient(redcap_record)

    if not patient_entry:
        LOG.info("Skipping clinical data pull with insufficient information to construct patient")
        return None

    specimen_entry, specimen_reference = create_specimen(redcap_record, patient_reference)
    location_entries, location_references = create_resident_locations(db, cache, redcap_record)
    encounter_entry, encounter_reference = create_encounter(db, redcap_record, patient_reference, location_references)

    if not encounter_entry:
        LOG.info("Skipping clinical data pull with insufficient information to construct encounter")
        return None

    questionnaire_response_entry = create_questionnaire_response(redcap_record, patient_reference, encounter_reference)

    specimen_observation_entry = create_specimen_observation_entry(specimen_reference, patient_reference, encounter_reference)

    resource_entries = [
        patient_entry,
        specimen_entry,
        encounter_entry,
        questionnaire_response_entry,
        specimen_observation_entry
    ]

    if location_entries:
        resource_entries.extend(location_entries)

    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        entries = list(filter(None, resource_entries))
    )


def create_patient(record: dict) -> Optional[tuple]:
    """ Returns a FHIR Patient resource entry and reference. """
    if not record["sex"] or not record["personid"]:
        return None, None

    gender = map_sex(record["sex"])

    # This matches how clinical parse_uw creates individual identifier
    patient_id = generate_hash(record["personid"].lower())

    patient_identifier = create_identifier(f"{SFS}/individual", patient_id)
    patient_resource = create_patient_resource([patient_identifier], gender)

    return create_entry_and_reference(patient_resource, "Patient")


def create_specimen(record: dict, patient_reference: dict) -> tuple:
    """ Returns a FHIR Specimen resource entry and reference. """
    barcode = record["barcode"]
    specimen_identifier = create_identifier(f"{SFS}/sample", barcode)
    specimen_type = "NSECR" # Nasal swab.

    specimen_resource = create_specimen_resource(
        [specimen_identifier], patient_reference, specimen_type
    )

    return create_entry_and_reference(specimen_resource, "Specimen")


def create_resident_locations(db: DatabaseSession, cache: TTLCache, record: dict) -> Optional[tuple]:
    """
    Returns FHIR Location resource entry and reference for resident address
    and Location resource entry for Census tract.
    """
    if not record["address"]:
        LOG.debug("No address found in REDCap record")
        return None, None

    address = {
        "street" : record["address"],
        "secondary": None,
        "city": None,
        "state": None,
        "zipcode": None
    }

    lat, lng, canonicalized_address = get_response_from_cache_or_geocoding(address, cache)

    if not canonicalized_address:
        LOG.debug("Geocoding of address failed")
        return None, None

    location_type_system = 'http://terminology.hl7.org/CodeSystem/v3-RoleCode'
    location_type = create_codeable_concept(location_type_system, 'PTRES')
    location_entries: List[dict] = []
    location_references: List[dict] = []
    address_partOf: Dict = None

    tract = location_lookup(db, (lat,lng), 'tract')

    if tract and tract.identifier:
        tract_identifier = create_identifier(f"{SFS}/location/tract", tract.identifier)
        tract_location = create_location_resource([location_type], [tract_identifier])
        tract_entry, tract_reference = create_entry_and_reference(tract_location, "Location")
        # tract_reference is not used outside of address_partOf so does not
        # not need to be appended to the list of location_references.
        address_partOf = tract_reference
        location_entries.append(tract_entry)

    address_hash = generate_hash(canonicalized_address)
    address_identifier = create_identifier(f"{SFS}/location/address", address_hash)
    addres_location = create_location_resource([location_type], [address_identifier], address_partOf)
    address_entry, address_reference = create_entry_and_reference(addres_location, "Location")

    location_entries.append(address_entry)
    location_references.append(address_reference)

    return location_entries, location_references


def create_encounter(db: DatabaseSession,
                     record: dict,
                     patient_reference: dict,
                     location_references: list) -> Optional[tuple]:
    """ Returns a FHIR Encounter resource entry and reference """
    encounter_location_references = create_encounter_location_references(db, record, location_references)

    if not encounter_location_references:
        return None, None

    encounter_date = record["collection_date"]
    # This matches how our clinical parse_uw generates encounter id
    encounter_id = generate_hash(f"{record['mrn']}{record['accession_no']}{encounter_date}".lower())
    encounter_identifier = create_identifier(f"{SFS}/encounter", encounter_id)
    encounter_class = create_coding(
        system = "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code = "AMB"
    )

    encounter_resource = create_encounter_resource(
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class,
        encounter_date = encounter_date,
        patient_reference = patient_reference,
        location_references = encounter_location_references
    )

    return create_entry_and_reference(encounter_resource, "Encounter")


def create_encounter_location_references(db: DatabaseSession, record: dict, resident_locations: list = None) -> Optional[list]:
    """ Returns FHIR Encounter location references """
    sample_origin = find_sample_origin_by_barcode(db, record["barcode"])

    if not sample_origin:
        return None

    origin_site_map = {
        "hmc_retro": "RetrospectiveHarborview",
        "uwmc_retro": "RetrospectiveUWMedicalCenter",
        "nwh_retro": "RetrospectiveNorthwest"
    }

    if sample_origin not in origin_site_map:
        raise UnknownSampleOrigin(f"Unknown sample_origin «{sample_origin}»")

    encounter_site = origin_site_map[sample_origin]
    site_identifier = create_identifier(f"{SFS}/site", encounter_site)
    site_reference = create_reference(
        reference_type = "Location",
        identifier = site_identifier
    )

    location_references = resident_locations or []
    location_references.append(site_reference)

    return list(map(lambda ref: {"location": ref}, location_references))


def find_sample_origin_by_barcode(db: DatabaseSession, barcode: str) -> Optional[str]:
    """
    Given an SFS *barcode* return the `sample_origin` found in sample.details
    """
    like_barcode = f"%{barcode}"

    sample = db.fetch_row("""
        select details ->> 'sample_origin' as sample_origin
        from warehouse.sample
        where identifier like %s
    """, (like_barcode,))

    if not sample:
        LOG.error(f"No sample with barcode «{barcode}» found.")
        return None

    if not sample.sample_origin:
        LOG.warning(f"Sample with barcode «{barcode}» did not have sample_origin in details")
        return None

    return sample.sample_origin


def create_questionnaire_response(record: dict, patient_reference: dict, encounter_reference: dict) -> Optional[dict]:
    """ Returns a FHIR Questionnaire Response resource entry """
    response_items = determine_questionnaire_items(record)

    if not response_items:
        return None

    questionnaire_response_resource = create_questionnaire_response_resource(
        patient_reference   = patient_reference,
        encounter_reference = encounter_reference,
        items               = response_items
    )

    return create_resource_entry(
        resource = questionnaire_response_resource,
        full_url = generate_full_url_uuid()
    )


def determine_questionnaire_items(record: dict) -> List[dict]:
    """ Returns a list of FHIR Questionnaire Response answer items """
    items: Dict[str, Any] = {}

    if record["age"]:
        items["age"] = [{ 'valueInteger': age_ceiling(int(record["age"]))}]

    questionnaire_items: List[dict] = []
    for key,value in items.items():
        questionnaire_items.append(create_questionnaire_response_item(
            question_id = key,
            answers = value
        ))

    return questionnaire_items


class UnknownSampleOrigin(ValueError):
    """
    Raised by :function: `create_encounter_location_references` if it finds
    a sample_origin that is not among a set of expected values
    """
    pass
