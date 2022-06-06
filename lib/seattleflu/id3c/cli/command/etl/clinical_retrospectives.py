"""
Clinical -> FHIR ETL shared functions to process retrospective data
into FHIR bundles
"""
import logging
from typing import Optional, Dict, Callable
from cachetools import TTLCache
from id3c.db.session import DatabaseSession
from id3c.cli.command.location import location_lookup
from id3c.cli.command.geocode import get_geocoded_address
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


def create_encounter_location_references(db: DatabaseSession, record: dict, resident_locations: list = None) -> Optional[list]:
    """ Returns FHIR Encounter location references """
    sample_origin = find_sample_origin_by_barcode(db, record["barcode"])

    if not sample_origin:
        return None

    origin_site_map = {
        "hmc_retro": "RetrospectiveHarborview",
        "uwmc_retro": "RetrospectiveUWMedicalCenter",
        "nwh_retro": "RetrospectiveNorthwest",
        "phskc_retro":  "RetrospectivePHSKC",

        # for future use
        "sch_retro":    "RetrospectiveSCH",
        "kp":           "KaiserPermanente",
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


def create_resident_locations(record: dict, db: DatabaseSession = None, cache: TTLCache = None) -> Optional[tuple]:
    """
    Returns FHIR Location resource entry and reference for resident address
    and Location resource entry for Census tract. Geocodes the address if
    necessary.
    """
    # default to a hashed address but fall back on a non-hashed address as long
    # as we have the ability to geocode it.
    if 'address_hash' in record:
        geocoding = False
        address = record["address_hash"]
    elif db and cache and 'address' in record:
        geocoding = True
        address = record['address']
    else:
        address = None

    if not address:
        LOG.debug("No address found in REDCap record")
        return None, None

    if geocoding:
        address_record = {
                "street" : address,
                "secondary": None,
                "city": None,
                "state": None,
                "zipcode": None
        }

        lat, lng, canonicalized_address = get_geocoded_address(address_record, cache)

        if not canonicalized_address:
            LOG.debug("Geocoding of address failed")
            return None, None

    location_type_system = 'http://terminology.hl7.org/CodeSystem/v3-RoleCode'
    location_type = create_codeable_concept(location_type_system, 'PTRES')
    location_entries: List[dict] = []
    location_references: List[dict] = []
    address_partOf: Dict = None

    # we can assume we have the census tract in the record if we are not geocoding,
    # otherwise we can look it up on the fly
    if geocoding:
        tract = location_lookup(db, (lat,lng), 'tract')
        tract_identifier = tract.identifier if tract and tract.identifier else None
    else:
        tract_identifier = record["census_tract"]

    if tract_identifier:
        tract_id = create_identifier(f"{SFS}/location/tract", tract_identifier)
        tract_location = create_location_resource([location_type], [tract_id])
        tract_entry, tract_reference = create_entry_and_reference(tract_location, "Location")

        # tract_reference is not used outside of address_partOf so does not
        # not need to be appended to the list of location_references.
        address_partOf = tract_reference
        location_entries.append(tract_entry)

    address_hash = generate_hash(canonicalized_address) if geocoding else record["address_hash"]
    address_identifier = create_identifier(f"{SFS}/location/address", address_hash)
    addres_location = create_location_resource([location_type], [address_identifier], address_partOf)
    address_entry, address_reference = create_entry_and_reference(addres_location, "Location")

    location_entries.append(address_entry)
    location_references.append(address_reference)

    return location_entries, location_references


def create_questionnaire_response(record: dict,
                                  patient_reference: dict,
                                  encounter_reference: dict,
                                  determine_questionnaire_items: Callable[[dict], List[dict]]) -> Optional[dict]:
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
