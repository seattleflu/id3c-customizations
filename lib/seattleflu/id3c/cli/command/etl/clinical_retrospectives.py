"""
Clinical -> FHIR ETL shared functions to process retrospective data
into FHIR bundles
"""
import logging
import re
from typing import Optional, Dict, Callable, Any
from cachetools import TTLCache
from id3c.db.session import DatabaseSession
from id3c.cli.command.location import location_lookup
from id3c.cli.command.geocode import get_geocoded_address
from . import standardize_whitespace
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
        "freestanding": "IMP",
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
    address_partOf: dict = None

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


def create_clinical_result_observation_resource(record: dict) -> Optional[List[Observation]]:
    """
    Determine the clinical results based on responses in *record* and
    create observation resources for each result following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/observation.html)
    """
    code_map = {
        '871562009': {
            'system': 'http://snomed.info/sct',
            'code': '871562009',
            'display': 'Detection of SARS-CoV-2',
        },
        '181000124108': {
            'system': 'http://snomed.info/sct',
            'code': '181000124108',
            'display': 'Influenza A virus present',
        },
        '441345003': {
            'system': 'http://snomed.info/sct',
            'code': '441345003',
            'display': 'Influenza B virus present',
        },
        '441278007': {
            'system': 'http://snomed.info/sct',
            'code': '441278007',
            'display': 'Respiratory syncytial virus untyped strain present',
        },
        '440925005': {
            'system': 'http://snomed.info/sct',
            'code': '440925005',
            'display': 'Human rhinovirus present',
        },
        '440930009': {
            'system': 'http://snomed.info/sct',
            'code': '440930009',
            'display': 'Human adenovirus present',
        },
    }

    # While we ingest inconclusive results for PHSKC samples as null, we do not ingest
    # inconclusive results for UW Retrospective samples. If other samples need their
    # inconclusive samples ingested, they should be added to this list.
    #
    # TODO: Don't really love this method of deciding inconclusive ingestion. Difficult b/c decision
    #       should be made in function given how it is passed as an argument to a future diagnostic
    #       result builder function.
    sites_ingesting_inconclusives = ['phskc']
    ingest_inconclusives = True if 'site' in record and record['site'].lower() in sites_ingesting_inconclusives else False

    # Create intermediary, mapped clinical results using SNOMED codes.
    # This is useful in removing duplicate tests (e.g. multiple tests run for
    # Influenza A)
    results = mapped_snomed_test_results(record, ingest_inconclusives)

    # Create observation resources for all results in clinical tests
    diagnostic_results: Any = {}

    for index, finding in enumerate(results):
        new_observation = observation_resource('clinical')
        new_observation['id'] = 'result-' + str(index + 1)
        new_observation['code']['coding'] = [code_map[finding]]
        new_observation['valueBoolean'] = results[finding]
        diagnostic_results[finding] = (new_observation)

    return list(diagnostic_results.values()) or None


def mapped_snomed_test_results(record: dict, ingesting_inconclusives: bool) -> Dict[str, bool]:
    """
    Given a *record*, returns a dict of the mapped SNOMED clinical
    finding code and the test result.
    """
    redcap_to_snomed_map = {
        'ncvrt':         '871562009',
        'result_value' : '871562009', # PHSKC UW Virology Test
        'revfla':        '181000124108',
        'fluapr':        '181000124108',
        'revflb':        '441345003',
        'flubpr':        '441345003',
        'revrsv':        '441278007',
        'revrhn':        '440925005',
        'revadv':        '440930009',
    }

    results: Dict[str, bool] = {}

    # Populate dict of tests administered during encounter by filtering out
    # null results. Map the REDCap test variable to the snomed code. In the
    # event of duplicate clinical findings, prioritize keeping positive results.
    for test in redcap_to_snomed_map:

        # Only try to get results for tests in the record
        if test not in record:
            continue

        code = redcap_to_snomed_map[test]

        # Skip updating results for tests already marked as positive
        if results.get(code):
            continue

        try:
            result = present(record, test)
        except UnknownTestResult as e:
            LOG.warning(e)
            continue

        # Only add inconclusive results when desired, otherwise skip
        # them. Always skip empty tests.
        if result == 'positive':
            result_val = True
        elif result == 'negative':
            result_val = False
        elif result =='inconclusive' and ingesting_inconclusives:
            result_val = None
        else:
            continue

        results[code] = result_val

    return results


def present(record: dict, test: str) -> Optional[str]:
    """
    Returns a test result presence absence string for a given *test* result from a
    given *record*. Empty or invalid tests are returned as None.
    """
    result = record[test]

    # Lowercase, remove non-alpahnumeric characters(except spaces), then standardize whitespace
    # Removal of non-alphanumeric characters is to account for inconsistencies in the data received
    standardized_result = standardize_whitespace(re.sub(r'[^a-z0-9 ]+','',result.lower())) if result else None

    if not standardized_result:
        return None

    # Only positive or negative results should be given a boolean value in the first position of the tuple.
    # The second position represents whether a test resulted or not, so should be true for positive, negative,
    # and inconclusive tests but false for everything else.
    test_result_prefix_map = {
        'negative'                              : 'negative',
        'none detected'                         : 'negative',
        'not detected'                          : 'negative',
        'ndet'                                  : 'negative',
        'det'                                   : 'positive',
        'positive'                              : 'positive',
        'cancel'                                : None,
        'disregard'                             : None,
        'duplicate request'                     : None,
        'incon'                                 : 'inconclusive',
        'indeterminate'                         : 'inconclusive',
        'pending'                               : None,
        'test not applicable'                   : None,
        'wrong test'                            : None,
        'followup testing required'             : None,
        'data entry correction'                 : None,
        'reorder requested'                     : None,
        'invalid'                               : None,
    }

    for prefix in test_result_prefix_map:
        if standardized_result.startswith(prefix):
            return test_result_prefix_map[prefix]

    raise UnknownTestResult(f"Unknown test result value «{standardized_result}» for «{record['barcode']}».")


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
