"""
Process REDCAP DETs that are specific to UW retrospective samples from
the Clinical Data Pulls Project.
"""
import logging
import re
from collections import defaultdict
from uuid import uuid4
from datetime import datetime
from typing import Optional, List, Dict, Any
from cachetools import TTLCache
from id3c.db.session import DatabaseSession
from id3c.cli.redcap import Record as REDCapRecord
from id3c.cli.command.etl import redcap_det
from id3c.cli.command.location import location_lookup
from id3c.cli.command.geocode import get_geocoded_address
from seattleflu.id3c.cli.command import age_ceiling
from . import standardize_whitespace, first_record_instance, race, ethnicity
from .fhir import *
from .redcap_map import *

LOG = logging.getLogger(__name__)

SFS = "https://seattleflu.org"
REDCAP_URL = "https://redcap.iths.org/"
PROJECT_ID = 19915

REVISION = 4

@redcap_det.command_for_project(
    "uw-retrospectives",
    redcap_url = REDCAP_URL,
    project_id = PROJECT_ID,
    revision = REVISION,
    include_incomplete = True,
    help = __doc__)

@first_record_instance
def redcap_det_uw_retrospectives(*,
                                   db: DatabaseSession,
                                   cache: TTLCache,
                                   det: dict,
                                   redcap_record: REDCapRecord) -> Optional[dict]:

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

    diagnostic_code = create_codeable_concept(
        system = f'{SFS}/presence-absence-panel',
        code = 'uw-retrospective'
    )

    diagnostic_report_resource_entry = create_diagnostic_report(
        redcap_record,
        patient_reference,
        specimen_reference,
        diagnostic_code,
        create_clinical_result_observation_resource
    )

    immunization_entries = create_immunization(redcap_record, patient_reference)
    condition_entries = create_conditions(redcap_record, patient_reference, encounter_reference)

    resource_entries = [
        patient_entry,
        specimen_entry,
        encounter_entry,
        questionnaire_response_entry,
        specimen_observation_entry
    ]

    for entries in [location_entries, immunization_entries, condition_entries]:
        if entries:
            resource_entries.extend(entries)

    if diagnostic_report_resource_entry:
        resource_entries.append(diagnostic_report_resource_entry)


    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{REDCAP_URL}{PROJECT_ID}/{redcap_record['barcode']}",
        entries = list(filter(None, resource_entries))
    )


def create_conditions(record:dict, patient_reference: dict, encounter_reference: dict) -> list:
    """
    Create condition resource following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/condition.html)
    """
    condition_entries = []

    icd10_columns = ['icd10_primary', 'icd10_secondary']

    for col in icd10_columns:
        # Some records contain multiple comma-separated icd10 codes in the same field.
        # It has been confirmed with the source of the data that those are instances of multiple
        # coding (https://www.hl7.org/fhir/icd.html#multiple-coding) which should remain together.
        # Removing the comma here to conform with the FHIR standard.
        # - drr  12/14/21
        icd10_code = standardize_whitespace(record.get(col, '').replace(',',' '))

        if icd10_code:
            condition_resource = create_condition_resource(icd10_code,
                                    patient_reference,
                                    None,
                                    create_codeable_concept("http://hl7.org/fhir/sid/icd-10", icd10_code),
                                    encounter_reference)

            condition_entries.append(create_resource_entry(
                resource = condition_resource,
                full_url = generate_full_url_uuid()
            ))

    return condition_entries

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

    lat, lng, canonicalized_address = get_geocoded_address(address, cache)

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
                     record: REDCapRecord,
                     patient_reference: dict,
                     location_references: list) -> Optional[tuple]:
    """ Returns a FHIR Encounter resource entry and reference """
    encounter_location_references = create_encounter_location_references(db, record, location_references)

    if not encounter_location_references:
        return None, None

    try:
        hospitalization = create_encounter_hospitalization(record)
    except UnknownHospitalDischargeDisposition as e:
        LOG.warning(e)
        return None, None

    encounter_date = record["collection_date"]
    if not encounter_date:
        return None, None

    # This matches how our clinical parse_uw generates encounter id
    encounter_id = generate_hash(f"{record['mrn']}{record['accession_no']}{encounter_date}".lower())
    encounter_identifier = create_identifier(f"{SFS}/encounter", encounter_id)

    encounter_class = create_encounter_class(record)
    encounter_status = create_encounter_status(record)

    encounter_resource = create_encounter_resource(
        encounter_source = create_redcap_uri(record),
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class,
        encounter_date = encounter_date,
        encounter_status = encounter_status,
        patient_reference = patient_reference,
        location_references = encounter_location_references,
        hospitalization = hospitalization,
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
        "nwh_retro": "RetrospectiveNorthwest",
        "phskc_retro": "RetrospectivePHSKC",
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


def create_encounter_hospitalization(redcap_record: dict) -> Optional[Dict[str, Dict]]:
    """
    Returns an Encounter.hospitalization entry created from a given *redcap_record*.
    (https://www.hl7.org/fhir/encounter-definitions.html#Encounter.hospitalization)
    """
    disposition = discharge_disposition(redcap_record)

    # For now, dischargeDisposition is the only info we store in
    # Encounter.hospitalization. If this info isn't available, skip creating
    # this resource entry.
    if not disposition:
        return None

    return {
        "dischargeDisposition": create_codeable_concept(
            system = 'http://hl7.org/fhir/ValueSet/encounter-discharge-disposition',
            code = disposition,
        )
    }


def discharge_disposition(redcap_record: dict) -> Optional[str]:
    """
    Given a *redcap_record*, returns the mapped FHIR
    Encounter.hospitalization.dischargeDisposition code
    (https://www.hl7.org/fhir/valueset-encounter-discharge-disposition.html)
    """

    disposition = redcap_record['discharge_disposition']
    if not disposition:
        return None

    # Lowercase, remove leading number characters, and standardize whitespace
    standardized_disposition = standardize_whitespace(re.sub('^\d+', '', disposition.lower()))

    if standardized_disposition.startswith('disch/trans/planned ip readm'):
        # This feels like sensitive information. Don't code the entire string.
        return 'other-hcf'

    mapper = {
        'against medical advice'                                : 'aadvice',
        'ama: against medical advice'                           : 'aadvice',
        'expired'                                               : 'exp',
        'expired: expired'                                      : 'exp',
        'deceased - o: deceased - organ donor'                  : 'exp',
        'deceased - organ donor'                                : 'exp',
        'home health care'                                      : 'home',
        'home hlth: home health care'                           : 'home',
        'home/self care'                                        : 'home',
        'home: home/self care'                                  : 'home',
        'hosp-med fac: hospice - medical facility'              : 'hosp',
        'hospice - home'                                        : 'hosp',
        'hospice-hm: hospice - home'                            : 'hosp',
        'hospice - medical facility'                            : 'hosp',
        'disch/transferred to long-term care hosp'              : 'long',
        'disch/trans/planned readm to long term care hospital'  : 'long',
        'ltc: disch/transferred to long-term care hosp'         : 'long',
        'disch/trans to court/law enforcement'                  : 'oth',
        'disch/trans : disch/trans to court/law enforcement'    : 'oth',
        'disch/trans to a designated disaster alternate care'   : 'oth',
        'disch/trans/planned readm to court/law enforcement'    : 'oth',
        'oth inst: other institution - not defined elsewhere'   : 'oth',
        'other institution - not defined elsewhere'             : 'oth',
        'ed dismiss - no show'                                  : 'oth',
        'ed dismiss - lwbs'                                     : 'oth',
        'disch/trans/planned readm to other institution-not defined elsewhere' : 'oth',
        'transfer to hospital'                                  : 'other-hcf',
        'transfer to : transfer to hospital'                    : 'other-hcf',
        'icf: icf- intermediate care facility'                  : 'other-hcf',
        'icf- intermediate care facility'                       : 'other-hcf',
        'disch/trans/planned readm to icf-intermediate care facility' : 'other-hcf',
        'ca ctr/chld : designated cancer center or childrens hospital': 'other-hcf',
        'designated cancer center or children\'s hospital'      : 'other-hcf',
        'disch/trans fed hospital'                              : 'other-hcf',
        'dischrg/tr: disch/trans fed hospital'                  : 'other-hcf',
        'disch/trans/planned readm to a federal hospital'       : 'other-hcf',
        'disch/trans/planned readm to designated cancer ctr or children\'s hospital': 'other-hcf',
        'disch/trans/planned readm to hospital'                 : 'other-hcf',
        #'Disch/Trans/Planned IP Readm between Service Area 20 NW/UWMC Campus': 'other-hcf',
        'discharged/transferred to a hospital-based medicare approved swing bed' : 'other-hcf',
        'disch/trans to a distinct psych unit/hospital'         : 'psy',
        'dsch/tran: disch/trans to a distinct psych unit/hospital': 'psy',
        'disch/trans/planned readm to a distinct psych unit/hospital': 'psy',
        'disch/trans to a distinct rehab unit/hospital'         : 'rehab',
        'disch/trans/planned readm to a distinct rehab unit/hospital' : 'rehab',
        'dis/trans: disch/trans to a distinct rehab unit/hospital': 'rehab',
        'disch/trans/planned readm to snf-skilled nursing facility': 'snf',
        'disch/trans to a nursing fac-medicaid cert'            : 'snf',
        'snf-skilled nursing facility'                          : 'snf',
        'snf: snf-skilled nursing facility'                     : 'snf',
        'still a patient'                                       : None,
        'still a pati: still a patient'                         : None,
    }

    if standardized_disposition not in mapper:
        # Commenting out this exception until the Codebook is defined in REDCap to limit values
        # for discharge disposition. For now we will map unknown values to `None` but this can
        # be reverted to raise `UnknownHospitalDischargeDisposition` once tbe REDCap Codebook
        # has been updated and all possible values have been added to the map above.
        # -drr 1/3/22

        #raise UnknownHospitalDischargeDisposition("Unknown discharge disposition value "
        #    f"«{standardized_disposition}» for barcode «{redcap_record['barcode']}».")

        return None

    return mapper[standardized_disposition]


def create_encounter_class(redcap_record: dict) -> dict:
    """
    Creates an Encounter.class coding from a given *redcap_record*. If no
    encounter class is given, defaults to the coding for `AMB`.

    This attribute is required by FHIR for an Encounter resource.
    (https://www.hl7.org/fhir/encounter-definitions.html#Encounter.class)
    """
    encounter_class = redcap_record.get('patient_class', '')

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
        "surgery admit" : "IMP",
    }

    standardized_encounter_class = standardize_whitespace(encounter_class.lower())

    if standardized_encounter_class and standardized_encounter_class not in mapper:
        raise Exception(f"Unknown encounter class «{encounter_class}» found in "
                        f"REDCAP_URL: {REDCAP_URL} PROJECT_ID: {PROJECT_ID} barcode: "
                        f"{redcap_record.get('barcode', 'UNKNOWN')}.")

    # Default to 'AMB' if encounter_class not defined
    return create_coding(
        system = "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code = mapper.get(standardized_encounter_class, 'AMB')
    )


def create_encounter_status(redcap_record: dict) -> str:
    """
    Returns an Encounter.status code from a given *redcap_record*. Defaults to
    'finished' if no encounter status is found, because we can assume this
    UW Retrospective encounter was an outpatient encounter.

    This attribute is required by FHIR for an Encounter resource.
    (https://www.hl7.org/fhir/encounter-definitions.html#Encounter.status)
    """
    status = redcap_record['encounter_status']
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
        raise Exception(f"Unknown encounter status «{standardized_status}» found in "
                        f"REDCAP_URL: {REDCAP_URL} PROJECT_ID: {PROJECT_ID} barcode: "
                        f"{redcap_record.get('barcode', 'UNKNOWN')}.")

    return mapper[standardized_status]


def create_immunization(record: dict, patient_reference: dict) -> Optional[list]:
    """ Returns a FHIR Immunization resource entry """
    immunization_entries = []

    immunization_columns = [
        {
            "status": "covid_status_1",
            "date": "covid_date_1",
            "name": "covid_vaccine"
        },
        {
            "status": "covid_status_2",
            "date": "covid_date_2",
            "name": "covid_vaccine"
        },
        {
            "status": "flu_status",
            "date": "flu_date",
            "name": None
        }
    ]

    vaccine_mapper = {
        "covid-19 moderna mrna 18 yrs and older": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "207",
            "display": "COVID-19, mRNA, LNP-S, PF, 100 mcg or 50 mcg dose",
        },
        "covid-19 moderna mrna 12 yrs and older": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "207",
            "display": "COVID-19, mRNA, LNP-S, PF, 100 mcg or 50 mcg dose",
        },
        "covid-19 moderna mrna lnp-s": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "207",
            "display": "COVID-19, mRNA, LNP-S, PF, 100 mcg or 50 mcg dose",
        },
        "covid-19 pfizer mrna lnp-s tris-sucrose 5-11 years old": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "218",
            "display": "COVID-19, mRNA, LNP-S, PF, 10 mcg/0.2 mL dose, tris-sucrose",
        },
        "covid-19 pfizer mrna purple cap": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "208",
            "display": "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose",
        },
        "covid-19 pfizer mrna 12 yrs and older (purple cap)": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "208",
            "display": "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose",
        },
        "covid-19 pfizer mrna tris-sucrose 5-11 years old": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "218",
            "display": "COVID-19, mRNA, LNP-S, PF, 10 mcg/0.2 mL dose, tris-sucrose",
        },
        "covid-19 pfizer mrna tris-sucrose 5-11 yrs old": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "218",
            "display": "COVID-19, mRNA, LNP-S, PF, 10 mcg/0.2 mL dose, tris-sucrose",
        },
        "covid-19 pfizer mrna tris-sucrose gray cap": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "208",
            "display": "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose",
        },
        "covid-19 pfizer mrna tris-sucrose 12 yrs and older (gray cap)": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "208",
            "display": "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose",
        },
        "covid-19 pfizer mrna lnp-s (comirnaty)": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "208",
            "display": "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose",
        },
        "covid-19 pfizer mrna lnp-s": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "208",
            "display": "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose",
        },
        "covid-19 astrazeneca vector-nr rs-chadox1": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "210",
            "display": "COVID-19 vaccine, vector-nr, rS-ChAdOx1, PF, 0.5 mL"
        },
        "covid-19 novavax subunit rs-nanoparticle": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "211",
            "display": "COVID-19 vaccine, Subunit, rS-nanoparticle+Matrix-M1 Adjuvant, PF, 0.5 mL",
        },
        "covid-19 janssen vector-nr rs-ad26": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "212",
            "display": "COVID-19 vaccine, vector-nr, rS-Ad26, PF, 0.5 mL",
        },
        "covid-19, unspecified": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "213",
            "display": "COVID-19 vaccine, UNSPECIFIED",
        },
        "flu unspecified": {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "88",
            "display": "influenza, unspecified formulation",
        },
        "": None
    }

    for column_map in immunization_columns:
        # Validate vaccination status
        immunization_status = standardize_whitespace(record[column_map["status"]]).lower()
        if immunization_status not in ["y", "n", ""]:
            raise UnknownImmunizationStatus (f"Unknown immunization status «{immunization_status}».")

        # Standardize vaccine name
        if column_map["status"] == "flu_status" and column_map["name"] == None:
            vaccine_name = "flu unspecified"
        else:
            vaccine_name = standardize_whitespace(record[column_map["name"]]).lower()

        # Validate vaccine name and determine CVX code
        vaccine_code = None
        if vaccine_name in vaccine_mapper:
            vaccine_code = vaccine_mapper[vaccine_name]
        else:
            raise UnknownVaccine (f"Unknown vaccine «{vaccine_name}».")

        # Standardize date format
        immunization_date = record[column_map["date"]]

        if immunization_status == "y" and vaccine_code:
            immunization_identifier_hash = generate_hash(f"{record['mrn']}{vaccine_code['code']}{immunization_date}".lower())
            immunization_identifier = create_identifier(f"{SFS}/immunization", immunization_identifier_hash)

            immunization_resource = create_immunization_resource(
                patient_reference = patient_reference,
                immunization_identifier = [immunization_identifier],
                immunization_date = immunization_date,
                immunization_status = "completed",
                vaccine_code = vaccine_code,
            )

            immunization_entries.append(create_resource_entry(
                resource = immunization_resource,
                full_url = generate_full_url_uuid()
            ))

    return immunization_entries


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

    if record["race"]:
        items["race"] = []
        for code in race(record["race"]):
            items["race"].append({ 'valueCoding': create_coding(f"{SFS}/race", code)})

    if record["ethnicity"]:
        items["ethnicity"] = [{ 'valueBoolean': ethnicity(record["ethnicity"]) }]

    questionnaire_items: List[dict] = []
    for key,value in items.items():
        questionnaire_items.append(create_questionnaire_response_item(
            question_id = key,
            answers = value
        ))

    return questionnaire_items


def create_clinical_result_observation_resource(redcap_record: dict) -> Optional[List[dict]]:
    """
    Determine the clinical results based on responses in *redcap_record* and
    create observation resources for each result following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/observation.html)
    """
    code_map = {
        '1240581000000104': {
            'system': 'http://snomed.info/sct',
            'code': '1240581000000104',
            'display': '2019-nCoV (novel coronavirus) detected',
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

    # Create intermediary, mapped clinical results using SNOMED codes.
    # This is useful in removing duplicate tests (e.g. multiple tests run for
    # Influenza A)
    results = mapped_snomed_test_results(redcap_record)

    # Create observation resources for all results in clinical tests
    diagnostic_results: Any = {}

    for index, finding in enumerate(results):
        new_observation = observation_resource('clinical')
        new_observation['id'] = 'result-' + str(index + 1)
        new_observation['code']['coding'] = [code_map[finding]]
        new_observation['valueBoolean'] = results[finding]
        diagnostic_results[finding] = (new_observation)

    return list(diagnostic_results.values()) or None


def present(redcap_record: dict, test: str) -> Optional[bool]:
    """
    Returns a mapped boolean *test* result from a given *redcap_record*. A
    return value of `None` means no test results are available.
    """
    result = redcap_record[test]

    # Lowercase, remove non-alpahnumeric characters(except spaces), then standardize whitespace
    # Removal of non-alphanumeric characters is to account for inconsistencies in the data received
    standardized_result = standardize_whitespace(re.sub(r'[^a-z0-9 ]+','',result.lower())) if result else None

    if not standardized_result:
        return None

    test_result_prefix_map = {
        'negative'                              : False,
        'none detected'                         : False,
        'not detected'                          : False,
        'detected'                              : True,
        'positive'                              : True,
        'cancel'                                : None,
        'disregard'                             : None,
        'duplicate request'                     : None,
        'inconclusive'                          : None, # XXX: Ingest this someday as present = null?
        'indeterminate'                         : None, # XXX: Ingest this someday as present = null?
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

    raise UnknownTestResult(f"Unknown test result value «{standardized_result}» for «{redcap_record['barcode']}».")


def mapped_snomed_test_results(redcap_record: dict) -> Dict[str, bool]:
    """
    Given a *redcap_record*, returns a dict of the mapped SNOMED clinical
    finding code and the test result.
    """
    # I'm using a British version (1) of snomed for COVID-19 rather than the
    # international verison, because it appears there is still no
    # observation result for SARS-CoV-2 in the latest international edition
    # (2).
    #
    # 1. https://snomedbrowser.com/Codes/Details/1240581000000104
    # 2: https://www.snomed.org/news-and-events/articles/march-2020-interim-snomedct-release-COVID-19
    #
    # -- kfay, 11 Mar 2020
    redcap_to_snomed_map = {
        'ncvrt': '1240581000000104',
        'revfla': '181000124108',
        'fluapr': '181000124108',
        'revflb': '441345003',
        'flubpr': '441345003',
        'revrsv': '441278007',
        'revrhn': '440925005',
        'revadv': '440930009',
    }

    results: Dict[str, bool] = {}

    # Populate dict of tests administered during encounter by filtering out
    # null results. Map the REDCap test variable to the snomed code. In the
    # event of duplicate clinical findings, prioritize keeping positive results.
    for test in redcap_to_snomed_map:
        code = redcap_to_snomed_map[test]

        # Skip updating results for tests already marked as positive
        if results.get(code):
            continue

        try:
            result = present(redcap_record, test)
        except UnknownTestResult as e:
            LOG.warning(e)
            continue

        # Don't add empty or inconclusive results
        if result is None:
            continue

        results[code] = result

    return results


class UnknownSampleOrigin(ValueError):
    """
    Raised by :function: `create_encounter_location_references` if it finds
    a sample_origin that is not among a set of expected values
    """
    pass

class UnknownHospitalDischargeDisposition(ValueError):
    """
    Raised by :function: `discharge_disposition` if it finds
    a discharge disposition value that is not among a set of mapped values
    """
    pass

class UnknownTestResult(ValueError):
    """
    Raised by :function: `present` if it finds a test result
    that is not among a set of mapped values
    """
    pass

class UnknownImmunizationStatus(ValueError):
    """
    Raised by :function: `create_immunization` if it finds a status
    that is not among a set of mapped values
    """
    pass

class UnknownVaccine(ValueError):
    """
    Raised by :function: `create_immunization` if it finds a vaccine
    name that is not among a set of mapped values
    """
    pass
