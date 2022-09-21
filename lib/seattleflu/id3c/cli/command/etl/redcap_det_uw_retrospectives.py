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
from seattleflu.id3c.cli.command import age_ceiling
from . import standardize_whitespace, first_record_instance, race, ethnicity
from .fhir import *
from .clinical_retrospectives import *
from .redcap_map import *

LOG = logging.getLogger(__name__)

SFS = "https://seattleflu.org"
REDCAP_URL = "https://redcap.iths.org/"
PROJECT_ID = 19915

REVISION = 5

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
    location_entries, location_references = create_resident_locations(redcap_record, db, cache)
    encounter_entry, encounter_reference = create_encounter(db, redcap_record, patient_reference, location_references)

    if not encounter_entry:
        LOG.info("Skipping clinical data pull with insufficient information to construct encounter")
        return None

    questionnaire_response_entry = create_questionnaire_response(redcap_record, patient_reference, encounter_reference, determine_questionnaire_items)

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

    # COVID-19 vaccine CVX codes were sourced from here:
    # https://www.cdc.gov/vaccines/programs/iis/COVID-19-related-codes.html
    cvx_codes = {
        88: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "88",
            "display": "influenza, unspecified formulation",
        },
        207: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "207",
            "display": "COVID-19, mRNA, LNP-S, PF, 100 mcg or 50 mcg dose",
        },
        208: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "208",
            "display": "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3 mL dose",
        },
        210: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "210",
            "display": "COVID-19 vaccine, vector-nr, rS-ChAdOx1, PF, 0.5 mL"
        },
        211: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "211",
            "display": "COVID-19 vaccine, Subunit, rS-nanoparticle+Matrix-M1 Adjuvant, PF, 0.5 mL",
        },
        212: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "212",
            "display": "COVID-19 vaccine, vector-nr, rS-Ad26, PF, 0.5 mL",
        },
        213: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "213",
            "display": "COVID-19 vaccine, UNSPECIFIED",
        },
        218: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "218",
            "display": "COVID-19, mRNA, LNP-S, PF, 10 mcg/0.2 mL dose, tris-sucrose",
        },
        511: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "511",
            "display": "COVID-19 IV Non-US Vaccine (CoronaVac, Sinovac)",
        },
    }

    vaccine_mapper = {
        "flu unspecified":                                                  88,
        "covid-19 moderna mrna lnp-s":                                      207,
        "covid-19 moderna mrna 12 yrs and older":                           207,
        "covid-19 moderna mrna 18 yrs and older":                           207,
        "covid-19 pfizer mrna lnp-s":                                       208,
        "covid-19 pfizer mrna lnp-s (comirnaty)":                           208,
        "covid-19 pfizer mrna purple cap":                                  208,
        "covid-19 pfizer mrna 12 yrs and older (purple cap)":               208,
        "covid-19 pfizer mrna tris-sucrose gray cap":                       208,
        "covid-19 pfizer mrna tris-sucrose 12 yrs and older (gray cap)":    208,
        "covid-19 astrazeneca vector-nr rs-chadox1":                        210,
        "covid-19 novavax subunit rs-nanoparticle":                         211,
        "covid-19 novavax subunit adjuvanted":                              211,
        "covid-19 janssen vector-nr rs-ad26":                               212,
        "covid-19, unspecified":                                            213,
        "covid-19 pfizer mrna lnp-s tris-sucrose 5-11 years old":           218,
        "covid-19 pfizer mrna tris-sucrose 5-11 years old":                 218,
        "covid-19 pfizer mrna tris-sucrose 5-11 yrs old":                   218,
        "covid-19 sinovac inactivated, non-us (coronavac)":                 511,
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
            vaccine_code = cvx_codes[vaccine_mapper[vaccine_name]] if vaccine_mapper[vaccine_name] else None
        else:
            raise UnknownVaccine (f"Unknown vaccine «{vaccine_name}».")

        # Standardize dates into ISO format. Possible formats are YYYY-MM-DD or MM/DD/YYYY
        immunization_date = record[column_map["date"]]
        if '/' in immunization_date:
            immunization_date = datetime.strptime(immunization_date, '%m/%d/%Y').strftime('%Y-%m-%d')

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


class UnknownHospitalDischargeDisposition(ValueError):
    """
    Raised by :function: `discharge_disposition` if it finds
    a discharge disposition value that is not among a set of mapped values
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
