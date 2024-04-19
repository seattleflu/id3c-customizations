"""
Process clinical documents into the relational warehouse.
"""
import click
import logging
import re
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, Dict
from id3c.cli.command import with_database_session
from id3c.db import find_identifier
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
from id3c.cli.command.etl.redcap_det import insert_fhir_bundle

from id3c.cli.command.etl import (
    etl,

    age,
    age_to_delete,
    find_or_create_site,
    find_sample,
    find_location,
    update_sample,
    upsert_encounter,
    upsert_individual,
    upsert_encounter_location,

    SampleNotFoundError,
    UnknownEthnicGroupError,
    UnknownFluShotResponseError,
    UnknownCovidScreenError,
    UnknownCovidShotResponseError,
    UnknownCovidShotManufacturerError,
    UnknownSiteError,
    UnknownAdmitEncounterResponseError,
    UnknownAdmitICUResponseError,

)
from . import race, ethnicity
from .fhir import *
from .clinical_retrospectives import *
from .redcap_map import map_symptom


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each clinical
# record when the clinical record is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# clinical records lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all clinical records,
# this revision number should be incremented.
REVISION = 4


@etl.command("clinical", help = __doc__)
@with_database_session

def etl_clinical(*, db: DatabaseSession):
    LOG.debug(f"Starting the clinical ETL routine, revision {REVISION}")

    # Fetch and iterate over clinical records that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same clinical records.
    LOG.debug("Fetching unprocessed clinical records")

    clinical = db.cursor("clinical")
    clinical.execute("""
        select clinical_id as id, document
          from receiving.clinical
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "revision": REVISION }]),))

    for record in clinical:
        with db.savepoint(f"clinical record {record.id}"):
            LOG.info(f"Processing clinical record {record.id}")

            # Check validity of barcode
            received_sample_identifier = sample_identifier(db,
                record.document["barcode"])

            # Skip row if no matching identifier found
            if received_sample_identifier is None:
                LOG.info("Skipping due to unknown barcode " + \
                          f"{record.document['barcode']}")
                mark_skipped(db, record.id)
                continue

            # Check sample exists in database
            sample = find_sample(db,
                identifier = received_sample_identifier)

            # Skip row if sample does not exist
            if sample is None:
                LOG.info("Skipping due to missing sample with identifier " + \
                            f"{received_sample_identifier}")
                mark_skipped(db, record.id)
                continue

            # Most of the time we expect to see existing sites so a
            # select-first approach makes the most sense to avoid useless
            # updates.
            site = find_or_create_site(db,
                identifier = site_identifier(record.document["site"]),
                details    = {"type": "retrospective"})


            # PHSKC and KP2023 will be handled differently than other clinical records, converted
            # to FHIR format and inserted into receiving.fhir table to be processed
            # by the FHIR ETL. When time allows, SCH and KP should follow suit.
            # Since KP2023 and KP samples both have KaiserPermanente as their site in id3c,
            # use the ndjson document's site to distinguish KP vs KP2023 samples
            if site.identifier == 'RetrospectivePHSKC' or record.document["site"].upper() == 'KP2023':
                fhir_bundle = generate_fhir_bundle(db, record.document, site.identifier)
                insert_fhir_bundle(db, fhir_bundle)

            else:
                # Most of the time we expect to see new individuals and new
                # encounters, so an insert-first approach makes more sense.
                # Encounters we see more than once are presumed to be
                # corrections.
                individual = upsert_individual(db,
                    identifier  = record.document["individual"],
                    sex         = sex(record.document["AssignedSex"]))

                encounter = upsert_encounter(db,
                    identifier      = record.document["identifier"],
                    encountered     = record.document["encountered"],
                    individual_id   = individual.id,
                    site_id         = site.id,
                    age             = age(record.document),
                    details         = encounter_details(record.document))

                sample = update_sample(db,
                    sample = sample,
                    encounter_id = encounter.id)

                # Link encounter to a Census tract, if we have it
                tract_identifier = record.document.get("census_tract")

                if tract_identifier:
                    # Special-case float-like identifiers in earlier date
                    tract_identifier = re.sub(r'\.0$', '', str(tract_identifier))

                    tract = find_location(db, "tract", tract_identifier)
                    assert tract, f"Tract «{tract_identifier}» is unknown"

                    upsert_encounter_location(db,
                        encounter_id = encounter.id,
                        relation = "residence",
                        location_id = tract.id)

            mark_processed(db, record.id, {"status": "processed"})

            LOG.info(f"Finished processing clinical record {record.id}")


def create_encounter(db: DatabaseSession,
                     record: dict,
                     patient_reference: dict,
                     location_references: list) -> Optional[tuple]:
    """ Returns a FHIR Encounter resource entry and reference """
    encounter_location_references = create_encounter_location_references(db, record, location_references)

    if not encounter_location_references:
        return None, None

    encounter_date = record["encountered"]
    if not encounter_date:
        return None, None

    encounter_id = record["identifier"]
    encounter_identifier = create_identifier(f"{SFS}/encounter", encounter_id)

    encounter_class = create_encounter_class(record)
    encounter_status = create_encounter_status(record)
    record_source = create_provenance(record)

    encounter_resource = create_encounter_resource(
        encounter_source = record_source,
        encounter_identifier = [encounter_identifier],
        encounter_class = encounter_class,
        encounter_date = encounter_date,
        encounter_status = encounter_status,
        patient_reference = patient_reference,
        location_references = encounter_location_references,
    )

    return create_entry_and_reference(encounter_resource, "Encounter")


def determine_questionnaire_items(record: dict) -> List[dict]:
    """ Returns a list of FHIR Questionnaire Response answer items """
    items: Dict[str, Any] = {}

    if record.get("age", None):
        items["age"] = [{ 'valueInteger': (int(record["age"]))}]

    if record.get("race", None):
        items["race"] = []
        for code in race(record["race"]):
            items["race"].append({ 'valueCoding': create_coding(f"{SFS}/race", code)})

    if record.get("ethnicity", None):
        items["ethnicity"] = [{ 'valueBoolean': ethnicity(record["ethnicity"]) }]

    if record.get("if_symptoms_how_long", None):
        items["if_symptoms_how_long"] = [{ 'valueString': if_symptoms_how_long(record["if_symptoms_how_long"])}]

    if record.get("vaccine_status", None):
        items["vaccine_status"] = [{ 'valueString': covid_vaccination_status(record["vaccine_status"])}]

    if record.get("inferred_symptomatic", None):
        items["inferred_symptomatic"] = [{ 'valueBoolean': inferred_symptomatic(record["inferred_symptomatic"])}]

    if record.get("survey_have_symptoms_now", None):
        items["survey_have_symptoms_now"] = [{ 'valueBoolean': survey_have_symptoms_now(record["survey_have_symptoms_now"])}]

    if record.get("survey_testing_because_exposed", None):
        items["survey_testing_because_exposed"] = [{ 'valueString': survey_testing_because_exposed(record["survey_testing_because_exposed"])}]


    questionnaire_items: List[dict] = []
    for key,value in items.items():
        questionnaire_items.append(create_questionnaire_response_item(
            question_id = key,
            answers = value
        ))

    return questionnaire_items


def generate_fhir_bundle(db: DatabaseSession, record: dict, site_id: str) -> Optional[dict]:

    patient_entry, patient_reference = create_patient(record)

    if not patient_entry:
        LOG.info("Skipping clinical data pull with insufficient information to construct patient")
        return None

    specimen_entry, specimen_reference = create_specimen(record, patient_reference)
    
    # PHSKC metadata include address information, while KP2023 metadata only include census tract information
    # function create_resident_locations will only return a resource if address information is present,
    # so use create_location_tract_only to process KP2023 location metadata
    # ideally would not require site id for this, since it makes it more difficult to incorporate future projects
    if site_id == 'RetrospectivePHSKC':
        location_entries, location_references = create_resident_locations(record)
    elif record["site"].upper() == 'KP2023':
        location_entries, location_references = create_location_tract_only(record)
    else:
        LOG.warning(f'Function generate_fhir_bundle does not currently create location resource entries for site {site_id}')

    encounter_entry, encounter_reference = create_encounter(db, record, patient_reference, location_references)

    if not encounter_entry:
        LOG.info("Skipping clinical data pull with insufficient information to construct encounter")
        return None

    questionnaire_response_entry = create_questionnaire_response(record, patient_reference, encounter_reference, determine_questionnaire_items)

    specimen_observation_entry = create_specimen_observation_entry(specimen_reference, patient_reference, encounter_reference)

    # for now, only run these for PHSKC, because otherwise an UnknownSiteError will be raised when test_coding is run.
    # it would be better to use information from the record to decide whether to run these steps though.
    # create_diagnostic_report returns None if there is no clinical result information, so it doesn't really need
    # to be run inside the if statement, but the diagnostic_code parameter is not type annotated as Optional,
    # and I don't know whether to require diagnostic_code to run create_diagnostic_report, even if it will return None?
    if site_id == 'RetrospectivePHSKC':
        diagnostic_code = create_codeable_concept(
            system = f'{SFS}/presence-absence-panel',
            code = test_coding(record['site'])
        )

        diagnostic_report_resource_entry = create_diagnostic_report(
            record,
            patient_reference,
            specimen_reference,
            diagnostic_code,
            create_clinical_result_observation_resource,
        )
    else:
        diagnostic_report_resource_entry = None

    resource_entries = [
        patient_entry,
        specimen_entry,
        encounter_entry,
        questionnaire_response_entry,
        specimen_observation_entry
    ]

    if location_entries:
        resource_entries.extend(location_entries)

    if diagnostic_report_resource_entry:
        resource_entries.append(diagnostic_report_resource_entry)

    if record["site"].upper() == 'KP2023':
        # KP2023 includes some types of metadata that PHSKC does not
        icd10_condition_entries = create_icd10_conditions_kp2023(record, patient_reference, encounter_reference)
        symptom_condition_entries = create_symptom_conditions(record, patient_reference, encounter_reference)
        immunization_entries = create_immunization_kp2023(record, patient_reference)
        resource_entries.extend(icd10_condition_entries + symptom_condition_entries + immunization_entries)

    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{record['_provenance']['filename']},row:{record['_provenance']['row']}" ,
        entries = list(filter(None, resource_entries))
    )


def create_location_tract_only(record: dict) -> Optional[tuple]:
    """
    Returns a location entry and location reference for a record containing census tract information
    If the record contains address information beyond a census tract, use function create_resident_locations instead
    """
    location_type_system = 'http://terminology.hl7.org/CodeSystem/v3-RoleCode'
    location_type = create_codeable_concept(location_type_system, 'PTRES')
    tract_identifier = record["census_tract"]
    
    if tract_identifier:
        tract_id = create_identifier(f"{SFS}/location/tract", tract_identifier)
        tract_location = create_location_resource([location_type], [tract_id])
        tract_entry, tract_reference = create_entry_and_reference(tract_location, "Location")
        
        # return entry and reference as lists of dicts for consistency with function create_resident_locations
        return [tract_entry], [tract_reference]
    else:
        return None, None


def create_immunization_kp2023(record: dict, patient_reference: dict) -> list:
    """ Returns a FHIR Immunization resource entry for each immunization recorded """
    immunization_entries = []

    immunization_columns = [
        {
            "date": "date_flu_1",
            "name": "flu_type_1"
        },
        {
            "date": "date_flu_2",
            "name": "flu_type_2"
        },
        {
            "date": "date_covid_1",
            "name": None
        },
        {
            "date": "date_covid_2",
            "name": None
        },
        {
            "date": "date_covid_3",
            "name": None
        },
        {
            "date": "date_covid_4",
            "name": None
        },
        {
            "date": "date_covid_5",
            "name": None
        },
        {
            "date": "date_covid_6",
            "name": None
        }
    ]

    # CVX codes were sourced from here:
    # https://www2a.cdc.gov/vaccines/iis/iisstandards/vaccines.asp?rpt=cvx
    # and here:
    # https://www2a.cdc.gov/vaccines/iis/iisstandards/vaccines.asp?rpt=tradename
    cvx_codes = {
        88: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "88",
            "display": "influenza, unspecified formulation",
        },
        158: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "158",
            "display": "influenza, injectable, quadrivalent, contains preservative"
        },
        205: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "205",
            "display": "influenza, seasonal vaccine, quadrivalent, adjuvanted, 0.5mL dose, preservative free"
        },
        150: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "150",
            "display": "influenza, injectable, quadrivalent, preservative free"
        },
        185: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "185",
            "display": "seasonal, quadrivalent, recombinant, injectable influenza vaccine, preservative free"
        },
        149: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "149",
            "display": "influenza, live, intranasal, quadrivalent"
        },
        197: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "197",
            "display": "influenza, high-dose seasonal, quadrivalent, 0.7mL dose, preservative free"
        },
        213: {
            "system": "http://hl7.org/fhir/sid/cvx",
            "code": "213",
            "display": "sars-cov-2 (covid-19) vaccine, unspecified"
        },
    }

    flu_vaccine_mapper = {
        "unknown":                          88,
        "afluria quadrivalent":             158,
        "fluad quadrivalent":               205,
        "fluarix quadrivalent":             150,
        "flublok quadrivalent":             185,
        "flucelvax quadrivalent":           88, # mark as unspecified because not known whether with or without preservative
        "flulaval quadrivalent":            88, # mark as unspecified not known whether with or without preservative
        "flumist quadrivalent":             149,
        "fluzone high-dose quadrivalent":   88, # probably 197, but marking as unspecified because don't want to assume not southern hemisphere version
        "fluzone quadrivalent":             88 # mark as unspecified because not known whether with or without preservative, or whether pediatric
    }

    for column_map in immunization_columns:
        # if there is no date, do not create a resource entry
        # is this a good assumption? what if there is a flu vaccine name but no date?
        if not record[column_map['date']]:
            continue

        # assign date. should be in ISO format after id3c clinical parse-kp2023
        immunization_date = record[column_map['date']]

        # assign name and code for flu vaccines
        if 'flu' in column_map['date']:
            if record[column_map['name']]:
                # assign and format vaccine name
                vaccine_name = record[column_map['name']].lower()
            else:
                # if there is a date but no vaccine name, assign the name as 'unknown' which will map to unspecified flu vaccine
                vaccine_name = 'unknown' # should we throw a warning here?

            # Validate vaccine name and determine CVX code
            vaccine_code = None
            if vaccine_name in flu_vaccine_mapper:
                vaccine_code = cvx_codes[flu_vaccine_mapper[vaccine_name]] if flu_vaccine_mapper[vaccine_name] else None
            else:
                raise UnknownVaccine (f"Unknown vaccine «{vaccine_name}».") 

        # assign name and code for covid vaccines
        elif 'covid' in column_map['date']:
            vaccine_code = cvx_codes[213] # covid vaccines are not specified in this study, so assign code for unspecified covid-19 vaccine

        if vaccine_code:
            # create hash from collection_id, which is hashed individual id, plus vaccine code and date administered
            immunization_identifier_hash = generate_hash(f"{record['collection_id']}{vaccine_code['code']}{immunization_date}".lower())
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


def create_symptom_conditions(record: dict, patient_reference: dict, encounter_reference: dict) -> list:
    """ Returns a FHIR Condition resource for each symptom present in a record """

    condition_entries = []

    for symptom in record['symptom']:
        mapped_symptom_name = map_symptom(symptom)
        onset_date = record['date_symptom_onset']
        symptom_code = create_codeable_concept(
            system = f"{SFS}/symptom",
            code = mapped_symptom_name
        )
        

        condition_resource = create_condition_resource(mapped_symptom_name,
                                patient_reference,
                                onset_date,
                                symptom_code,
                                encounter_reference
                            )

        condition_entries.append(create_resource_entry(
            resource = condition_resource,
            full_url = generate_full_url_uuid()
        ))

    return condition_entries


def create_icd10_conditions_kp2023(record:dict, patient_reference: dict, encounter_reference: dict) -> list:
    """
    Create a condition resource for each ICD-10 code, following the FHIR format
    (http://www.hl7.org/implement/standards/fhir/condition.html)
    """

    condition_entries = []

    icd10_codes = {
        "I25": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "I25",
            "display": "chronic ischemic heart disease"
        },
        "I50": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "I50",
            "display": "heart failure"
        },
        "J41": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "J41",
            "display": "simple and mucopurulent chronic bronchitis"
        },
        "J42": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "J42",
            "display": "unspecified chronic bronchitis"
        },
        "J44": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "J44",
            "display": "other chronic obstructive pulmonary disease"
        },
        "J45": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "J45",
            "display": "asthma"
        },
        "J47": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "J47",
            "display": "bronchiectasis"
        },
        "J80": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "J80",
            "display": "acute respiratory distress syndrome"
        },
        "E11": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "E11",
            "display": "type 2 diabetes mellitus"
        },
        "Z51.1": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "Z51.1",
            "display": "encounter for antineoplastic chemotherapy and immunotherapy"
        },
        "Z94": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "Z94",
            "display": "transplanted organ and tissue status"
        },
        "B18": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "B18",
            "display": "chronic viral hepatitis"
        },
        "K70": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "K70",
            "display": "alcoholic liver disease"
        },
        "C00": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C00",
            "display": "malignant neoplasm of lip"
        },
        "C01": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C01",
            "display": "malignant neoplasm of base of tongue"
        },
        "C02": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C02",
            "display": "malignant neoplasm of other and unspecified parts of tongue"
        },
        "C03": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C03",
            "display": "malignant neoplasm of gum"
        },
        "C04": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C04",
            "display": "malignant neoplasm of floor of mouth"
        },
        "C05": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C05",
            "display": "malignant neoplasm of palate"
        },
        "C06": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C06",
            "display": "malignant neoplasm of other and unspecified parts of mouth"
        },
        "C07": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C07",
            "display": "malignant neoplasm of parotid gland"
        },
        "C08": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C08",
            "display": "malignant neoplasm of other and unspecified major salivary glands"
        },
        "C09": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C09",
            "display": "malignant neoplasm of tonsil"
        },
        "C10": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C10",
            "display": "malignant neoplasm of oropharynx"
        },
        "C11": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C11",
            "display": "malignant neoplasm of nasopharynx"
        },
        "C12": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C12",
            "display": "malignant neoplasm of pyriform sinus"
        },
        "C13": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C13",
            "display": "malignant neoplasm of hypopharynx"
        },
        "C14": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C14",
            "display": "malignant neoplasm of other and ill-defined sites in the lip, oral cavity and pharynx"
        },
        "C15": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C15",
            "display": "malignant neoplasm of esophagus"
        },
        "C16": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C16",
            "display": "malignant neoplasm of stomach"
        },
        "C17": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C17",
            "display": "malignant neoplasm of small intestine"
        },
        "C18": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C18",
            "display": "malignant neoplasm of colon"
        },
        "C19": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C19",
            "display": "malignant neoplasm of rectosigmoid junction"
        },
        "C20": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C20",
            "display": "malignant neoplasm of rectum"
        },
        "C21": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C21",
            "display": "malignant neoplasm of anus and anal canal"
        },
        "C22": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C22",
            "display": "malignant neoplasm of liver and intrahepatic bile ducts"
        },
        "C23": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C23",
            "display": "malignant neoplasm of gallbladder"
        },
        "C24": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C24",
            "display": "malignant neoplasm of other and unspecified parts of biliary tract"
        },
        "C25": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C25",
            "display": "malignant neoplasm of pancreas"
        },
        "C26": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C26",
            "display": "malignant neoplasm of other and ill-defined digestive organs"
        },
        "C30": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C30",
            "display": "malignant neoplasm of nasal cavity and middle ear"
        },
        "C31": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C31",
            "display": "malignant neoplasm of accessory sinuses"
        },
        "C32": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C32",
            "display": "malignant neoplasm of larynx"
        },
        "C33": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C33",
            "display": "malignant neoplasm of trachea"
        },
        "C34": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C34",
            "display": "malignant neoplasm of bronchus and lung"
        },
        "C37": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C37",
            "display": "malignant neoplasm of thymus"
        },
        "C38": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C38",
            "display": "malignant neoplasm of heart, mediastinum and pleura"
        },
        "C39": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C39",
            "display": "malignant neoplasm of other and ill-defined sites in the respiratory system and intrathoracic organs"
        },
        "C40": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C40",
            "display": "malignant neoplasm of bone and articular cartilage of limbs"
        },
        "C41": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C41",
            "display": "malignant neoplasm of bone and articular cartilage of other and unspecified sites"
        },
        "C43": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C43",
            "display": "malignant melanoma of skin"
        },
        "C44": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C44",
            "display": "other and unspecified malignant neoplasm of skin"
        },
        "C45": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C45",
            "display": "mesothelioma"
        },
        "C46": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C46",
            "display": "kaposi's sarcoma"
        },
        "C47": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C47",
            "display": "malignant neoplasm of peripheral nerves and autonomic nervous system"
        },
        "C48": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C48",
            "display": "malignant neoplasm of retroperitoneum and peritoneum"
        },
        "C49": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C49",
            "display": "malignant neoplasm of other connective and soft tissue"
        },
        "C4A": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C4A",
            "display": "merkel cell carcinoma"
        },
        "C50": {
            "system": "http://hl7.org/fhir/sid/icd-10",            
            "code": "C50",
            "display": "malignant neoplasms of breast"
        },
        "C51": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C51",
            "display": "malignant neoplasm of vulva"
        },
        "C52": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C52",
            "display": "malignant neoplasm of vagina"
        },
        "C53": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C53",
            "display": "malignant neoplasm of cervix uteri"
        },
        "C54": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C54",
            "display": "malignant neoplasm of corpus uteri"
        },
        "C55": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C55",
            "display": "malignant neoplasm of uterus, part unspecified"
        },
        "C56": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C56",
            "display": "malignant neoplasm of ovary"
        },
        "C57": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C57",
            "display": "malignant neoplasm of other and unspecified female genital organs"
        },
        "C58": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C58",
            "display": "malignant neoplasm of placenta"
        },
        "C60": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C60",
            "display": "malignant neoplasm of penis"
        },
        "C61": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C61",
            "display": "malignant neoplasm of prostate"
        },
        "C62": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C62",
            "display": "malignant neoplasm of testis"
        },
        "C63": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C63",
            "display": "malignant neoplasm of other and unspecified male genital organs"
        },
        "C64": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C64",
            "display": "malignant neoplasm of kidney, except renal pelvis"
        },
        "C65": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C65",
            "display": "malignant neoplasm of renal pelvis"
        },
        "C66": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C66",
            "display": "malignant neoplasm of ureter"
        },
        "C67": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C67",
            "display": "malignant neoplasm of bladder"
        },
        "C68": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C68",
            "display": "malignant neoplasm of other and unspecified urinary organs"
        },
        "C69": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C69",
            "display": "malignant neoplasm of eye and adnexa"
        },
        "C70": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C70",
            "display": "malignant neoplasm of meninges"
        },
        "C71": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C71",
            "display": "malignant neoplasm of brain"
        },
        "C72": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C72",
            "display": "malignant neoplasm of spinal cord, cranial nerves and other parts of central nervous system"
        },
        "C73": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C73",
            "display": "malignant neoplasm of thyroid gland"
        },
        "C74": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C74",
            "display": "malignant neoplasm of adrenal gland"
        },
        "C75": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C75",
            "display": "malignant neoplasm of other endocrine glands and related structures"
        },
        "C76": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C76",
            "display": "malignant neoplasm of other and ill-defined sites"
        },
        "C77": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C77",
            "display": "secondary and unspecified malignant neoplasm of lymph nodes"
        },
        "C78": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C78",
            "display": "secondary malignant neoplasm of respiratory and digestive organs"
        },
        "C79": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C79",
            "display": "secondary malignant neoplasm of other and unspecified sites"
        },
        "C7A": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C7A",
            "display": "malignant neuroendocrine tumors"
        },
        "C7B": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C7B",
            "display": "secondary neuroendocrine tumors"
        },
        "C80": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C80",
            "display": "malignant neoplasm without specification of site"
        },
        "C81": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C81",
            "display": "hodgkin lymphoma"
        },
        "C82": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C82",
            "display": "follicular lymphoma"
        },
        "C83": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C83",
            "display": "non-follicular lymphoma"
        },
        "C84": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C84",
            "display": "mature t/nk-cell lymphomas"
        },
        "C85": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C85",
            "display": "other specified and unspecified types of non-hodgkin lymphoma"
        },
        "C86": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C86",
            "display": "other specified types of t/nk-cell lymphoma"
        },
        "C88": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C88",
            "display": "malignant immunoproliferative diseases and certain other b-cell lymphomas"
        },
        "C90": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C90",
            "display": "multiple myeloma and malignant plasma cell neoplasms"
        },
        "C91": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C91",
            "display": "lymphoid leukemia"
        },
        "C92": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C92",
            "display": "myeloid leukemia"
        },
        "C93": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C93",
            "display": "monocytic leukemia"
        },
        "C94": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C94",
            "display": "other leukemias of specified cell type"
        },
        "C95": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C95",
            "display": "leukemia of unspecified cell type"
        },
        "C96": {
            "system": "http://hl7.org/fhir/sid/icd-10",
            "code": "C96",
            "display": "other and unspecified malignant neoplasms of lymphoid, hematopoietic and related tissue"
        }
    }

    for icd10_code in record['icd10']:
        condition_resource = create_condition_resource(icd10_code,
                                patient_reference,
                                None,
                                create_codeable_concept(
                                    system = icd10_codes[icd10_code]["system"], 
                                    code = icd10_codes[icd10_code]["code"], 
                                    display = icd10_codes[icd10_code]["display"]),
                                encounter_reference
                            )

        condition_entries.append(create_resource_entry(
            resource = condition_resource,
            full_url = generate_full_url_uuid()
        ))

    return condition_entries


def test_coding(site_name: str) -> str:
    """
    Given a *site_name*, returns the code associated with the external test run
    on samples from this site.
    """
    if not site_name:
        LOG.debug("No site name found")
        return "Unknown"

    site_name = site_name.upper()

    code_map = {
        "PHSKC": "phskc-retrospective"
    }

    if site_name not in code_map:
        raise UnknownSiteError(f"Unknown site name «{site_name}»")

    return code_map[site_name]

def site_identifier(site_name: str) -> str:
    """
    Given a *site_name*, returns its matching site identifier.
    """
    if not site_name:
        LOG.debug("No site name found")
        return "Unknown"  # TODO

    site_name = site_name.upper()

    site_map = {
        "UWMC": "RetrospectiveUWMedicalCenter",
        "HMC": "RetrospectiveHarborview",
        "NWH":"RetrospectiveNorthwest",
        "UWNC": "RetrospectiveUWMedicalCenter",
        "SCH": "RetrospectiveChildrensHospitalSeattle",
        "KP": "KaiserPermanente",
        "PHSKC": "RetrospectivePHSKC",
        "KP2023": "KaiserPermanente" # kp2023 samples should be mapped to kp site in id3c; they are marked as kp2023 in manifest document because they will be processed into fhir bundles, unlike earlier kp samples
    }
    if site_name not in site_map:
        raise UnknownSiteError(f"Unknown site name «{site_name}»")

    return site_map[site_name]

def sex(sex_name) -> str:
    """
    Given a *sex_name*, returns its matching sex identifier.

    Raises an :class:`Exception` if the given sex name is unknown.
    """
    if not sex_name:
        LOG.debug("No sex name found")
        return None

    sex_map = {
        "m": "male",
        "f": "female",
        1.0: "male",
        0.0: "female",
        "clinically undetermined": "other",
        "other": "other",
        "x (non-binary)": "other",
        "unknown": None,
    }

    def standardize_sex(sex):
        try:
            if isinstance(sex, str):
                sex = sex.lower()

            return sex if sex in sex_map.values() else sex_map[sex]
        except KeyError:
            raise Exception(f"Unknown sex name «{sex}»") from None

    return standardize_sex(sex_name)


def encounter_details(document: dict) -> dict:
    """
    Describe encounter details in a simple data structure designed to be used
    from SQL.
    """
    details = {
            "age": age_to_delete(document.get("age")), # XXX TODO: Remove age from details

            # XXX TODO: Remove locations from details
            "locations": {
                "home": {
                    "region": document.get("census_tract"),
                }
            },
            "responses": {
                "Race": race(document.get("Race")),
                "FluShot": flu_shot(document.get("FluShot")),
                "AssignedSex": [sex(document.get("AssignedSex"))],
                "HispanicLatino": hispanic_latino(document.get("HispanicLatino")),
                "MedicalInsurance": insurance(document.get("MedicalInsurance")),
                "AdmitDuringThisEncounter": admit_encounter(document.get("AdmitDuringThisEncounter")),
                "AdmitToICU": admit_icu(document.get("AdmitToICU")),
            },
        }

    if "ICD10" in document:
        details["responses"]["ICD10"] = document.get("ICD10")

    if "CovidScreen" in document:
        details["responses"]["CovidScreen"] = covid_screen(document.get("CovidScreen"))

    for k in ["CovidShot1", "CovidShot2", "CovidShot3", "CovidShot4"]:
        if k in document:
            details["responses"][k] = covid_shot(document[k])

    if "CovidShotManufacturer" in document:
        details["responses"]["CovidShotManufacturer"] = covid_shot_maunufacturer(document.get("CovidShotManufacturer"))

    for k in ["CovidShot1Manu", "CovidShot2Manu", "CovidShot3Manu", "CovidShot4Manu"]:
        if k in document:
            details["responses"][k] = covid_shot_maunufacturer(document[k])

    # include vaccine date fields if present and not empty
    for k in ["FluShotDate", "CovidShot1Date", "CovidShot2Date", "CovidShot3Date", "CovidShot4Date"]:
        if document.get(k):
            details["responses"][k] = [document[k]]

    return details


def hispanic_latino(ethnic_group: Optional[Any]) -> list:
    """
    Given an *ethnic_group*, returns yes/no value for HispanicLatino key.
    """
    if ethnic_group is None:
        LOG.debug("No ethnic group response found.")
        return [None]

    ethnic_map = {
        "Not Hispanic or Latino": "no",
        "Non-Hispanic/Latino": "no",
        "Non-Hispanic": "no",
        "Hispanic or Latino": "yes",
        "Hispanic/Latino": "yes",
        "Hispanic": "yes",
        0.0: "no",
        1.0: "yes",
        "Patient Refused/Did Not Wish To Indicate": None,
        "Patient Refused": None,
        "Unknown": None,
        "Unavailable or Unknown": None,
    }

    if ethnic_group not in ethnic_map:
        raise UnknownEthnicGroupError(f"Unknown ethnic group «{ethnic_group}»")

    return [ethnic_map[ethnic_group]]

def flu_shot(flu_shot_response: Optional[Any]) -> list:
    """
    Given a *flu_shot_response*, returns yes/no value for FluShot key.

    >>> flu_shot(0.0)
    ['no']

    >>> flu_shot('TRUE')
    ['yes']

    >>> flu_shot('maybe')
    Traceback (most recent call last):
        ...
    id3c.cli.command.etl.UnknownFluShotResponseError: Unknown flu shot response «maybe»

    """

    if isinstance(flu_shot_response, str):
        flu_shot_response = flu_shot_response.lower()

    if flu_shot_response is None or flu_shot_response == "":
        LOG.debug("No flu shot response found.")
        return [None]

    flu_shot_map = {
        0.0 : "no",
        1.0 : "yes",
        "false": "no",
        "true": "yes",
    }

    if flu_shot_response not in flu_shot_map:
        raise UnknownFluShotResponseError(
            f"Unknown flu shot response «{flu_shot_response}»")

    return [flu_shot_map[flu_shot_response]]

def admit_encounter(admit_encounter_response: Optional[Any]) -> list:
    """
    Given a *admit_encounter_response*, returns yes/no value for AdmitDuringThisEncounter key.

    >>> admit_encounter(0.0)
    ['no']

    >>> admit_encounter('TRUE')
    ['yes']

    >>> admit_encounter('maybe')
    Traceback (most recent call last):
        ...
    id3c.cli.command.etl.UnknownAdmitEncounterResponseError: Unknown admit during encounter response «maybe»

    """

    if isinstance(admit_encounter_response, str):
        admit_encounter_response = admit_encounter_response.lower()

    if admit_encounter_response is None or admit_encounter_response == "":
        LOG.debug("No admit during this encounter response found.")
        return [None]

    admit_encounter_map = {
        0.0 : "no",
        1.0 : "yes",
        "false": "no",
        "true": "yes",
    }

    if admit_encounter_response not in admit_encounter_map:
        raise UnknownAdmitEncounterResponseError(
            f"Unknown admit during encounter response «{admit_encounter_response}»")

    return [admit_encounter_map[admit_encounter_response]]


def admit_icu(admit_icu_response: Optional[Any]) -> list:
    """
    Given a *admit_icu_response*, returns yes/no value for AdmitToICU key.

    >>> admit_icu(0.0)
    ['no']

    >>> admit_icu('TRUE')
    ['yes']

    >>> admit_icu('maybe')
    Traceback (most recent call last):
        ...
    id3c.cli.command.etl.UnknownAdmitICUResponseError: Unknown admit to ICU response «maybe»

    """

    if isinstance(admit_icu_response, str):
        admit_icu_response = admit_icu_response.lower()

    if admit_icu_response is None or admit_icu_response == "":
        LOG.debug("No admit to icu response found.")
        return [None]

    admit_icu_map = {
        0.0 : "no",
        1.0 : "yes",
        "false": "no",
        "true": "yes",
    }

    if admit_icu_response not in admit_icu_map:
        raise UnknownAdmitICUResponseError(
            f"Unknown admit to ICU response «{admit_icu_response}»")

    return [admit_icu_map[admit_icu_response]]

def covid_shot(covid_shot_response: Optional[Any]) -> list:
    """
    Given a *covid_shot_response*, returns yes/no value for CovidShot key(s).

    >>> covid_shot(0.0)
    ['no']

    >>> covid_shot('TRUE')
    ['yes']

    >>> covid_shot('maybe')
    Traceback (most recent call last):
        ...
    id3c.cli.command.etl.UnknownCovidShotResponseError: Unknown COVID shot response «maybe»

    """

    if isinstance(covid_shot_response, str):
        covid_shot_response = covid_shot_response.lower()

    if covid_shot_response is None or covid_shot_response == "":
        LOG.debug("No COVID shot response found.")
        return [None]

    covid_shot_map = {
        0.0 : "no",
        1.0 : "yes",
        "false": "no",
        "true": "yes",
    }

    if covid_shot_response not in covid_shot_map:
        raise UnknownCovidShotResponseError(
            f"Unknown COVID shot response «{covid_shot_response}»")

    return [covid_shot_map[covid_shot_response]]


def covid_shot_maunufacturer(covid_shot_manufacturer_name: Optional[Any]) -> list:
    """
    Given a *covid_shot_manufacturer_name*, returns validated and standarized value.

    >>> covid_shot_maunufacturer('PFIZER')
    ['pfizer']

    >>> covid_shot_maunufacturer('Moderna')
    ['moderna']

    >>> covid_shot_maunufacturer('Janssen')
    ['janssen']

    >>> covid_shot_maunufacturer('SomeCompany')
    Traceback (most recent call last):
        ...
    id3c.cli.command.etl.UnknownCovidShotManufacturerError: Unknown COVID shot manufacturer «somecompany»

    """

    if isinstance(covid_shot_manufacturer_name, str):
        covid_shot_manufacturer_name = covid_shot_manufacturer_name.lower().strip()

    if covid_shot_manufacturer_name is None or covid_shot_manufacturer_name == "":
        LOG.debug("No COVID shot manufacturer name found.")
        return [None]

    valid_covid_manufacturers = [
        "pfizer",
        "moderna",
        "janssen",
    ]

    if covid_shot_manufacturer_name not in valid_covid_manufacturers:
        raise UnknownCovidShotManufacturerError(
            f"Unknown COVID shot manufacturer «{covid_shot_manufacturer_name}»")

    return [covid_shot_manufacturer_name]


def covid_screen(is_covid_screen: Optional[Any]) -> list:
    """
    Given a *is_covid_screen*, returns yes/no value for CovidScreen key.

    >>> covid_screen('FALSE')
    ['no']

    >>> covid_screen('TRUE')
    ['yes']

    >>> covid_screen('maybe')
    Traceback (most recent call last):
        ...
    id3c.cli.command.etl.UnknownCovidScreenError: Unknown COVID screen «maybe»

    """

    if isinstance(is_covid_screen, str):
        is_covid_screen = is_covid_screen.lower()

    if is_covid_screen is None or is_covid_screen == "":
        LOG.debug("No COVID screen value found.")
        return [None]

    covid_screen_map = {
        "false": "no",
        "true": "yes",
        "unknown": None,
    }

    if is_covid_screen not in covid_screen_map:
        raise UnknownCovidScreenError(
            f"Unknown COVID screen «{is_covid_screen}»")

    return [covid_screen_map[is_covid_screen]]


def insurance(insurance_response: Optional[Any]) -> list:
    """
    Given a case-insensitive *insurance_response*, returns corresponding
    insurance identifier.

    Raises an :class:`Exception` if the given insurance name is unknown.

    >>> insurance('medicaid')
    ['government']

    >>> insurance('PRIVATE')
    ['privateInsurance']

    >>> insurance('some scammy insurance company')
    Traceback (most recent call last):
        ...
    Exception: Unknown insurance name «some scammy insurance company»

    """
    if insurance_response is None:
        LOG.debug("No insurance response found.")
        return [None]

    if not isinstance(insurance_response, list):
        insurance_response = [ insurance_response ]

    insurance_map = {
        "commercial": "privateInsurance",
        "comm": "privateInsurance",
        "private": "privateInsurance",
        "medicaid": "government",
        "medicare": "government",
        "tricare": "government",
        "care": "government",
        "caid": "government",
        "financial aid": "other",
        "self-pay": "other",
        "other": "other",
        "self": "other",
        "tce": "other",
        "case rate": "other",
        "wc": "other",
        "unknown": None,
        "none": "none",
    }

    def standardize_insurance(insurance):
        try:
            insurance = insurance.lower()
            return insurance if insurance in insurance_map.values() else insurance_map[insurance]
        except KeyError:
            raise Exception(f"Unknown insurance name «{insurance}»") from None

    return list(map(standardize_insurance, insurance_response))


def if_symptoms_how_long(if_symptoms_how_long_response: Optional[Any]) -> Optional[str]:
    """
    Given a *if_symptoms_how_long_response*, returns a standardized value.
    Raises an :class:`Exception` if the given response is unknown.

    >>> if_symptoms_how_long('1 day')
    '1_day'

    >>> if_symptoms_how_long("I don't have symptoms")
    'no_symptoms'

    >>> if_symptoms_how_long("I don't know")
    Traceback (most recent call last):
        ...
    Exception: Unknown if_symptoms_how_long value «i don't know»

    """

    if isinstance(if_symptoms_how_long_response, str):
        if_symptoms_how_long_response = if_symptoms_how_long_response.strip().lower()

    if if_symptoms_how_long_response is None or if_symptoms_how_long_response == "":
        LOG.debug("No if_symptoms_how_long response found.")
        return None

    symptoms_duration_map = {
        "1 day": "1_day",
        "2 days": "2_days",
        "3 days": "3_days",
        "4 days": "4_days",
        "5 days": "5_days",
        "6 days": "6_days",
        "7 days": "7_days",
        "8 days": "8_days",
        "9+ days": "9_or_more_days",
        "i don't have symptoms": "no_symptoms",
    }

    if if_symptoms_how_long_response not in symptoms_duration_map:
        raise Exception(f"Unknown if_symptoms_how_long value «{if_symptoms_how_long_response}»")

    return symptoms_duration_map[if_symptoms_how_long_response]


def covid_vaccination_status(covid_vaccination_status_response: Optional[Any]) -> Optional[str]:
    """
    Given a *covid_vaccination_status_response*, returns a standardized value.
    Raises an :class:`Exception` if the given response is unknown.

    >>> covid_vaccination_status('Yes I am fully vaccinated.')
    'fully_vaccinated'

    >>> covid_vaccination_status("No but I am partially vaccinated (e.g. 1 dose of a 2-dose series).")
    'partially_vaccinated'

    >>> covid_vaccination_status("I don't know")
    Traceback (most recent call last):
        ...
    Exception: Unknown covid_vaccination_status value «i don't know»

    """

    if isinstance(covid_vaccination_status_response, str):
        covid_vaccination_status_response = covid_vaccination_status_response.lower().strip()

    if covid_vaccination_status_response is None or covid_vaccination_status_response == "":
        LOG.debug("No covid_vaccination_status_response response found.")
        return None

    covid_vaccination_status_map = {
        "yes i am fully vaccinated.":                                                   "fully_vaccinated",
        "no i am not vaccinated.":                                                      "not_vaccinated",
        "no but i am partially vaccinated (e.g. 1 dose of a 2-dose series).":           "partially_vaccinated",
        "yes i am fully vaccinated and i also have received a booster.":                "boosted",
        "yes i am fully vaccinated and i also have received 1 booster dose.":           "boosted",
        "yes i am fully vaccinated and i also have received 2 or more booster doses.":  "boosted_two_plus",
    }

    if covid_vaccination_status_response not in covid_vaccination_status_map:
        raise Exception(f"Unknown covid_vaccination_status value «{covid_vaccination_status_response}»")

    return covid_vaccination_status_map[covid_vaccination_status_response]


def inferred_symptomatic(inferred_symptomatic_response: Optional[Any]) -> Optional[bool]:
    """
    Given a *inferred_symptomatic_response*, returns boolean value.
    Raises an :class:`Exception` if the given response is unknown.

    >>> inferred_symptomatic('FALSE')
    False

    >>> inferred_symptomatic('TRUE')
    True

    >>> inferred_symptomatic('maybe')
    Traceback (most recent call last):
        ...
    Exception: Unknown inferred_symptomatic_response «maybe»

    """

    if isinstance(inferred_symptomatic_response, str):
        inferred_symptomatic_response = inferred_symptomatic_response.lower().strip()

    if inferred_symptomatic_response is None or inferred_symptomatic_response == "":
        LOG.debug("No inferred_symptomatic response found.")
        return None

    inferred_symptomatic_map = {
        "false": False,
        "true": True,
    }

    if inferred_symptomatic_response not in inferred_symptomatic_map:
        raise Exception(f"Unknown inferred_symptomatic_response «{inferred_symptomatic_response}»")

    return inferred_symptomatic_map[inferred_symptomatic_response]


def survey_have_symptoms_now(survey_have_symptoms_now_response: Optional[Any]) -> Optional[bool]:
    """
    Given a *survey_have_symptoms_now_response*, returns boolean value.
    Raises an :class:`Exception` if the given response is unknown.

    >>> survey_have_symptoms_now('yes')
    True

    >>> survey_have_symptoms_now('no')
    False

    >>> survey_have_symptoms_now('maybe')
    Traceback (most recent call last):
        ...
    Exception: Unknown survey_have_symptoms_now_response «maybe»

    """

    if isinstance(survey_have_symptoms_now_response, str):
        survey_have_symptoms_now_response = survey_have_symptoms_now_response.lower().strip()

    if survey_have_symptoms_now_response is None or survey_have_symptoms_now_response == "":
        LOG.debug("No survey_have_symptoms_now response found.")
        return None

    survey_have_symptoms_now_map = {
        "yes": True,
        "no": False,
    }

    if survey_have_symptoms_now_response not in survey_have_symptoms_now_map:
        raise Exception(f"Unknown survey_have_symptoms_now_response «{survey_have_symptoms_now_response}»")

    return survey_have_symptoms_now_map[survey_have_symptoms_now_response]


def survey_testing_because_exposed(survey_testing_because_exposed_response: Optional[Any]) -> Optional[str]:
    """
    Given a *survey_testing_because_exposed_response*, returns a standardized value.
    Raises an :class:`Exception` if the given response is unknown.

    >>> survey_testing_because_exposed("No")
    'no'

    >>> survey_testing_because_exposed("Yes - Received alert by phone app that I was near a person with COVID")
    'yes_received_app_alert'

    >>> survey_testing_because_exposed("maybe")
    Traceback (most recent call last):
        ...
    Exception: Unknown survey_testing_because_exposed value «maybe»

    """

    if isinstance(survey_testing_because_exposed_response, str):
        survey_testing_because_exposed_response = survey_testing_because_exposed_response.lower().strip()

    if survey_testing_because_exposed_response is None or survey_testing_because_exposed_response == "":
        LOG.debug("No survey_testing_because_exposed response found.")
        return None

    survey_testing_because_exposed_map = {
        "no":                                                                       "no",
        "yes - i believe i have been exposed":                                      "yes_believe_exposed",
        "yes - referred by a contact such as a friend-family-coworker":             "yes_referred_by_contact",
        "yes - received alert by phone app that i was near a person with covid":    "yes_received_app_alert",
        "yes - referred by public health":                                          "yes_referred_by_public_health",
        "yes - referred by your health care provider":                              "yes_referred_by_provider"
    }

    if survey_testing_because_exposed_response not in survey_testing_because_exposed_map:
        raise Exception(f"Unknown survey_testing_because_exposed value «{survey_testing_because_exposed_response}»")

    return survey_testing_because_exposed_map[survey_testing_because_exposed_response]


def create_provenance(record: dict) -> str:
    """
    Create JSON object indicating the source file and row of a given *record*.

    Used in FHIR Encounter resources as the meta.source, which ultimately winds
    up in ID3C's ``warehouse.encounter.details`` column.
    """
    data_scheme = 'data:application/json'

    if '_provenance' in record and set(['filename','row']).issubset(record['_provenance']):
        return data_scheme + ',' + quote(json.dumps(record['_provenance']))
    else:
        raise Exception(f"Error: _provenance missing or incomplete (must contain filename and row)")


def sample_identifier(db: DatabaseSession, barcode: str) -> Optional[str]:
    """
    Find corresponding UUID for scanned sample or collection barcode within
    warehouse.identifier.

    Will be sample barcode if from UW or PHSKC, and collection barcode if from SCH.
    """
    identifier = find_identifier(db, barcode)

    if identifier:
        assert identifier.set_name == "samples" or \
            identifier.set_name == "collections-seattleflu.org", \
            f"Identifier found in set «{identifier.set_name}», not «samples»"

    return identifier.uuid if identifier else None

def mark_skipped(db, clinical_id: int) -> None:
    LOG.debug(f"Marking clinical record {clinical_id} as skipped")
    mark_processed(db, clinical_id, { "status": "skipped" })


def mark_processed(db, clinical_id: int, entry: Mapping) -> None:
    LOG.debug(f"Marking clinical document {clinical_id} as processed")

    data = {
        "clinical_id": clinical_id,
        "log_entry": Json({
            **entry,
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.clinical
               set processing_log = processing_log || %(log_entry)s
             where clinical_id = %(clinical_id)s
            """, data)

class UnknownVaccine(ValueError):
    """
    Raised by :function: `create_immunization` if it finds a vaccine
    name that is not among a set of mapped values
    """
    pass
