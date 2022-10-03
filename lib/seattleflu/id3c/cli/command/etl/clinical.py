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


            # PHSKC will be handled differently that other clinical records, converted
            # to FHIR format and inserted into receiving.fhir table to be processed
            # by the FHIR ETL. When time allows, SCH and KP should follow suit.
            if site.identifier == 'RetrospectivePHSKC':
                fhir_bundle = generate_fhir_bundle(db, record.document)
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

    if record["age"]:
        items["age"] = [{ 'valueInteger': (int(record["age"]))}]

    if record["race"]:
        items["race"] = []
        for code in race(record["race"]):
            items["race"].append({ 'valueCoding': create_coding(f"{SFS}/race", code)})

    if record["ethnicity"]:
        items["ethnicity"] = [{ 'valueBoolean': ethnicity(record["ethnicity"]) }]

    if record["if_symptoms_how_long"]:
        items["if_symptoms_how_long"] = [{ 'valueString': if_symptoms_how_long(record["if_symptoms_how_long"])}]

    if record["vaccine_status"]:
        items["vaccine_status"] = [{ 'valueString': covid_vaccination_status(record["vaccine_status"])}]

    if record["inferred_symptomatic"]:
        items["inferred_symptomatic"] = [{ 'valueBoolean': inferred_symptomatic(record["inferred_symptomatic"])}]

    if record["survey_have_symptoms_now"]:
        items["survey_have_symptoms_now"] = [{ 'valueBoolean': survey_have_symptoms_now(record["survey_have_symptoms_now"])}]

    if record["survey_testing_because_exposed"]:
        items["survey_testing_because_exposed"] = [{ 'valueString': survey_testing_because_exposed(record["survey_testing_because_exposed"])}]


    questionnaire_items: List[dict] = []
    for key,value in items.items():
        questionnaire_items.append(create_questionnaire_response_item(
            question_id = key,
            answers = value
        ))

    return questionnaire_items


def generate_fhir_bundle(db: DatabaseSession, record: dict) -> Optional[dict]:

    patient_entry, patient_reference = create_patient(record)

    if not patient_entry:
        LOG.info("Skipping clinical data pull with insufficient information to construct patient")
        return None

    specimen_entry, specimen_reference = create_specimen(record, patient_reference)
    location_entries, location_references = create_resident_locations(record)
    encounter_entry, encounter_reference = create_encounter(db, record, patient_reference, location_references)

    if not encounter_entry:
        LOG.info("Skipping clinical data pull with insufficient information to construct encounter")
        return None

    questionnaire_response_entry = create_questionnaire_response(record, patient_reference, encounter_reference, determine_questionnaire_items)

    specimen_observation_entry = create_specimen_observation_entry(specimen_reference, patient_reference, encounter_reference)

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

    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{record['_provenance']['filename']},row:{record['_provenance']['row']}" ,
        entries = list(filter(None, resource_entries))
    )

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
    if flu_shot_response is None:
        LOG.debug("No flu shot response found.")
        return [None]

    if isinstance(flu_shot_response, str):
        flu_shot_response = flu_shot_response.lower()

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
    if admit_encounter_response is None:
        LOG.debug("No admit during this encounter response found.")
        return [None]

    if isinstance(admit_encounter_response, str):
        admit_encounter_response = admit_encounter_response.lower()

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
    if admit_icu_response is None:
        LOG.debug("No admit to icu response found.")
        return [None]

    if isinstance(admit_icu_response, str):
        admit_icu_response = admit_icu_response.lower()

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
    if covid_shot_response is None:
        LOG.debug("No COVID shot response found.")
        return [None]

    if isinstance(covid_shot_response, str):
        covid_shot_response = covid_shot_response.lower()

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
    if covid_shot_manufacturer_name is None:
        LOG.debug("No COVID shot manufacturer name found.")
        return [None]

    if isinstance(covid_shot_manufacturer_name, str):
        covid_shot_manufacturer_name = covid_shot_manufacturer_name.lower().strip()

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
    if is_covid_screen is None:
        LOG.debug("No COVID screen value found.")
        return [None]

    if isinstance(is_covid_screen, str):
        is_covid_screen = is_covid_screen.lower()

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

    if if_symptoms_how_long_response is None:
        LOG.debug("No if_symptoms_how_long response found.")
        return None

    if isinstance(if_symptoms_how_long_response, str):
        if_symptoms_how_long_response = if_symptoms_how_long_response.lower().strip()

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

    if covid_vaccination_status_response is None:
        LOG.debug("No covid_vaccination_status_response response found.")
        return None

    if isinstance(covid_vaccination_status_response, str):
        covid_vaccination_status_response = covid_vaccination_status_response.lower().strip()

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
    if inferred_symptomatic_response is None:
        LOG.debug("No inferred_symptomatic response found.")
        return None

    if isinstance(inferred_symptomatic_response, str):
        inferred_symptomatic_response = inferred_symptomatic_response.lower().strip()

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
    if survey_have_symptoms_now_response is None:
        LOG.debug("No survey_have_symptoms_now response found.")
        return None

    if isinstance(survey_have_symptoms_now_response, str):
        survey_have_symptoms_now_response = survey_have_symptoms_now_response.lower().strip()

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

    if survey_testing_because_exposed_response is None:
        LOG.debug("No survey_testing_because_exposed response found.")
        return None

    if isinstance(survey_testing_because_exposed_response, str):
        survey_testing_because_exposed_response = survey_testing_because_exposed_response.lower().strip()

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
