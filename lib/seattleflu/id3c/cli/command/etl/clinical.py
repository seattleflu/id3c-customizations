"""
Process clinical documents into the relational warehouse.
"""
import click
import logging
import re
from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from id3c.cli.command import with_database_session
from id3c.db import find_identifier
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json
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
from . import race


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

    for k in ["CovidShot1", "CovidShot2"]:
        if k in document:
            details["responses"][k] = covid_shot(document[k])

    if "CovidShotManufacturer" in document:
        details["responses"]["CovidShotManufacturer"] = covid_shot_maunufacturer(document.get("CovidShotManufacturer"))

    # include vaccine date fields if present and not empty
    for k in ["FluShotDate", "CovidShot1Date", "CovidShot2Date"]:
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


def sample_identifier(db: DatabaseSession, barcode: str) -> Optional[str]:
    """
    Find corresponding UUID for scanned sample or collection barcode within
    warehouse.identifier.

    Will be sample barcode if from UW and collection barcode if from SCH.
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
