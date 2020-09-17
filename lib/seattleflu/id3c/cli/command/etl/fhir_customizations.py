"""
FHIR ETL functions that are custom for Seattle Flu Study.
"""
from id3c.cli.command.etl.fhir import *


def check_specimen_for_update(db: DatabaseSession,
                              sample: Any,
                              specimen: Specimen):
    """
    Check if the given *specimen* has NWGC ID or an extension for sample
    details and update the given *sample* details with these values if
    they exist.

    Returns the original or updated *sample*.
    """
    nwgc_id = identifier(specimen, f"{INTERNAL_SYSTEM}/nwgc_id").strip()
    specimen_details = matching_extension_value(
        extensions = specimen.extension,
        url = f"{INTERNAL_SYSTEM}/sample_details",
        value_type = "valueString"
    )
    # Update sample.details if the Specimen Resource contains a
    # NWGC ID or an extension for Custom
    if nwgc_id is not None or specimen_details is not None:
        sample = update_sample_details(db, sample, specimen_details, nwgc_id)

    return sample


def update_sample_details(db: DatabaseSession,
                          sample: Any,
                          specimen_details: str = None,
                          nwgc_id: str = None) -> Any:
    """
    Update the details for *sample* with additional details provided in
    *specimen*.

    The provided additional details are merged (at the top-level only) into
    the existing sample details, if any.
    """
    additional_details = json.loads(specimen_details) if specimen_details else {}
    if nwgc_id:
        update_details_nwgc_id(sample, nwgc_id, additional_details)

    LOG.info(f"Updating sample {sample.id} «{sample.identifier}» details")
    sample = db.fetch_row("""
        update warehouse.sample
           set details = coalesce(details, '{}') || %s
         where sample_id = %s
        returning sample_id as id, identifier
        """, (Json(additional_details), sample.id))

    assert sample.id, "Updating details affected no rows!"

    return sample


def update_details_nwgc_id(sample: Any, nwgc_id: str, additional_details: dict) -> None:
    """
    Given a *sample* fetched from `warehouse.sample`,
    extend `sample.details.nwgc_id` to an array if needed.

    Add provided *nwgc_id* to the existing array if it doesn't already exist
    and add to the *additional_details* dict.
    """
    if not sample.details:
        return

    existing_nwgc_ids = sample.details.get("nwgc_id", [])

    # Extend details.nwgc_id to an array
    if not isinstance(existing_nwgc_ids, list):
        existing_nwgc_ids = [existing_nwgc_ids]

    existing_nwgc_ids.append(int(nwgc_id))

    # Concatenate exisiting and new nwgc_ids and deduplicate
    additional_details["nwgc_id"] = list(set(existing_nwgc_ids))


def process_result(db: DatabaseSession,
                   sample_id: int,
                   observation: Observation,
                   report_effective_datetime: str = None) -> None:
    """
    Given an  *observation* containing a presence-absence test result,
    upserts them to ID3C, attaching a sample and target ID.

    Includes handling of custom extensions that are expected to be in the
    *observation* resource.
    """
    target_identifier = matching_system_code(observation.code, f"{INTERNAL_SYSTEM}/target/identifier")
    target_control_status = matching_system_code(observation.code, f"{INTERNAL_SYSTEM}/target/control")

    assert target_identifier, \
        f"No {INTERNAL_SYSTEM}/target/identifier code found for observation"

    assert target_control_status, \
        f"No {INTERNAL_SYSTEM}/target/control code found for observation"

    target = find_or_create_target(db,
        identifier = target_identifier,
        control = target_control_status
    )

    result_identifier = identifier(observation, f"{INTERNAL_SYSTEM}/presence-absence")

    assert result_identifier, \
        f"No {INTERNAL_SYSTEM}/presence-absence identifier found for observation"

    result_value = observation_value(observation)

    details = { "device": observation.device.identifier.value }

    if report_effective_datetime:
        details["assay_date"] = report_effective_datetime.as_json()

    expected_extensions = [
        ("assay_type", "valueString"),
        ("replicates", "valueString"),
        ("extraction_date", "valueDate")
    ]

    for extension, value_type in expected_extensions:
        extension_value = matching_extension_value(
            extensions = observation.extension,
            url = f"{INTERNAL_SYSTEM}/{extension}",
            value_type = value_type
        )

        if extension_value:
            try:
                details[extension] = extension_value.as_json()
            except AttributeError:
                try:
                    details[extension] = json.loads(extension_value)
                except json.decoder.JSONDecodeError:
                    details[extension] = extension_value


    upsert_presence_absence(db,
        identifier = result_identifier,
        sample_id = sample_id,
        target_id = target.id,
        present = result_value,
        details = details
    )
