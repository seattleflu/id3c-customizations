"""
Process presence-absence tests that are specific to the custom JSON dump
from Samplify to create standardized FHIR bundles that are inserted into
the receiving FHIR table.

Each FHIR bundle will contain results for one sample so that if errors arise,
results for each sample can get processed separately without blocking ingestion
of results of other samples.

The presence-absence ETL process will abort under these conditions:

1. If we receive an unexpected value for the top-level key

2. If we receive a bogus "chip" value

3. If we receive an unexpected value for the "controlStatus" of a target

4. If we receive an unexpected value for the "targetResult" of a specific test

5. If we receive an unexpected value for the "assayName"

6. If we receive an unexpected value for the "assayType"
"""
import click
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from id3c.cli.command import with_database_session
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json, as_json
from id3c.cli.command.etl import etl
from id3c.cli.command.etl.redcap_det import insert_fhir_bundle
from .fhir import *


LOG = logging.getLogger(__name__)


# This revision number is stored in the processing_log of each presence-absence
# record when the presence-absence test is successfully processed by this ETL
# routine. The routine finds new-to-it records to process by looking for
# presence-absence tests lacking this revision number in their log.  If a
# change to the ETL routine necessitates re-processing all presence-absence tests,
# this revision number should be incremented.
REVISION = 8

INTERNAL_SYSTEM = "https://seattleflu.org"
SNOMED_SYSTEM = 'http://snomed.info/sct'

@etl.command("presence-absence", help = __doc__)
@with_database_session

def etl_presence_absence(*, db: DatabaseSession):
    LOG.debug(f"Starting the presence_absence ETL routine, revision {REVISION}")

    # Fetch and iterate over presence-absence tests that aren't processed
    #
    # Rows we fetch are locked for update so that two instances of this
    # command don't try to process the same presence-absence tests.
    LOG.debug("Fetching unprocessed presence-absence tests")

    presence_absence = db.cursor("presence_absence")
    presence_absence.itersize = 1
    presence_absence.execute("""
        select presence_absence_id as id, document
          from receiving.presence_absence
         where not processing_log @> %s
         order by id
           for update
        """, (Json([{ "revision": REVISION }]),))

    for group in presence_absence:
        with db.savepoint(f"presence_absence group {group.id}"):
            LOG.info(f"Processing presence_absence group {group.id}")

            # Samplify will now send documents with a top level key
            # "samples". The new format also includes a "chip" key for each
            # sample which is then included in the unique identifier for
            # each presence/absence result
            #   -Jover, 14 Nov 2019
            try:
                received_samples = group.document["samples"]
            except KeyError as error:
                # Skip documents in the old format because they do not
                # include the "chip" key which is needed for the
                # unique identifier for each result.
                #   -Jover, 14 Nov 2019
                # Also skip old format to avoid ingesting wrong data from
                # plate swapped data! This will lead to 188 samples with the
                # wrong nwgc_id associated with them.
                #   -Jover, 06 Dec 2019
                if (group.document.get("store") is not None or
                    group.document.get("Update") is not None):

                    LOG.info("Skipping presence_absence record that is in old format")
                    mark_processed(db, group.id)
                    continue

                else:
                    raise error from None

            for received_sample in received_samples:

                received_sample_barcode = received_sample.get("investigatorId")
                received_sample_id = received_sample["sampleId"]

                if not received_sample_barcode:
                    LOG.warning(f"Skipping sample «{received_sample_id}» without investigatorId (SFS barcode)")
                    continue

                # Don't go process if sample doesn't have results.
                test_results = received_sample["targetResults"]
                if not test_results:
                    LOG.warning(f"Skipping sample «{received_sample_barcode}» without any results")
                    continue

                # Must be current results
                if not received_sample.get("isCurrentExpressionResult"):
                    LOG.warning(f"Skipping out-of-date results for sample «{received_sample_barcode}»")
                    continue

                # Guard against empty chip values
                chip = received_sample.get("chip")
                assert chip or "chip" not in received_sample, "Received bogus chip id"

                LOG.info(f"Processing sample «{received_sample_barcode}»")
                specimen_entry, specimen_reference = create_specimen(received_sample)

                results: List[dict] = []
                results_references: List[dict] = []
                # Process all results for this sample.
                for index, test_result in enumerate(test_results):
                    result_id = f"result-{index+1}"

                    result_observation_resource = create_result_observation(
                        received_sample = received_sample,
                        test_result = test_result,
                        result_id = result_id
                    )

                    if not result_observation_resource:
                        continue

                    results.append(result_observation_resource)
                    results_references.append({"reference": f"#{result_id}"})

                # Skip sample if there are no valid results
                if not results:
                    LOG.debug(f"Sample «{received_sample_barcode} did not have any valid results")
                    continue

                # Create FHIR Diagnostic Report Resource and Bundle entry
                diagnostic_report_resource = create_diagnostic_report_resource(
                    diagnostic_code = create_codeable_concept(
                        system = f"{INTERNAL_SYSTEM}/presence-absence-panel",
                        code = 'NWGC'
                    ),
                    specimen_reference = specimen_reference,
                    result = results_references,
                    contained = results,
                    datetime = received_sample.get("assayDate")
                )

                diagnostic_report_entry = create_resource_entry(
                    resource = diagnostic_report_resource,
                    full_url = generate_full_url_uuid()
                )

                # Create FHIR Bundle
                fhir_bundle = create_bundle_resource(
                    bundle_id = str(uuid4()),
                    timestamp = datetime.now().astimezone().isoformat(),
                    source = f"receiving/presence_absence/{group.id}",
                    entries = [specimen_entry, diagnostic_report_entry]
                )

                # Insert FHIR Bundle into receiving.fhir
                insert_fhir_bundle(db, fhir_bundle)

            mark_processed(db, group.id)

            LOG.info(f"Finished processing presence_absence group {group.id}")


def create_specimen(received_sample: dict) -> tuple:
    """ Returns a FHIR Specimen resource entry and reference """
    # SFS sample barcode
    sfs_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/sample",
        value = received_sample.get("investigatorId")
    )

    # NWGC ID
    nwgc_identifier = create_identifier(
        system = f"{INTERNAL_SYSTEM}/nwgc_id",
        value = str(received_sample["sampleId"])
    )

    # Capture details about the go/no-go sequencing call for this sample.
    sample_details = as_json({
        "sequencing_call": {
            "comment": received_sample["sampleComment"],
            "initial": received_sample["initialProceedToSequencingCall"],
            "final": received_sample["sampleProceedToSequencing"],
        },
    })
    specimen_extension = create_extension_element(
        url = f"{INTERNAL_SYSTEM}/sample_details",
        value = { "valueString" : sample_details }
    )

    specimen_resource = create_specimen_resource(
        specimen_identifier = [sfs_identifier, nwgc_identifier],
        extension = [specimen_extension]
    )

    return create_entry_and_reference(specimen_resource, "Specimen")


def create_result_observation(received_sample: dict,
                              test_result: dict,
                              result_id: str) -> Optional[Dict[str, Any]]:
    """
    Returns a FHIR Observation Resource for the given *test_result* of the
    *received_sample*.
    """
    test_result_target_id = test_result["geneTarget"]
    received_sample_barcode = received_sample["investigatorId"]
    received_sample_id = received_sample["sampleId"]

    LOG.debug(f"Processing target «{test_result_target_id}» for sample «{received_sample_barcode}»")

    # Skip this result if it's actually a non-result
    present = target_present(test_result)
    if present is None:
        LOG.debug(f"No test result for «{test_result_target_id}», skipping")
        return None

    # The unique identifier for each result.  If chip is
    # applicable, then it's included to differentiate the same
    # sample being run on multiple chips (uncommon, but it
    # happens).
    chip = received_sample.get("chip")
    if chip:
        identifier = f"NWGC/{received_sample_id}/{test_result_target_id}/{chip}"
    else:
        identifier = f"NWGC/{received_sample_id}/{test_result_target_id}"

    result_observation = {
        "resourceType": "Observation",
        "id": result_id,
        "status": "final",
        'code': create_result_code(test_result),
        "valueCodeableConcept": create_codeable_concept(
            system = SNOMED_SYSTEM,
            code = present
        ),
        "identifier": [create_identifier(
            system = f"{INTERNAL_SYSTEM}/presence-absence",
            value = identifier
        )],
        "extension": create_observation_extensions(received_sample, test_result)
    }

    device_reference = create_device_reference(received_sample)

    if device_reference:
        result_observation["device"] = device_reference

    return result_observation


def target_present(test_result: dict) -> Optional[str]:
    """
    Returns a SNOMED code for the given received *test_result*, or None if the
    test should be skipped.

    SNOMED codes for qualifier values are:
    "10828004": Positive
    "260385009": Negative
    "82334004": Indeterminate

    Raises a :py:class:`ValueError` if a value cannot be determined.
    """
    status = (
           test_result.get("targetStatus")
        or test_result.get("sampleState")
    )

    mapping = {
        "Detected": "10828004",
        "NotDetected": "260385009",

        "Positive": "10828004",
        "PositiveControlPass": "10828004",
        "Negative": "260385009",
        "Indeterminate": "82334004",
        "Inconclusive": "82334004",

        # These are valid _workflow_ statuses, but they're not really test
        # results; they describe the circumstances around performing the test,
        # not the result of the test itself.  We skip ingesting them for now as
        # there is no place for them in our current data model.
        #
        # I did consider making these map to None/null like Indeterminate.
        # That would make "present is null" results in the database mean "this
        # test was run, but the result is unknown due to circumstances left
        # unspecified".  I ultimately decided against it as the goal with ID3C
        # is to aim for simpler data models which are easier to reckon about,
        # not track everything that's performed like a LIMS/LIS does.
        #   -trs, 20 Mar 2020
        "Fail": None,
        "Repeat": None,
        "Review": None,

        # Control samples get a specific `sampleState` of 'ControlPass`
        # since we don't ingest results for control samples, these results
        # can be skipped.
        #   -Jover, 14 September 2020
        "ControlPass": None,
    }

    if not status or status not in mapping:
        raise ValueError(f"Unable to determine target presence given «{test_result}»")

    return mapping[status]


def create_result_code(test_result: dict) -> Dict[str, Any]:
    """
    Returns a FHIR codeable concept that with the codes for the provided
    *test_result*.

    The codes include the target identifier and control status of the target.
    """
    identifier_coding = create_coding(
        system = f"{INTERNAL_SYSTEM}/target/identifier",
        code = test_result["geneTarget"]
    )

    control_coding = create_coding(
        system = f"{INTERNAL_SYSTEM}/target/control",
        code = str(target_control(test_result["controlStatus"]))
    )

    return {
        "coding": [identifier_coding, control_coding]
    }


def target_control(control: str) -> bool:
    """
    Determine the control status of the target.
    """
    expected_values = ["NotControl", "PositiveControl"]
    if not control or control not in expected_values:
        raise UnknownControlStatusError(f"Unknown control status «{control}».")
    return control == "PositiveControl"


def create_device_reference(received_sample: dict) -> Optional[Dict[str, Any]]:
    """
    Returns a FHIR Reference to the device used to generate results for
    the *received_sample*.

    Device is determined based on the `assay_name` or `chip`
    in *received_sample*. If neither exists, then returns None.
    """
    assay_name = received_sample.get("assayName")
    chip = received_sample.get("chip")

    if assay_name:
        assert assay_name in {"OpenArray", "TaqmanQPCR"}, f"Found unknown assay name «{assay_name}»"
        device = assay_name
    elif chip:
        device = "OpenArray"
    else:
        return None

    return create_reference(
        reference_type = 'Device',
        identifier = create_identifier(
            system = f'{SFS}/device',
            value = device
        )
    )


def create_observation_extensions(received_sample: dict,
                                  test_result: dict) -> List[Dict[str, Any]]:
    """
    Create a list of FHIR extension elements related to the *received_sample*
    and *test_result* that do not fit in the standard fields of the
    FHIR Observation Resource.
    """
    replicates = test_result["wellResults"]
    assay_type = received_sample.get("assayType")

    if assay_type:
        assert assay_type in {'Clia', 'Research'}, f"Found unknown assay type «{assay_type}»"
    else:
        # Assumes anything with 4 wellResults is "Clia" and everything else
        # "Research" assays
        assay_type = 'Clia' if len(replicates) == 4 else 'Research'

    assay_type_extension = create_extension_element(
        url = f"{INTERNAL_SYSTEM}/assay_type",
        value = {"valueString": assay_type}
    )

    replicates_extension = create_extension_element(
        url = f"{INTERNAL_SYSTEM}/replicates",
        value = {"valueString": as_json(replicates)}
    )

    extensions = [assay_type_extension, replicates_extension]

    extraction_date = received_sample.get("extractionDate")
    if extraction_date:
        extraction_date_extension = create_extension_element(
            url = f"{INTERNAL_SYSTEM}/extraction_date",
            value = {"valueDate": extraction_date}
        )
        extensions.append(extraction_date_extension)

    return extensions


def mark_processed(db, group_id: int) -> None:
    LOG.debug(f"Marking presence_absence group {group_id} as processed")

    data = {
        "group_id": group_id,
        "log_entry": Json({
            "revision": REVISION,
            "timestamp": datetime.now(timezone.utc),
        }),
    }

    with db.cursor() as cursor:
        cursor.execute("""
            update receiving.presence_absence
               set processing_log = processing_log || %(log_entry)s
             where presence_absence_id = %(group_id)s
            """, data)


class UnknownControlStatusError(ValueError):
    """
    Raised by :function:`target_control` if its provided *control*
    is not among the set of expected values.
    """
    pass
