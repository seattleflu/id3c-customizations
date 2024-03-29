import logging
from flask import jsonify, request, abort, Response, Blueprint, send_file
from flask_cors import cross_origin
from id3c.api.routes import api_v1, blueprints, api_unversioned
from id3c.api.exceptions import BadRequest
from id3c.api.utils.routes import authenticated_datastore_session_required
from pathlib import Path
from . import datastore
import os
import re

LOG = logging.getLogger(__name__)

base_dir     = Path(__file__).parent.resolve()

api_v2 = Blueprint('api_v2', 'api_v2', url_prefix='/v2')
blueprints.append(api_v2)

api_v3 = Blueprint('api_v3', 'api_v3', url_prefix='/v3')
blueprints.append(api_v3)

@api_unversioned.route("/documentation/customizations", methods = ['GET'])
def get_documentation():
    """
    Show an index page with documentation for id3c-customizations endpoints.
    """
    return send_file(base_dir / "static/documentation.html", "text/html; charset=UTF-8")

@api_v1.route("/shipping/return-results/<barcode>", methods = ['GET'])
@cross_origin(origins=[
    "https://seattleflu.org",
    "https://dev.seattleflu.org",
    "http://localhost:3000",
    "http://localhost:8080"])
def get_barcode_results_v1(barcode=None):
    """
    Mark old endpoint as 410 Gone
    """
    return "use /v2/shipping/return-results/<barcode>", 410


@api_v2.route("/shipping/return-results/<barcode>", methods = ['GET'])
@cross_origin(origins=[
    "https://seattleflu.org",
    "https://dev.seattleflu.org",
    "http://localhost:3000",
    "http://localhost:8080"])
@authenticated_datastore_session_required
def get_barcode_results_v2(barcode, session):
    """
    Export presence/absence results for a specific collection *barcode*
    """
    LOG.debug(f"Exporting presence/absence results for <{barcode}>")
    results = datastore.fetch_barcode_results(session, barcode)
    return jsonify(results)


@api_v1.route("/shipping/augur-build-metadata", methods = ['GET'])
def get_metadata_v1():
    """
    Mark old endpoint as 410 Gone
    """
    return "use /v2/shipping/augur-build-metadata instead", 410


@api_v2.route("/shipping/augur-build-metadata", methods = ['GET'])
@authenticated_datastore_session_required
def get_metadata_v2(session):
    """
    Export metadata needed for SFS augur build
    """
    LOG.debug("Exporting metadata for SFS augur build")

    metadata = datastore.fetch_rows_from_table(session, ("shipping", "metadata_for_augur_build_v2"))

    return Response((row[0] + '\n' for row in metadata), mimetype="application/x-ndjson")


@api_v3.route("/shipping/augur-build-metadata", methods = ['GET'])
@authenticated_datastore_session_required
def get_metadata_v3(session):
    """
    Export metadata needed for SFS augur build
    """
    LOG.debug("Exporting metadata for SFS augur build")

    metadata = datastore.fetch_rows_from_table(session, ("shipping", "metadata_for_augur_build_v3"))

    return Response((row[0] + '\n' for row in metadata), mimetype="application/x-ndjson")


@api_v1.route("/shipping/genomic-data/<lineage>/<segment>", methods = ['GET'])
@authenticated_datastore_session_required
def get_genomic_data(lineage, segment, session):
    """
    Export genomic data needed for SFS augur build based on provided
    *lineage* and *segment*.
    The *lineage* should be in the full lineage in ltree format
    such as 'Influenza.A.H1N1'
    """
    LOG.debug(f"Exporting genomic data for lineage <{lineage}> and segment <{segment}>")

    sequences = datastore.fetch_genomic_sequences(session, lineage, segment)

    return Response((row[0] + '\n' for row in sequences), mimetype="application/x-ndjson")


@api_v1.route("/shipping/scan-demographics", methods = ['GET'])
@authenticated_datastore_session_required
def get_scan_demographics(session):
    """
    Export basic demographics for SCAN
    """
    LOG.debug("Exporting demographics for SCAN")

    demographics = datastore.fetch_rows_from_table(session, ("shipping", "scan_demographics_v1"))

    return Response((row[0] + '\n' for row in demographics), mimetype="application/x-ndjson")


@api_v2.route("/shipping/scan-demographics", methods = ['GET'])
@authenticated_datastore_session_required
def get_scan_demographics__v2(session):
    """
    Export basic demographics for SCAN
    """
    LOG.debug("Exporting demographics with COVID status for SCAN")

    demographics = datastore.fetch_rows_from_table(session, ("shipping", "scan_demographics_v2"))

    return Response((row[0] + '\n' for row in demographics), mimetype="application/x-ndjson")


@api_v1.route("/shipping/scan-hcov19-positives", methods = ['GET'])
@authenticated_datastore_session_required
def get_scan_positives(session):
    """
    Export aggregate numbers of hCoV-19 result counts for SCAN
    """
    LOG.debug("Exporting hCoV-19 result counts for SCAN")

    positives = datastore.fetch_rows_from_table(session, ("shipping", "scan_hcov19_result_counts_v1"))

    return Response((row[0] + '\n' for row in positives), mimetype="application/x-ndjson")


@api_v2.route("/shipping/scan-hcov19-positives", methods = ['GET'])
@authenticated_datastore_session_required
def get_scan_positives__v2(session):
    """
    Export aggregate numbers of hCoV-19 result counts for SCAN, grouped by priority code
    """
    LOG.debug("Exporting hCoV-19 result counts for SCAN with priority code")

    positives = datastore.fetch_rows_from_table(session, ("shipping", "scan_hcov19_result_counts_v2"))

    return Response((row[0] + '\n' for row in positives), mimetype="application/x-ndjson")


@api_v1.route("/shipping/scan-enrollments", methods = ['GET'])
@authenticated_datastore_session_required
def get_scan_enrollments(session):
    """
    Export basic enrollment metadata for SCAN
    """
    LOG.debug("Exporting enrollment metadata for SCAN")

    enrollments = datastore.fetch_rows_from_table(session, ("shipping", "scan_enrollments_v1"))

    return Response((row[0] + '\n' for row in enrollments), mimetype="application/x-ndjson")


@api_v1.route("/shipping/scan-enrollments-internal", methods = ['GET'])
@authenticated_datastore_session_required
def get_scan_enrollments_internal(session):
    """
    Export basic enrollment metadata for SCAN
    """
    LOG.debug("Exporting enrollment metadata for SCAN internal dashboard")

    enrollments = datastore.fetch_rows_from_table(session, ("shipping", "scan_redcap_enrollments_v1"))

    return Response((row[0] + '\n' for row in enrollments), mimetype="application/x-ndjson")


@api_v1.route("/shipping/latest-results", methods = ['GET'])
@authenticated_datastore_session_required
def get_latest_results(session):
    """
    Export latest results without PHI
    """
    LOG.debug("Exporting latest results for LIMS integration")

    # restrict access to LIMS IP and localhost
    if request.remote_addr not in [os.environ['LIMS_IP'], '127.0.0.1']:
        abort(403)

    latest_results = datastore.fetch_rows_from_table(session, ("shipping", "latest_results"))

    return Response((row[0] + '\n' for row in latest_results), mimetype="application/x-ndjson")


@api_v1.route("/shipping/hct-tableau-results", methods = ['GET'])
@authenticated_datastore_session_required
def get_hct_tableau_results(session):
    """
    Export HCT results needed for tableau dashboards
    """
    LOG.debug("Exporting HCT results for Tableau dashboard backing data")

    hct_tableau_results = datastore.fetch_rows_from_table(session, ("shipping", "uw_reopening_results_hct_data_pulls"))

    return Response((row[0] + '\n' for row in hct_tableau_results), mimetype="application/x-ndjson")


@api_v1.route("/shipping/hct-tableau-encounters", methods = ['GET'])
@authenticated_datastore_session_required
def get_hct_tableau_encounters(session):
    """
    Export HCT encounters needed for tableau dashboards
    """
    LOG.debug("Exporting HCT encounters for Tableau dashboard backing data")

    hct_tableau_encounters = datastore.fetch_rows_from_table(session, ("shipping", "uw_reopening_encounters_hct_data_pulls"))

    return Response((row[0] + '\n' for row in hct_tableau_encounters), mimetype="application/x-ndjson")


@api_v1.route("/operations/deliverables-log", methods = ['GET'])
@authenticated_datastore_session_required
def get_deliverables_log(session):
    """
    Export deliverables log
    """
    LOG.debug("Exporting deliverables log for LIMS integration")

    # restrict access to LIMS IP and localhost
    if request.remote_addr not in [os.environ['LIMS_IP'], '127.0.0.1']:
        abort(403)

    sent_on = request.args.get('sent')
    process_name = request.args.get('process_name')

    date_format = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if not sent_on:
        raise BadRequest(f"Missing required argument «sent≫.")
    if not date_format.match(sent_on):
        raise BadRequest(f"Argument «sent≫ improperly formatted (expected format: YYYY-MM-DD).")
    elif not process_name:
        raise BadRequest(f"Missing required argument «process_name≫.")
    elif process_name not in ['return-of-results', 'wa-doh-linelists']:
        raise BadRequest(f"Unrecognized «process_name≫ (expected: 'return-of-results' or 'wa-doh-linelists').")

    deliverables_log = datastore.fetch_deliverables_log(session, sent_on, process_name)

    return Response((row[0] + '\n' for row in deliverables_log), mimetype="application/x-ndjson")
