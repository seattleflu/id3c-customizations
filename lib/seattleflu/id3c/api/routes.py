import logging
from flask import jsonify, Response, Blueprint
from flask_cors import cross_origin
from id3c.api.routes import api_v1, blueprints
from id3c.api.utils.routes import authenticated_datastore_session_required
from . import datastore

LOG = logging.getLogger(__name__)

api_v2 = Blueprint('api_v2', 'api_v2', url_prefix='/v2')
blueprints.append(api_v2)

api_v3 = Blueprint('api_v3', 'api_v3', url_prefix='/v3')
blueprints.append(api_v3)

@api_v1.route("/shipping/return-results/<barcode>", methods = ['GET'])
@cross_origin(origins=[
    "https://seattleflu.org",
    "https://dev.seattleflu.org",
    "http://localhost:8080"])
@authenticated_datastore_session_required
def get_barcode_results(barcode, session):
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
