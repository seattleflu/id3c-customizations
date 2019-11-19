import logging
from flask import jsonify, Response
from flask_cors import cross_origin
from id3c.api.routes import api_v1
from id3c.api.utils.routes import authenticated_datastore_session_required
from . import datastore

LOG = logging.getLogger(__name__)

@api_v1.route("/shipping/return-results/<barcode>", methods = ['GET'])
@cross_origin(origins=["https://seattleflu.org","https://dev.seattleflu.org","http://localhost:8080"])
@authenticated_datastore_session_required
def get_barcode_results(barcode, session):
    """
    Export presence/absence results for a specific collection *barcode*
    """
    LOG.debug(f"Exporting presence/absence results for <{barcode}>")
    results = datastore.fetch_barcode_results(session, barcode)
    return jsonify(results)


@api_v1.route("/shipping/augur-build-metadata", methods = ['GET'])
@authenticated_datastore_session_required
def get_metadata(session):
    """
    Export metadata needed for SFS augur build
    """
    LOG.debug("Exporting metadata for SFS augur build")

    metadata = datastore.fetch_metadata_for_augur_build(session)

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
