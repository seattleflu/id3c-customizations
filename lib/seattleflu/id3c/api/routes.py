import logging
from flask import jsonify
from flask_cors import cross_origin
from id3c.api.routes import api_v1
from id3c.api.utils.routes import authenticated_datastore_session_required
from . import datastore

LOG = logging.getLogger(__name__)

@api_v1.route("/shipping/return-results/<barcode>", methods = ['GET'])
@cross_origin(origins=["http://localhost:8080"])
@authenticated_datastore_session_required
def get_barcode_results(barcode, session):
    """
    Export presence/absence results for a specific collection *barcode*
    """
    LOG.debug(f"Exporting presence/absence results for <{barcode}>")
    results = datastore.fetch_barcode_results(session, barcode)
    return jsonify(results)
