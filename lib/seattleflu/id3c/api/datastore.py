from typing import Any
from id3c.db.session import DatabaseSession
from id3c.api.datastore import catch_permission_denied
from id3c.api.utils import export

@export
@catch_permission_denied
def fetch_barcode_results(session: DatabaseSession,
                          barcode: str) -> Any:
    """
    Export presence/absence results from shipping view for a specific
    *barcode*
    """
    barcode_result = session.fetch_row("""
        select barcode, status, organisms_present
        from shipping.return_results_v1
        where barcode = %s
    """, (barcode,))

    if not barcode_result:
        results = { "status": "unknownBarcode" }
    else:
        results = barcode_result._asdict()

    return results
