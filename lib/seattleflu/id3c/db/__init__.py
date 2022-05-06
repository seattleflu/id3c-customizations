"""
Database interfaces
"""
import logging
from datetime import datetime
from typing import Optional
from id3c.db.session import DatabaseSession
from id3c.db.datatypes import Json

LOG = logging.getLogger(__name__)


def log_deliverable(db: DatabaseSession,
                    process_name:str,
                    details:dict,
                    sent:Optional[str]=None,
                    sample_barcode:Optional[str]=None,
                    collection_barcode:Optional[str]=None):
    LOG.debug(f"Logging deliverable")

    if sample_barcode or collection_barcode:
        deliverable_log = db.fetch_row("""
            insert into operations.deliverables_log (
                sample_barcode,
                collection_barcode,
                details,
                process_name,
                sent) values (%s, %s, %s, %s, coalesce(%s, now()))
            returning deliverables_log as id, sample_barcode, collection_barcode, details, process_name, sent
            """, (sample_barcode,collection_barcode, Json(details), process_name, sent))

        LOG.debug(f"Deliverable log added for {sample_barcode or collection_barcode} ")
    else:
        LOG.warning(f"Deliverable logging skipped. Deliverable log requires sample or collection barcode value.")
