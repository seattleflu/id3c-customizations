from typing import Any, Iterable, Tuple
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


@export
@catch_permission_denied
def fetch_metadata_for_augur_build(session: DatabaseSession) -> Iterable[Tuple[str]]:
    """
    Export metadata for augur build from shipping view
    """
    with session, session.cursor() as cursor:
        cursor.execute("""
            select row_to_json(r)::text
            from shipping.metadata_for_augur_build_v3 as r
            """)

        yield from cursor


@export
@catch_permission_denied
def fetch_genomic_sequences(session: DatabaseSession,
                        lineage: str,
                        segment: str) -> Iterable[Tuple[str]]:
    """
    Export sample identifier and sequence from shipping view based on the
    provided *lineage* and *segment*
    """
    with session, session.cursor() as cursor:
        cursor.execute("""
            select row_to_json(r)::text
            from (select sample, seq
                    from shipping.genomic_sequences_for_augur_build_v1
                   where organism = %s and segment = %s) as r
            """,(lineage, segment))

        yield from cursor
