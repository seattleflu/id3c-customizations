from typing import Any, Iterable, Tuple
from psycopg2.sql import SQL, Identifier
from id3c.db.session import DatabaseSession
from id3c.api.datastore import catch_permission_denied
from id3c.api.utils import export

@export
@catch_permission_denied
def fetch_rows_from_table(session: DatabaseSession,
                          qualified_table: Tuple) -> Iterable[Tuple[str]]:
    """
    Exports all rows in a given *qualified_table* and yields them as JSON (one
    per line) in a generative fashion.

    *qualified_table* should be a tuple of (schema, table). All identifiers will
    be properly quoted by this method.
    """
    assert len(qualified_table) == 2, \
        "A schema and table name must be included in the qualified table tuple"

    table = SQL(".").join(map(Identifier, qualified_table))

    with session, session.cursor() as cursor:
        cursor.execute(SQL("""
            select row_to_json(r)::text
            from {} as r
            """).format(table))

        yield from cursor


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
        from shipping.return_results_v2
        where barcode = %s
    """, (barcode,))

    if not barcode_result:
        results = { "status": "unknownBarcode" }
    else:
        results = barcode_result._asdict()

    return results


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
