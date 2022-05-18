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


@export
@catch_permission_denied
def fetch_deliverables_log(session: DatabaseSession,
                        sent_on: str,
                        process_name: str) -> Iterable[Tuple[str]]:
    """
    Export entries from operations.deliverables_log based on the
    provided *sent_on* date and *process_name*, with associated sample
    and collection barcodes populated.
    """

    with session, session.cursor() as cursor:
        cursor.execute("""
            select row_to_json(r)::text
            from (select deliverables_log_id,
                        lower(coalesce(sample_barcode, right(sample.identifier,8))) as sample_barcode,
                        lower(coalesce(collection_barcode, right(sample.collection_identifier,8))) as collection_barcode,

                        -- only return details that may be useful for QC
                        (select jsonb_object_agg(key, value) FROM jsonb_each(deliverables_log.details)
                            where key in (
                                          -- wa doh linelist details
                                          '_provenance',
                                          'record_id',
                                          'study_arm',
                                          'date_tested',
                                          'test_result',
                                          'collection_date',
                                          'redcap_event_name',

                                          -- return of results details
                                          'result_ts',
                                          'swab_type',
                                          'collect_ts',
                                          'status_code',
                                          'staff_observed',
                                          'pre_analytical_specimen_collection')
                        ) as details

                    from operations.deliverables_log
                        left join warehouse.identifier samp_identifier on samp_identifier.barcode = sample_barcode
                        left join warehouse.sample on samp_identifier.uuid::text = sample.identifier
                    where sent::date = %s and process_name = %s) as r
            """,(sent_on, process_name))

        yield from cursor
