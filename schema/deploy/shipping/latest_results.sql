-- Deploy seattleflu/schema:shipping/age-bin-decade to pg
-- requires: shipping/schema

begin;

create table shipping.latest_results (
    qrcode text,
    collect_ts text,
    result_ts text,
    status_code text,
    swab_type text,
    staff_observed text,
    pre_analytical_specimen_collection text,
    created timestamp with time zone not null default now()
);

comment on table shipping.latest_results is
    'Latest return of results with PHI data removed';

comment on column shipping.latest_results.qrcode is
    'Barcode corresponding to result';
comment on column shipping.latest_results.collect_ts is
    'Collection timestamp for result';
comment on column shipping.latest_results.result_ts is
    'Test result timestamp';
comment on column shipping.latest_results.status_code is
    'Test result status code (positive, negative, inconclusive, pending, never-tested)';
comment on column shipping.latest_results.swab_type is
    'Swab type of sample (ans, mtb, tiny)';
comment on column shipping.latest_results.staff_observed is
    'Value indicating if sample was staff-observed';
comment on column shipping.latest_results.pre_analytical_specimen_collection is
    'Pre-analytical specimen collection context (IRB or clinical)';    
comment on column shipping.latest_results.created is
    'Timestamp of record insert';

commit;
