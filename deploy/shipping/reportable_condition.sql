-- Deploy seattleflu/id3c-customizations:shipping/reportable_condition to pg
-- requires: warehouse/target/data
-- requires: seattleflu/schema:warehouse/presence_absence
-- requires: seattleflu/schema:warehouse/organism
-- requires: seattleflu/schema:warehouse/sample

begin;

set local search_path to shipping;

create table reportable_condition (
    reportable_condition_id integer primary key generated by default as identity,
    identifier text references warehouse.sample (identifier) not null,
    site text references warehouse.site (identifier),
    lineage public.ltree references warehouse.organism (lineage) not null,
    processing_log jsonb not null default '[]'
        constraint reportable_condition_processing_log_is_array
            check (jsonb_typeof(processing_log) = 'array')
);

comment on table reportable_condition is
    'Append-only set of reportable_condition documents';

comment on column reportable_condition.reportable_condition_id is
    'Internal id of this record';

comment on column reportable_condition.processing_log is
    'Event log recording details of reportable conditions into the "view"';

commit;
