-- Verify seattleflu/id3c-customizations:shipping/views on pg
-- requires: seattleflu/schema:shipping/schema

begin;

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.reportable_condition_v1');

rollback;
