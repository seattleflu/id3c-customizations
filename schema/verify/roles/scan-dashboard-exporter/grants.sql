-- Verify seattleflu/id3c-customizations:roles/scan-dashboard-exporter/grants on pg
-- requires: roles/scan-dashboard-exporter/create
-- requires: seattleflu/schema:shipping/schema

begin;

select 1/pg_catalog.has_database_privilege('scan-dashboard-exporter', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('scan-dashboard-exporter', 'shipping', 'usage')::int;

rollback;
