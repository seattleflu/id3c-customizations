-- Verify seattleflu/id3c-customizations:roles/ehs-results-exporter/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('ehs-results-exporter', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('ehs-results-exporter', 'shipping', 'usage')::int;


rollback;
