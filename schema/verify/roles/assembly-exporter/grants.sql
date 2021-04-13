-- Verify seattleflu/id3c-customizations:roles/assembly-exporter/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('assembly-exporter', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('assembly-exporter', 'shipping', 'usage')::int;

rollback;
