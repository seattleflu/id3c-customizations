-- Verify seattleflu/id3c-customizations:roles/latest-results-viewer/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('latest-results-viewer', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('latest-results-viewer', 'shipping', 'usage')::int;
select 1/pg_catalog.has_table_privilege('latest-results-viewer', 'shipping.latest_results', 'select')::int;

select 1/(not pg_catalog.has_table_privilege('latest-results-viewer', 'shipping.latest_results', 'insert,update,delete'))::int;

rollback;
