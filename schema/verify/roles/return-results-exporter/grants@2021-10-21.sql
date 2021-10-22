-- Verify seattleflu/id3c-customizations:roles/return-results-exporter/grants on pg
-- requires: shipping/views

begin;

select 1/pg_catalog.has_database_privilege('return-results-exporter', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('return-results-exporter', 'shipping', 'usage')::int;
select 1/pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v1', 'select')::int;
select 1/pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v2', 'select')::int;
select 1/pg_catalog.has_table_privilege('return-results-exporter', 'shipping.latest_results', 'insert,delete')::int;

select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v1', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v2', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'shipping.latest_results', 'select,update'))::int;

rollback;
