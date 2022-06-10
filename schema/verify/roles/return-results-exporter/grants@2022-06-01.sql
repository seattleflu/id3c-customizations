-- Verify seattleflu/id3c-customizations:roles/return-results-exporter/grants on pg
-- requires: shipping/views

begin;

select 1/pg_catalog.has_database_privilege('return-results-exporter', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('return-results-exporter', 'shipping', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('return-results-exporter', 'operations', 'usage')::int;
select 1/pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v1', 'select')::int;
select 1/pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v2', 'select')::int;
select 1/pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v3', 'select')::int;
select 1/pg_catalog.has_table_privilege('return-results-exporter', 'shipping.sample_with_best_available_encounter_data_v1', 'select')::int;
select 1/pg_catalog.has_table_privilege('return-results-exporter', 'shipping.linelist_data_for_wa_doh_v1', 'select')::int;
select 1/pg_catalog.has_table_privilege('return-results-exporter', 'shipping.latest_results', 'insert,delete')::int;
select 1/(pg_catalog.has_table_privilege('return-results-exporter', 'operations.deliverables_log', 'select,insert'))::int;

select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v1', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v2', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'shipping.return_results_v3', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'shipping.sample_with_best_available_encounter_data_v1', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'shipping.linelist_data_for_wa_doh_v1', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'shipping.latest_results', 'select,update'))::int;
select 1/(not pg_catalog.has_table_privilege('return-results-exporter', 'operations.deliverables_log', 'update,delete'))::int;

rollback;
