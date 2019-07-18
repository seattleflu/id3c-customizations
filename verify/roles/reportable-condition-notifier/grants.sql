-- Verify seattleflu/id3c-customizations:roles/reportable-condition-notifier/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('reportable-condition-notifier', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('reportable-condition-notifier', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('reportable-condition-notifier', 'shipping', 'usage')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-notifier', 'warehouse.site', 'select')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-notifier', 'shipping.reportable_condition', 'select')::int;
select 1/pg_catalog.has_column_privilege('reportable-condition-notifier', 'shipping.reportable_condition', 'processing_log', 'update')::int;

select 1/(not pg_catalog.has_table_privilege('reportable-condition-notifier', 'warehouse.site', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('reportable-condition-notifier', 'shipping.reportable_condition', 'insert,delete'))::int;
select 1/(not pg_catalog.has_column_privilege('reportable-condition-notifier', 'shipping.reportable_condition', 'reportable_condition_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('reportable-condition-notifier', 'shipping.reportable_condition', 'identifier', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('reportable-condition-notifier', 'shipping.reportable_condition', 'site', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('reportable-condition-notifier', 'shipping.reportable_condition', 'lineage', 'update'))::int;

rollback;
