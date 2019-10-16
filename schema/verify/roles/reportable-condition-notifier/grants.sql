-- Verify seattleflu/id3c-customizations:roles/reportable-condition-notifier/grants on pg
-- requires: roles/reportable-condition-notifier/create
-- requires: shipping/views

begin;

select 1/pg_catalog.has_database_privilege('reportable-condition-notifier', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('reportable-condition-notifier', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('reportable-condition-notifier', 'shipping', 'usage')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-notifier', 'warehouse.site', 'select')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-notifier', 'warehouse.presence_absence', 'select')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-notifier', 'shipping.reportable_condition_v1', 'select')::int;
select 1/pg_catalog.has_column_privilege('reportable-condition-notifier', 'warehouse.presence_absence', 'details', 'update')::int;

select 1/(not pg_catalog.has_table_privilege('reportable-condition-notifier', 'warehouse.site', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('reportable-condition-notifier', 'warehouse.presence_absence', 'insert,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('reportable-condition-notifier', 'shipping.reportable_condition_v1', 'insert,delete'))::int;
select 1/(not pg_catalog.has_column_privilege('reportable-condition-notifier', 'warehouse.presence_absence', 'presence_absence_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('reportable-condition-notifier', 'warehouse.presence_absence', 'identifier', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('reportable-condition-notifier', 'warehouse.presence_absence', 'sample_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('reportable-condition-notifier', 'warehouse.presence_absence', 'target_id', 'update'))::int;
select 1/(not pg_catalog.has_column_privilege('reportable-condition-notifier', 'warehouse.presence_absence', 'present', 'update'))::int;

rollback;
