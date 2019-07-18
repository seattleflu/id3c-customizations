-- Verify seattleflu/id3c-customizations:roles/reportable-condition-processor/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('reportable-condition-processor', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('reportable-condition-processor', 'warehouse', 'usage')::int;
select 1/pg_catalog.has_schema_privilege('reportable-condition-processor', 'shipping', 'usage')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.encounter', 'select')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.site', 'select')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.sample', 'select')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.presence_absence', 'select')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.target', 'select')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.organism', 'select')::int;
select 1/pg_catalog.has_table_privilege('reportable-condition-processor', 'shipping.reportable_condition', 'select,insert')::int;


select 1/(not pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.encounter', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.site', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.sample', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.presence_absence', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.target', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('reportable-condition-processor', 'warehouse.organism', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('reportable-condition-processor', 'shipping.reportable_condition', 'update,delete'))::int;

rollback;
