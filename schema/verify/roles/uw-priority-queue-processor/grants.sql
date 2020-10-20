-- Verify seattleflu/id3c-customizations:roles/uw-priority-queue-processor/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('uw-priority-queue-processor', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('uw-priority-queue-processor', 'shipping', 'usage')::int;

rollback;
