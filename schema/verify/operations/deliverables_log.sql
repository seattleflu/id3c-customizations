-- Verify seattleflu/id3c-customizations:operations/deliverables_log on pg

begin;

select pg_catalog.has_table_privilege('operations.deliverables_log', 'select');
select pg_catalog.has_table_privilege('operations.deliverables_log', 'insert');
select pg_catalog.has_table_privilege('operations.deliverables_log', 'delete');

rollback;
