-- Verify seattleflu/id3c-customizations:shipping/latest_results on pg

begin;

select pg_catalog.has_table_privilege('shipping.latest_results', 'select');
select pg_catalog.has_table_privilege('shipping.latest_results', 'insert');
select pg_catalog.has_table_privilege('shipping.latest_results', 'delete');

rollback;
