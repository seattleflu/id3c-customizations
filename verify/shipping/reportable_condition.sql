-- Verify seattleflu/id3c-customizations:shipping/reportable_condition on pg

begin;

select pg_catalog.has_table_privilege('shipping.reportable_condition', 'select');

rollback;
