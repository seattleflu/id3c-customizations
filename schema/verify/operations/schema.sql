-- Verify seattleflu/id3c-customizations:operations/schema on pg

begin;

select 1/pg_catalog.has_schema_privilege('operations', 'usage')::int;

rollback;
