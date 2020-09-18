-- Verify seattleflu/id3c-customizations:btree_gist on pg

begin;

create temporary table __btree_gist_test (a text);
create index __btree_gist_test_idx on __btree_gist_test using gist (a);

select 1/(count(*) = 1)::int
  from pg_catalog.pg_extension as e
  left join pg_catalog.pg_namespace as n on (n.oid = e.extnamespace)
 where extname = 'btree_gist'
   and nspname = 'public';

rollback;
