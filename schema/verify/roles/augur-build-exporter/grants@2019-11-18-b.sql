-- Verify seattleflu/id3c-customizations:roles/augur-build-exporter/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('augur-build-exporter', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('augur-build-exporter', 'shipping', 'usage')::int;
select 1/pg_catalog.has_table_privilege('augur-build-exporter', 'shipping.metadata_for_augur_build_v2', 'select')::int;
select 1/pg_catalog.has_table_privilege('augur-build-exporter', 'shipping.genomic_sequences_for_augur_build_v1', 'select')::int;

select 1/(not pg_catalog.has_table_privilege('augur-build-exporter', 'shipping.metadata_for_augur_build_v2', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('augur-build-exporter', 'shipping.genomic_sequences_for_augur_build_v1', 'insert,update,delete'))::int;

rollback;
