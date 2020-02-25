-- Verify seattleflu/id3c-customizations:shipping/views on pg
-- requires: seattleflu/schema:shipping/schema

begin;

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.reportable_condition_v1');

-- Verify that the view has been dropped
select 1/(count(*) = 0)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.metadata_for_augur_build_v1');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.metadata_for_augur_build_v2');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.genomic_sequences_for_augur_build_v1');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.flu_assembly_jobs_v1');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.return_results_v1');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.fhir_encounter_details_v1');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.incidence_model_observation_v1');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.incidence_model_observation_v2');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.observation_with_presence_absence_result_v1');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.incidence_model_observation_v3');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.observation_with_presence_absence_result_v2');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.metadata_for_augur_build_v3');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.sample_with_best_available_encounter_data_v1');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.incidence_model_observation_v4');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.metadata_for_augur_build_v4');

select 1/(count(*) = 1)::int
  from information_schema.views
 where array[table_schema, table_name]::text[]
     = pg_catalog.parse_ident('shipping.incidence_model_observation_v5');

rollback;
