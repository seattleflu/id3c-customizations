-- Deploy seattleflu/id3c-customizations:shipping/views to pg
-- requires: seattleflu/schema:shipping/schema

-- Hello!  All shipping views are defined here.  Rework this change with Sqitch
-- to change a view definition or add new views.  This workflow helps keep
-- inter-view dependencies manageable.

begin;

-- This view is versioned as a hedge against future changes.  Changing this
-- view in place is fine as long as changes are backwards compatible.  Think of
-- the version number as the major part of a semantic versioning scheme.  If
-- there needs to be a lag between view development and consumers being
-- updated, copy the view definition into v2 and make changes there.

create or replace view shipping.reportable_condition_v1 as

    with reportable as (
        select array_agg(lineage) as lineages
        from warehouse.organism
        where details @> '{"report_to_public_health":true}'
    )

    select
        presence_absence.presence_absence_id,
        organism.lineage::text,
        sample.identifier as sample,
        barcode,
        site.identifier as site,
        presence_absence.details->>'reporting_log' as reporting_log,
        sample.details->'_provenance'->>'workbook' as workbook,
        sample.details->'_provenance'->>'sheet' as sheet,
        sample.details->>'sample_origin' as sample_origin,
        sample.details->>'swab_site' as swab_site

    from warehouse.presence_absence
    join warehouse.target using (target_id)
    join warehouse.organism using (organism_id)
    join warehouse.sample using (sample_id)
    join warehouse.identifier on (
        sample.identifier = cast(identifier.uuid as text))
    left join warehouse.encounter using (encounter_id)
    left join warehouse.site using (site_id)

    where organism.lineage <@ (table reportable)
    and present

    order by encountered desc;


create or replace view shipping.metadata_for_augur_build_v2 as

    select  sample as strain,
            encountered as date,
            'seattle' as region,
            -- XXX TODO: Change to PUMA and neighborhoods
            residence_census_tract as location,
            'Seattle Flu Study' as authors,
            case
                when age_range_coarse <@ '[0 mon, 18 years)'::intervalrange then 'child'
                else 'adult'
            end as age_category,
            case
                when site_type in ('childrensHospital', 'childrensClinic', 'childrensHospital', 'clinic', 'hospital', 'retrospective') then 'clinical'
                when site_type in ('childcare' , 'collegeCampus' , 'homelessShelter' , 'port', 'publicSpace', 'workplace') then 'community'
            end as site_category,
            residence_census_tract,
            flu_shot,
            sex

      from shipping.incidence_model_observation_v3
      join warehouse.encounter on encounter.identifier = incidence_model_observation_v3.encounter;

comment on view shipping.metadata_for_augur_build_v2 is
		'View of metadata necessary for SFS augur build';


create or replace view shipping.genomic_sequences_for_augur_build_v1 as

    select distinct on (sample.identifier, organism.lineage, segment)
           sample.identifier as sample,
           organism.lineage as organism,
           genomic_sequence.segment,
           round(((length(replace(seq, 'N', '')) * 1.0 / length(seq))), 4) as coverage,
           genomic_sequence.seq
      from warehouse.sample
      join warehouse.consensus_genome using (sample_id)
      join warehouse.genomic_sequence using (consensus_genome_id)
      join warehouse.organism using (organism_id)

      order by sample.identifier, organism.lineage, segment, coverage desc;

comment on view shipping.genomic_sequences_for_augur_build_v1 is
    'View of genomic sequences for SFS augur build';


create or replace view shipping.flu_assembly_jobs_v1 as

    select sample.identifier as sfs_uuid,
           sample.details ->> 'nwgc_id' as nwgc_id,
           sequence_read_set.urls,
           target.identifier as target,
           o1.lineage as target_linked_organism,
           coalesce(o2.lineage, o1.lineage) as assembly_job_organism,
           sequence_read_set.details

      from warehouse.sequence_read_set
      join warehouse.sample using (sample_id)
      join warehouse.presence_absence using (sample_id)
      join warehouse.target using (target_id)
      join warehouse.organism o1 using (organism_id)
      -- Reason for o2 join: For pan-subtype targets like Flu_A_pan,
      -- we want to spread out to the more specific lineages for assembly jobs.
      -- Since this view is Flu specific, we are hard-coding it to spread just one level down.
      left join warehouse.organism o2 on (o2.lineage ~ concat(o1.lineage::text, '.*{1}')::lquery)

    where o1.lineage ~ 'Influenza.*'
    and present
    -- The sequence read set details currently holds the state of assembly jobs for each lineage.
    -- So if null, then definitely no jobs have been completed.
    -- If not null, then check if the assembly job organism matches any key in the sequence read set details.
    and (sequence_read_set.details is null
         or not sequence_read_set.details ? coalesce(o2.lineage, o1.lineage)::text)

    order by sample.details ->> 'nwgc_id';

comment on view shipping.flu_assembly_jobs_v1 is
    'View of flu jobs that still need to be run through the assembly pipeline';


create or replace view shipping.return_results_v1 as

    select barcode,
           case
             when sample_id is null then 'notReceived'
             when sample_id is not null and count(present) = 0 then 'processing'
             when count(present) > 0 then 'complete'
           end as status,
           -- We only return the top level organisms for results so we want to omit subtypes
           array_agg(distinct subpath(organism, 0, 1)::text)
            filter (where present and organism is not null)  as organisms_present,
           array_agg(distinct subpath(organism, 0, 1)::text)
            filter (where not present and organism is not null) as organisms_absent

      from warehouse.identifier
      join warehouse.identifier_set using (identifier_set_id)
      left join warehouse.sample on uuid::text = sample.collection_identifier
      left join shipping.presence_absence_result_v1 on sample.identifier = presence_absence_result_v1.sample

    --These are all past and current collection identifier sets not including self-test
    where identifier_set.name in ('collections-seattleflu.org',
                                  'collections-kiosks',
                                  'collections-environmental',
                                  'collections-swab&send',
                                  'collections-household-observation',
                                  'collections-household-intervention') and
          (organism is null or
          -- We only return results for these organisms, so omit all other presence/absence results
          organism <@ '{"Adenovirus", "Human_coronavirus", "Enterovirus", "Influenza", "Human_metapneumovirus", "Human_parainfluenza", "Rhinovirus", "RSV"}'::ltree[])
    group by barcode, sample_id
    order by barcode;

comment on view shipping.return_results_v1 is
    'View of barcodes and presence/absence results for return of results on website';


create or replace view shipping.fhir_encounter_details_v1 as

    with
        fhir as (
          select encounter_id,
                 case
                    when details -> 'Condition' is not null then array_agg(distinct condition."id" order by condition."id")
                    else null
                 end as symptoms,
                 jsonb_object_agg(response."linkId", response.answer order by response.answer) filter (where response."linkId" is not null) as responses
            from warehouse.encounter
            left join jsonb_to_recordset(details -> 'Condition') as condition("id" text) on true
            left join jsonb_to_recordset(details -> 'QuestionnaireResponse') as q("item" jsonb) on true
            left join jsonb_to_recordset(q."item") as response("linkId" text, "answer" jsonb) on true
          group by encounter_id
        ),

        vaccine as (
            --Builds vaccine jsonb object without null values that spans multiple rows.
            --Expand the rows into key/value pairs with jsonb_each and
            --aggregate that back into a single jsonb value.
            --  -Jover 10 Jan 2020
            select encounter_id,
                   jsonb_object_agg(t.k, t.v order by t.v) filter (where t.k is not null) as vaccine
              from (
                  select encounter_id,
                         jsonb_strip_nulls(
                           jsonb_build_object(
                             'vaccine', vaccine."valueBoolean",
                             'vaccine_date', vaccine."valueDate")) as vaccine_obj
                  from fhir,
                       jsonb_to_record(responses) as response("vaccine" jsonb),
                       jsonb_to_recordset(response."vaccine") as vaccine("valueBoolean" bool, "valueDate" text)) as vaccine,
              jsonb_each(vaccine.vaccine_obj) as t(k,v)
            group by encounter_id
        ),

        array_responses as (
            select
                encounter_id,
                case
                    when responses -> 'race' is not null then array_agg(coalesce(race."valueCoding" ->> 'code', race."valueString") order by race."valueString")
                    else null
                end as race,
                case
                    when responses -> 'insurance' is not null then array_agg(insurance."valueString" order by insurance."valueString")
                    else null
                end as insurance
              from fhir
              left join jsonb_to_record(responses) as response("race" jsonb, "insurance" jsonb) on true
              left join jsonb_to_recordset(response."race") as race ("valueCoding" jsonb, "valueString" text) on true
              left join jsonb_to_recordset(response."insurance") as insurance ("valueString" text) on true
            group by encounter_id, responses
        ),

        boolean_responses as (
            select
                encounter_id,
                ethnicity."valueBoolean" as hispanic_or_latino,
                travel_countries."valueBoolean" as travel_countries,
                travel_states."valueBoolean" as travel_states
            from fhir
            left join jsonb_to_record(responses) as response("ethnicity" jsonb, "travel_countries" jsonb, "travel_states" jsonb) on true
            left join jsonb_to_recordset(response."ethnicity") as ethnicity("valueBoolean" bool) on true
            left join jsonb_to_recordset(response."travel_countries") as travel_countries("valueBoolean" bool) on true
            left join jsonb_to_recordset(response."travel_states") as travel_states("valueBoolean" bool) on true
        )


    select
        encounter_id,
        symptoms,
        (vaccine ->> 'vaccine')::bool as vaccine,
        vaccine ->> 'vaccine_date' as vaccine_date,
        race,
        insurance,
        hispanic_or_latino,
        travel_countries,
        travel_states
    from fhir
         left join vaccine using (encounter_id)
         left join array_responses using (encounter_id)
         left join boolean_responses using (encounter_id);

comment on view shipping.fhir_encounter_details_v1 is
  'A view of encounter details that are in FHIR format';

revoke all
    on shipping.fhir_encounter_details_v1
  from "incidence-modeler";

grant select
   on shipping.fhir_encounter_details_v1
   to "incidence-modeler";

-- Not including drop view statements for shipping views ported from
-- core ID3C because other views are dependent on them.

commit;
