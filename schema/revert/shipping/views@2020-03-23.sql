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

drop view shipping.reportable_condition_v1;
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

/* The shipping.reportable_condition_v1 view needs hCoV-19 visibility, so
 * remains owned by postgres, but it should only be accessible by
 * reportable-condition-notifier.  Revoke existing grants to every other role.
 *
 * XXX FIXME: There is a bad interplay here if roles/x/grants is also reworked
 * in the future.  It's part of the broader bad interplay between views and
 * their grants.  I think it was a mistake to lump grants to each role in their
 * own change instead of scattering them amongst the changes that create/rework
 * tables and views and things that are granted on.  I made that choice
 * initially so that all grants for a role could be seen in a single
 * consolidated place, which would still be nice.  There's got to be a better
 * system for managing this (a single idempotent change script with all ACLs
 * that is always run after other changes? cleaner breaking up of sqitch
 * projects?), but I don't have time to think on it much now.  Luckily for us,
 * I think the core reporter role is unlikely to be reworked soon, but we
 * should be wary.
 *   -trs, 7 March 2020
 */
revoke all on shipping.reportable_condition_v1 from reporter;


drop view shipping.metadata_for_augur_build_v2;
create or replace view shipping.metadata_for_augur_build_v2 as

    select sample as strain,
            cast(encountered as date) as date,
            'seattle' as region,
            -- XXX TODO: Change to PUMA and neighborhoods
            residence_census_tract as location,
            'Seattle Flu Study' as authors,
            age_range_coarse,
            case age_range_coarse <@ '[0 mon, 18 years)'::intervalrange
                when 't' then 'child'
                when 'f' then 'adult'
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

-- Does not need HCoV-19 visibility and should filter it out
-- anyway, but be safe.
alter view shipping.flu_assembly_jobs_v1 owner to "view-owner";



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


create or replace view shipping.incidence_model_observation_v1 as

    select encounter.identifier as encounter,

           (encountered at time zone 'US/Pacific')::date as encountered_date,
           to_char((encountered at time zone 'US/Pacific')::date, 'IYYY-"W"IW') as encountered_week,

           site.details->>'type' as site_type,

           individual.identifier as individual,
           individual.sex,

           -- Reporting 90 is more accurate than reporting nothing, and it will
           -- never be a real value in our dataset.
           --
           -- XXX TODO: This will be pre-processed out of the JSON in the future.
           ceiling(age_in_years(age))::int as age,

           age_bin_fine.range as age_range_fine,
           lower(age_bin_fine.range) as age_range_fine_lower,
           upper(age_bin_fine.range) as age_range_fine_upper,

           age_bin_coarse.range as age_range_coarse,
           lower(age_bin_coarse.range) as age_range_coarse_lower,
           upper(age_bin_coarse.range) as age_range_coarse_upper,

           residence_census_tract,
           work_census_tract,

           coalesce(encounter_responses.flu_shot,fhir.vaccine) as flu_shot,
           coalesce(encounter_responses.symptoms,fhir.symptoms) as symptoms,
           coalesce(encounter_responses.race,fhir.race) as race,
           coalesce(encounter_responses.hispanic_or_latino,fhir.hispanic_or_latino) as hispanic_or_latino,

           sample.identifier as sample

      from warehouse.encounter
      join warehouse.individual using (individual_id)
      join warehouse.site using (site_id)
      left join warehouse.sample using (encounter_id)
      left join shipping.age_bin_fine on age_bin_fine.range @> ceiling(age_in_years(age))::int
      left join shipping.age_bin_coarse on age_bin_coarse.range @> ceiling(age_in_years(age))::int
      left join shipping.fhir_encounter_details_v1 as fhir using (encounter_id)
      left join (
          select encounter_id, hierarchy->'tract' as residence_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'residence'
          or relation = 'lodging'
        ) as residence using (encounter_id)
      left join (
          select encounter_id, hierarchy->'tract' as work_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'workplace'
        ) as workplace using (encounter_id),

      lateral (
          -- XXX TODO: The data in this subquery will be modeled better in the
          -- future and the verbosity of extracting data from the JSON details
          -- document will go away.
          --   -trs, 22 March 2019

          select -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(nullif(responses."FluShot"[1], 'doNotKnow'), 'dontKnow')::bool as flu_shot,

                 -- XXX FIXME: Remove duplicate value collapsing when we're no
                 -- longer affected by this known Audere data quality issue.
                 array_distinct(responses."Symptoms") as symptoms,
                 array_distinct(responses."Race") as race,

                 -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(responses."HispanicLatino"[1], 'preferNotToSay')::bool as hispanic_or_latino

            from jsonb_to_record(encounter.details->'responses')
              as responses (
                  "FluShot" text[],
                  "Symptoms" text[],
                  "Race" text[],
                  "HispanicLatino" text[]))
        as encounter_responses

     order by encountered;

comment on view shipping.incidence_model_observation_v1 is
    'View of warehoused encounters and important questionnaire responses for modeling and viz teams';

revoke all
    on shipping.incidence_model_observation_v1
  from "incidence-modeler";

grant select
   on shipping.incidence_model_observation_v1
   to "incidence-modeler";


create or replace view shipping.incidence_model_observation_v2 as

    select encounter.identifier as encounter,

           (encountered at time zone 'US/Pacific')::date as encountered_date,
           to_char((encountered at time zone 'US/Pacific')::date, 'IYYY-"W"IW') as encountered_week,

           site.identifier as site,
           site.details->>'type' as site_type,

           individual.identifier as individual,
           individual.sex,

           age_in_years(age) as age,

           age_bin_fine_v2.range as age_range_fine,
           age_in_years(lower(age_bin_fine_v2.range)) as age_range_fine_lower,
           age_in_years(upper(age_bin_fine_v2.range)) as age_range_fine_upper,

           age_bin_coarse_v2.range as age_range_coarse,
           age_in_years(lower(age_bin_coarse_v2.range)) as age_range_coarse_lower,
           age_in_years(upper(age_bin_coarse_v2.range)) as age_range_coarse_upper,

           residence_census_tract,
           work_census_tract,

           coalesce(encounter_responses.flu_shot,fhir.vaccine) as flu_shot,
           coalesce(encounter_responses.symptoms,fhir.symptoms) as symptoms,
           coalesce(encounter_responses.race,fhir.race) as race,
           coalesce(encounter_responses.hispanic_or_latino,fhir.hispanic_or_latino) as hispanic_or_latino,

           sample.identifier as sample

      from warehouse.encounter
      join warehouse.individual using (individual_id)
      join warehouse.site using (site_id)
      left join warehouse.sample using (encounter_id)
      left join shipping.age_bin_fine_v2 on age_bin_fine_v2.range @> age
      left join shipping.age_bin_coarse_v2 on age_bin_coarse_v2.range @> age
      left join shipping.fhir_encounter_details_v1 as fhir using (encounter_id)
      left join (
          select encounter_id, hierarchy->'tract' as residence_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'residence'
          or relation = 'lodging'
        ) as residence using (encounter_id)
      left join (
          select encounter_id, hierarchy->'tract' as work_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'workplace'
        ) as workplace using (encounter_id),

      lateral (
          -- XXX TODO: The data in this subquery will be modeled better in the
          -- future and the verbosity of extracting data from the JSON details
          -- document will go away.
          --   -trs, 22 March 2019

          select -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(nullif(responses."FluShot"[1], 'doNotKnow'), 'dontKnow')::bool as flu_shot,

                 -- XXX FIXME: Remove duplicate value collapsing when we're no
                 -- longer affected by this known Audere data quality issue.
                 array_distinct(responses."Symptoms") as symptoms,
                 array_distinct(responses."Race") as race,

                 -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(responses."HispanicLatino"[1], 'preferNotToSay')::bool as hispanic_or_latino

            from jsonb_to_record(encounter.details->'responses')
              as responses (
                  "FluShot" text[],
                  "Symptoms" text[],
                  "Race" text[],
                  "HispanicLatino" text[]))
        as encounter_responses

     order by encountered;

comment on view shipping.incidence_model_observation_v2 is
    'Version 2 of view of warehoused encounters and important questionnaire responses for modeling and viz teams';

revoke all
    on shipping.incidence_model_observation_v2
  from "incidence-modeler";

grant select
   on shipping.incidence_model_observation_v2
   to "incidence-modeler";


create or replace view shipping.observation_with_presence_absence_result_v1 as

    select target,
           present,
           present::int as presence,
           observation.*,
           organism
      from shipping.incidence_model_observation_v2 as observation
      join shipping.presence_absence_result_v1 using (sample)
      order by site, encounter, sample, target;

comment on view shipping.observation_with_presence_absence_result_v1 is
  'Joined view of shipping.incidence_model_observation_v2 and shipping.presence_absence_result_v1';


create or replace view shipping.incidence_model_observation_v3 as

    select encounter.identifier as encounter,

           to_char((encountered at time zone 'US/Pacific')::date, 'IYYY-"W"IW') as encountered_week,

           site.details->>'type' as site_type,

           individual.identifier as individual,
           individual.sex,

           age_bin_fine_v2.range as age_range_fine,
           age_in_years(lower(age_bin_fine_v2.range)) as age_range_fine_lower,
           age_in_years(upper(age_bin_fine_v2.range)) as age_range_fine_upper,

           age_bin_coarse_v2.range as age_range_coarse,
           age_in_years(lower(age_bin_coarse_v2.range)) as age_range_coarse_lower,
           age_in_years(upper(age_bin_coarse_v2.range)) as age_range_coarse_upper,

           residence_census_tract,

           coalesce(encounter_responses.flu_shot,fhir.vaccine) as flu_shot,
           coalesce(encounter_responses.symptoms,fhir.symptoms) as symptoms,

           sample.identifier as sample

      from warehouse.encounter
      join warehouse.individual using (individual_id)
      join warehouse.site using (site_id)
      left join warehouse.sample using (encounter_id)
      left join shipping.age_bin_fine_v2 on age_bin_fine_v2.range @> age
      left join shipping.age_bin_coarse_v2 on age_bin_coarse_v2.range @> age
      left join shipping.fhir_encounter_details_v1 as fhir using (encounter_id)
      left join (
          select encounter_id, hierarchy->'tract' as residence_census_tract
          from warehouse.encounter_location
          left join warehouse.location using (location_id)
          where relation = 'residence'
          or relation = 'lodging'
        ) as residence using (encounter_id),

      lateral (
          -- XXX TODO: The data in this subquery will be modeled better in the
          -- future and the verbosity of extracting data from the JSON details
          -- document will go away.
          --   -trs, 22 March 2019

          select -- XXX FIXME: Remove use of nullif() when we're no longer
                 -- dealing with raw response values.
                 nullif(nullif(responses."FluShot"[1], 'doNotKnow'), 'dontKnow')::bool as flu_shot,

                 -- XXX FIXME: Remove duplicate value collapsing when we're no
                 -- longer affected by this known Audere data quality issue.
                 array_distinct(responses."Symptoms") as symptoms

            from jsonb_to_record(encounter.details->'responses')
              as responses (
                  "FluShot" text[],
                  "Symptoms" text[]))
        as encounter_responses

     order by encountered;

comment on view shipping.incidence_model_observation_v3 is
    'Version 3 of view of warehoused encounters and important questionnaire responses for modeling and viz teams';

revoke all
    on shipping.incidence_model_observation_v3
  from "incidence-modeler";

grant select
   on shipping.incidence_model_observation_v3
   to "incidence-modeler";


create or replace view shipping.observation_with_presence_absence_result_v2 as

    select target,
           present,
           present::int as presence,
           observation.*,
           organism
      from shipping.incidence_model_observation_v3 as observation
      join shipping.presence_absence_result_v1 using (sample)
      order by site_type, encounter, sample, target;

comment on view shipping.observation_with_presence_absence_result_v2 is
  'Joined view of shipping.incidence_model_observation_v3 and shipping.presence_absence_result_v1';


create or replace view shipping.metadata_for_augur_build_v3 as

    select sample.identifier as strain,
            coalesce(
              encountered_date,
              case
                when date_or_null(sample.details->>'date') <= current_date
                  then date_or_null(sample.details->>'date')
              end
            ) as date,
            'seattle' as region,
            -- XXX TODO: Change to PUMA and neighborhoods
            residence_census_tract as location,
            'Seattle Flu Study' as authors,
            age_range_coarse,
            case age_range_coarse <@ '[0 mon, 18 years)'::intervalrange
                when 't' then 'child'
                when 'f' then 'adult'
            end as age_category,
            warehouse.site.details->>'category' as site_category,
            residence_census_tract,
            flu_shot,
            sex

      from warehouse.sample
      left join shipping.incidence_model_observation_v2 on sample.identifier = incidence_model_observation_v2.sample
      left join warehouse.site on site.identifier = incidence_model_observation_v2.site

     where sample.identifier is not null;

comment on view shipping.metadata_for_augur_build_v3 is
		'View of metadata necessary for SFS augur build';


create or replace view shipping.sample_with_best_available_encounter_data_v1 as

    with specimen_manifest_data as (
        select
            sample_id,
            date_or_null(details->>'date') as collection_date,
            trim(both ' ' from details->>'swab_site') as swab_site,
            trim(both ' ' from details->>'sample_origin') as sample_origin
        from
            warehouse.sample
    ),

    site_details as (
      select
          site_id,
          site.identifier as site,
          site.details->>'type' as site_type,
          site.details->>'category' as site_category,
          coalesce(site.details->>'swab_site', site.details->>'sample_origin') as manifest_regex
      from warehouse.site
    ),

    samples_with_manifest_data as (
      select
        sample_id,
        site_id,
        coalesce(encountered::date, collection_date) as best_available_encounter_date,

        coalesce(
          case
              -- Environmental samples must be processed first, because they're
              -- often taken at existing human swab sites
              when manifest.sample_origin = 'es'
                then 'environmental'
              else manifest.swab_site
          end,

          manifest.sample_origin
        ) as site_manifest_details,

        site_id is not null as has_encounter_data

        from warehouse.sample
        left join warehouse.encounter using (encounter_id)
        left join specimen_manifest_data as manifest using (sample_id)
    )

  select
    sample_id,
    sample.identifier as sample,
    has_encounter_data,
    best_available_encounter_date,

    case
      when best_available_encounter_date < '2019-10-01'::date then 'Y1'
      when best_available_encounter_date < '2020-10-01'::date then 'Y2'
      else null
    end as season,

    coalesce(site.site_id, site_details.site_id) as best_available_site_id,
    coalesce(site.identifier, site_details.site) as best_available_site,
    coalesce(site.details->>'type', site_type) as best_available_site_type,
    coalesce(site.details->>'category', site_category) as best_available_site_category

  from warehouse.sample
  left join samples_with_manifest_data using (sample_id)
  left join site_details on (site_manifest_details similar to manifest_regex)
  left join warehouse.site on (samples_with_manifest_data.site_id = site.site_id)
  where sample.identifier is not null
  ;

comment on view shipping.sample_with_best_available_encounter_data_v1 is
    'Version 1 of view of warehoused samples and their best available encounter date and site details important for metrics calculations';


create or replace view shipping.return_results_v2 as

    select barcode,
           case
             when sample_id is null then 'notReceived'
             when sample_id is not null and count(present) = 0 then 'processing'
             when count(present) > 0 then 'complete'
           end as status,
           array_agg(distinct organism::text)
            filter (where present and organism is not null)  as organisms_present,
           array_agg(distinct organism::text)
            filter (where not present and organism is not null) as organisms_absent

      from warehouse.identifier
      join warehouse.identifier_set using (identifier_set_id)
      left join warehouse.sample on uuid::text = sample.collection_identifier
      left join warehouse.encounter using (encounter_id)
      left join shipping.presence_absence_result_v2 on sample.identifier = presence_absence_result_v2.sample

    --These are all past and current collection identifier sets not including self-test
    where identifier_set.name in ('collections-swab&send', 'collections-self-test')
      and (organism is null or
          -- We only return results for COVID-19, so omit all other presence/absence results
          organism <@ '{"Human_coronavirus.2019"}'::ltree[])
          -- We only want results collected after Izzy updated the REDCap consent form in swab & send.
          -- This filter-by timestamp comes from REDCap
      and coalesce(encountered, date_or_null(warehouse.sample.details->>'date')) > '2020-03-04 08:35:00-8'::timestamp with time zone
    group by barcode, sample_id
    order by barcode;

comment on view shipping.return_results_v2 is
    'Version 2 of view of barcodes and presence/absence results for return of results on website';


create or replace view shipping.hcov19_observation_v1 as

    with hcov19_presence_absence as (
        -- Collapse potentially multiple hCoV-19 results
        select distinct on (sample_id)
            sample_id,
            pa.created::date as hcov19_result_received,
            pa.present as hcov19_present
        from
            warehouse.presence_absence as pa
            join warehouse.target using (target_id)
            join warehouse.organism using (organism_id)
        where
            organism.lineage <@ 'Human_coronavirus.2019'
            and not control
        order by
            sample_id,
            present desc nulls last -- t → f → null
    )

    select
        sample_id,
        sample.identifier as sample,

        -- Lab testing-related columns
        hcov19_result_received,
        hcov19_present,

        -- Encounter-related columns
        encounter_id,
        best_available_encounter_date as encountered,
        to_char(best_available_encounter_date, 'IYYY-"W"IW') as encountered_week,

        best_available_site as site,
        best_available_site_type as site_type,

        location.hierarchy->'puma' as puma,

        -- Individual-related columns
        individual_id,
        age_bin_fine_v2.range as age_range_fine,
        lower(age_bin_fine_v2.range) as age_range_fine_lower,
        upper(age_bin_fine_v2.range) as age_range_fine_upper,
        sex,

        -- Misc cruft
        sample.details->>'sample_origin' as manifest_origin

        /* XXX TODO
         *   → symptoms (Mike says can be JSON blob)
         *   → symptom onset (date)
         *   → race, SES, housing, etc
         */
    from
        warehouse.sample
        left join shipping.sample_with_best_available_encounter_data_v1 using (sample_id)
        left join warehouse.encounter using (encounter_id)
        left join warehouse.individual using (individual_id)
        left join shipping.age_bin_fine_v2 on age_bin_fine_v2.range @> age
        left join warehouse.primary_encounter_location using (encounter_id)
        left join warehouse.location using (location_id)
        left join hcov19_presence_absence using (sample_id)
    where
        /* All tested samples plus samples and encounters after 22 Feb 2020, as
         * we presume those _will be_ tested.  This criteria comes from Mike
         * Famulare:
         *
         *   https://seattle-flu-study.slack.com/archives/GU24NGD18/p1583684258051000
         *
         * Note that when comparing some row-valued X, the expressions "X is
         * not null" and "X is distinct from null" behave differently.  We want
         * the latter.
         */
        (hcov19_presence_absence is distinct from null or best_available_encounter_date >= '2020-02-22')

        /* Exclude environmental swabs.
         *
         * Note that the standard index on details which supports containment
         * (@>) can't/won't be used by the planner because of both the negated
         * condition ("not … @>") and coupled "is null" check.  If this
         * condition is slow, we could directly index
         * details->>'sample_origin'.
         */
        and (sample.details is null or sample.details->>'sample_origin' != 'es')
;


/* The shipping.hcov19_observation_v1 view needs hCoV-19 visibility, so
 * remains owned by postgres, but it should only be accessible by those with
 * hcov19-visibility.  Revoke existing grants to every other role.
 *
 * XXX FIXME: There is a bad interplay here if roles/x/grants is also reworked
 * in the future.  It's part of the broader bad interplay between views and
 * their grants.  I think it was a mistake to lump grants to each role in their
 * own change instead of scattering them amongst the changes that create/rework
 * tables and views and things that are granted on.  I made that choice
 * initially so that all grants for a role could be seen in a single
 * consolidated place, which would still be nice.  There's got to be a better
 * system for managing this (a single idempotent change script with all ACLs
 * that is always run after other changes? cleaner breaking up of sqitch
 * projects?), but I don't have time to think on it much now.  Luckily for us,
 * I think the core reporter role is unlikely to be reworked soon, but we
 * should be wary.
 *   -trs, 7 March 2020
 */
revoke all on shipping.hcov19_observation_v1 from reporter;

grant select
    on shipping.hcov19_observation_v1
    to "hcov19-visibility";


comment on view shipping.hcov19_observation_v1 is
  'Custom view of hCoV-19 samples with presence-absence results and best available encounter data';


create or replace view shipping.scan_return_results_v1 as

    with hcov19_presence_absence as (
        -- Collapse potentially multiple hCoV-19 results
        select distinct on (sample_id)
            sample_id,
            presence_absence_id,
            pa.present as hcov19_present
        from
            warehouse.presence_absence as pa
            join warehouse.target using (target_id)
            join warehouse.organism using (organism_id)
        where
            organism.lineage <@ 'Human_coronavirus.2019'
            and not control
        /*
          Keep only the most recent push. According to Lea, samples are only
          retested if there is a failed result. A positive, negative, or
          indeterminate result would not be retested.

          https://seattle-flu-study.slack.com/archives/CV1E2BC8N/p1584570226450500?thread_ts=1584569401.449800&cid=CV1E2BC8N
        */
        order by
            sample_id, presence_absence_id desc
    ),

    scan_barcodes as (
      select
        sample_id,
        barcode as qrcode,
        encountered::date as collect_ts

      from
        warehouse.identifier
        join warehouse.identifier_set using (identifier_set_id)
        left join warehouse.sample on uuid::text = sample.collection_identifier
        left join warehouse.encounter using (encounter_id)
      where
        identifier_set.name in('collections-scan')
      order by encountered, barcode
    )

    select
        qrcode,
        collect_ts,
        case
            when sample_id is null then 'not-received'
            when sample_id is not null and presence_absence_id is null then 'pending'
            when hcov19_present is true then 'positive'
            when hcov19_present is false then 'negative'
            when presence_absence_id is not null and hcov19_present is null then 'inconclusive'
        end as status_code
    from
      scan_barcodes
      left join hcov19_presence_absence using (sample_id)
    ;

/* The shipping.scan_return_results_v1 view needs hCoV-19 visibility, so
 * remains owned by postgres, but it should only be accessible by those with
 * hcov19-visibility.  Revoke existing grants to every other role.
 *
 * XXX FIXME: There is a bad interplay here if roles/x/grants is also reworked
 * in the future.  It's part of the broader bad interplay between views and
 * their grants.  I think it was a mistake to lump grants to each role in their
 * own change instead of scattering them amongst the changes that create/rework
 * tables and views and things that are granted on.  I made that choice
 * initially so that all grants for a role could be seen in a single
 * consolidated place, which would still be nice.  There's got to be a better
 * system for managing this (a single idempotent change script with all ACLs
 * that is always run after other changes? cleaner breaking up of sqitch
 * projects?), but I don't have time to think on it much now.  Luckily for us,
 * I think the core reporter role is unlikely to be reworked soon, but we
 * should be wary.
 *   -trs, 7 March 2020
 */
revoke all on shipping.scan_return_results_v1 from reporter;

grant select
    on shipping.scan_return_results_v1
    to "hcov19-visibility";


comment on view shipping.scan_return_results_v1 is
  'View of barcodes and presence/absence results for SCAN return of results on the UW Lab Med site';


commit;
