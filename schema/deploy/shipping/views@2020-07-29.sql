-- Deploy seattleflu/id3c-customizations:shipping/views to pg
-- requires: seattleflu/schema:shipping/schema

-- Hello!  All shipping views are defined here.  Rework this change with Sqitch
-- to change a view definition or add new views.  This workflow helps keep
-- inter-view dependencies manageable.

-- Views are versioned as a hedge against future changes.  Changing these
-- views in place is fine as long as changes are backwards compatible. Think of
-- the version number as the major part of a semantic versioning scheme.  If
-- there needs to be a lag between view development and consumers being
-- updated, copy the view definition into v2 and make changes there.

begin;

-- Drop all views at the top in order of dependency so we don't have to
-- worry about view dependencies when reworking view definitions.
drop view if exists shipping.scan_enrollments_v1;
drop view if exists shipping.seattle_neighborhood_districts_v1;
drop view if exists shipping.scan_hcov19_positives_v1;
drop view if exists shipping.scan_demographics_v1;

drop view if exists shipping.scan_return_results_v1;
drop view if exists shipping.return_results_v2;
drop view if exists shipping.return_results_v1;
drop view if exists shipping.reportable_condition_v1;

drop view if exists shipping.metadata_for_augur_build_v3;
drop view if exists shipping.metadata_for_augur_build_v2;
drop view if exists shipping.genomic_sequences_for_augur_build_v1;
drop view if exists shipping.flu_assembly_jobs_v1;

drop view if exists shipping.scan_follow_up_encounters_v1;
drop view if exists shipping.scan_encounters_v1;
drop view if exists shipping.hcov19_observation_v1;

drop view if exists shipping.observation_with_presence_absence_result_v2;
drop view if exists shipping.observation_with_presence_absence_result_v1;

drop view if exists shipping.incidence_model_observation_v3;
drop view if exists shipping.incidence_model_observation_v2;
drop view if exists shipping.incidence_model_observation_v1;

drop view if exists shipping.fhir_encounter_details_v2;
drop view if exists shipping.fhir_encounter_details_v1;
drop materialized view if exists shipping.fhir_questionnaire_responses_v1;

drop view if exists shipping.sample_with_best_available_encounter_data_v1;

/******************** VIEWS FOR INTERNAL USE ********************/
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
  ;

comment on view shipping.sample_with_best_available_encounter_data_v1 is
    'Version 1 of view of warehoused samples and their best available encounter date and site details important for metrics calculations';


create materialized view shipping.fhir_questionnaire_responses_v1 as

    select encounter_id,
           "linkId" as link_id,
           array_remove(array_agg("valueString" order by "valueString"), null) as string_response,
           -- All boolean values must be true to be considered True
           -- I don't think we will ever get two answers for one boolean question, but just in case.
           -- Jover, 18 March 2020
           bool_and("valueBoolean") as boolean_response,
           array_remove(array_agg("valueDate" order by "valueDate"), null) as date_response,
           array_remove(array_agg("valueInteger" order by "valueInteger"), null) as integer_response,
           array_remove(array_agg("code" order by "code"), null) as code_response
      from warehouse.encounter,
           jsonb_to_recordset(details -> 'QuestionnaireResponse') as q("item" jsonb),
           jsonb_to_recordset("item") as response("linkId" text, "answer" jsonb),
           jsonb_to_recordset("answer") as answer("valueString" text,
                                                  "valueBoolean" bool,
                                                  "valueDate" text,
                                                  "valueInteger" integer,
                                                  "valueCoding" jsonb),
           jsonb_to_record("valueCoding") as code("code" text)
    -- Don't need age because it is formalized in `warehouse.encounter.age`
    where "linkId" != 'age'
    group by encounter_id, link_id;

create index fhir_questionnaire_responses_link_id_idx on shipping.fhir_questionnaire_responses_v1 (link_id);
create index fhir_questionnaire_responses_encounter_id_idx on shipping.fhir_questionnaire_responses_v1 (encounter_id);
create unique index fhir_questionnaire_responses_unique_link_id_per_encounter on shipping.fhir_questionnaire_responses_v1 (encounter_id, link_id);

comment on materialized view shipping.fhir_questionnaire_responses_v1 is
  'View of FHIR Questionnaire Responses store in encounter details';

revoke all
    on shipping.fhir_questionnaire_responses_v1
  from "incidence-modeler";

grant select
   on shipping.fhir_questionnaire_responses_v1
   to "incidence-modeler";


create or replace view shipping.fhir_encounter_details_v1 as

    with
        symptoms as (
          select encounter_id,
                 array_agg(distinct condition."id" order by condition."id") as symptoms
            from warehouse.encounter,
                 jsonb_to_recordset(details -> 'Condition') as condition("id" text)
          group by encounter_id
        ),

        vaccine as (
          select encounter_id,
                 boolean_response as vaccine,
                 date_response[1] as vaccine_date
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vaccine'
        ),

        race as (
          select encounter_id,
                 case
                    when array_length(code_response, 1) is null then string_response
                    else code_response
                 end as race
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'race'
        ),

        insurance as (
          select encounter_id,
                 string_response as insurance
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'insurance'
        ),

        ethnicity as (
          select encounter_id,
                 boolean_response as hispanic_or_latino
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'ethnicity'
        ),

        travel_countries as (
          select encounter_id,
                 boolean_response as travel_countries
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'travel_countries'
        ),

        travel_states as (
          select encounter_id,
                 boolean_response as travel_states
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'travel_states'
        )

    select
        encounter_id,
        symptoms,
        vaccine,
        vaccine_date,
        race,
        insurance,
        hispanic_or_latino,
        travel_countries,
        travel_states
      from warehouse.encounter
      left join symptoms using (encounter_id)
      left join vaccine using (encounter_id)
      left join race using (encounter_id)
      left join insurance using (encounter_id)
      left join ethnicity using (encounter_id)
      left join travel_countries using (encounter_id)
      left join travel_states using (encounter_id);

comment on view shipping.fhir_encounter_details_v1 is
  'A view of encounter details that are in FHIR format';

revoke all
    on shipping.fhir_encounter_details_v1
  from "incidence-modeler";

grant select
   on shipping.fhir_encounter_details_v1
   to "incidence-modeler";


create or replace view shipping.fhir_encounter_details_v2 as

    with
        symptoms as (
          select encounter_id,
                 array_agg(distinct condition."id" order by condition."id") as symptoms,
                 -- In our FHIR etl we give all symptoms the same onsetDateTime
                 "onsetDateTime" as symptom_onset
            from warehouse.encounter,
                 jsonb_to_recordset(details -> 'Condition') as condition("id" text, "onsetDateTime" text)
          where not condition."id" like '%_2'
          group by encounter_id, symptom_onset
        ),

        symptoms_2 as (
          select encounter_id,
                 array_agg(distinct rtrim(condition."id", '_2') order by rtrim(condition."id", '_2')) as symptoms_2,
                 -- In our FHIR etl we give all symptoms the same onsetDateTime
                 "onsetDateTime" as symptom_onset_2
            from warehouse.encounter,
                 jsonb_to_recordset(details -> 'Condition') as condition("id" text, "onsetDateTime" text)
          where condition."id" like '%_2'
          group by encounter_id, symptom_onset_2
        ),

        scan_study_arm as (
          select encounter_id,
                 string_response[1] as scan_study_arm
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'redcap_event_name'
        ),

        priority_code as (
          select encounter_id,
                 string_response[1] as priority_code
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'priority_code'
        ),

        vaccine as (
          select encounter_id,
                 boolean_response as vaccine,
                 date_response[1] as vaccine_date
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vaccine'
        ),

        race as (
          select encounter_id,
                 case
                    when array_length(code_response, 1) is null then string_response
                    else code_response
                 end as race
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'race'
        ),

        insurance as (
          select encounter_id,
                 string_response as insurance
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'insurance'
        ),

        ethnicity as (
          select encounter_id,
                 boolean_response as hispanic_or_latino
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'ethnicity'
        ),

        travel_countries as (
          select encounter_id,
                 bool_or(boolean_response) as travel_countries
            from shipping.fhir_questionnaire_responses_v1
          where link_id in ('travel_countries','travel_countries_phs')
          group by encounter_id
        ),

        countries as (
          select encounter_id,
                 string_response as countries
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'country'
        ),

        travel_states as (
          select encounter_id,
                 bool_or(boolean_response) as travel_states
            from shipping.fhir_questionnaire_responses_v1
          where link_id in ('travel_states', 'travel_states_phs')
          group by encounter_id
        ),

        states as (
          select encounter_id,
                 string_response as states
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'state'
        ),

        pregnant as (
          select encounter_id,
                 boolean_response as pregnant
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'pregnant_yesno'
        ),

        income as (
          select encounter_id,
                 string_response[1] as income
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'income'
        ),

        housing_type as (
          select encounter_id,
                 string_response[1] as housing_type
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'housing_type'
        ),

        house_members as (
          select encounter_id,
                 string_response[1] as house_members
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'house_members'
        ),

        clinical_care as (
          select encounter_id,
                 string_response as clinical_care
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'doctor_3e8fae'
        ),

        hospital_where as (
          select encounter_id,
                 string_response[1] as hospital_where
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'hospital_where'
        ),

        hospital_visit_type as (
          select encounter_id,
                 string_response[1] as hospital_visit_type
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'hospital_ed'
        ),

        hospital_arrive as (
          select encounter_id,
                 date_response[1] as hospital_arrive
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'hospital_arrive'
        ),

        hospital_leave as (
          select encounter_id,
                 date_response[1] as hospital_leave
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'hospital_leave'
        ),

        smoking as (
          select encounter_id,
                 string_response as smoking
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'smoke_9a005a'
        ),

        chronic_illness as (
          select encounter_id,
                 string_response as chronic_illness
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'chronic_illness'
        ),

        overall_risk_health as (
          select encounter_id,
                 string_response as overall_risk_health
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'overall_risk_health'
        ),

        overall_risk_setting as (
          select encounter_id,
                 string_response as overall_risk_setting
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'overall_risk_setting'
        ),

        long_term_type as (
          select encounter_id,
                 string_response as long_term_type
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'longterm_type'
        ),

        ace as (
          select encounter_id,
                 string_response as ace_inhibitor
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'ace'
        ),

        website_id as (
          select encounter_id,
                 string_response[1] as website_id
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'website_id'
        ),

        prior_test as (
          select encounter_id,
                 boolean_response as prior_test
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'prior_test'
        ),

        prior_test_positive as (
          select encounter_id,
                 string_response as prior_test_positive
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'prior_test_positive'
        ),

        prior_test_positive_date as (
          select encounter_id,
                 date_response[1] as prior_test_positive_date
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'prior_test_positive_date'
        ),

        prior_test_type as (
          select encounter_id,
                 string_response as prior_test_type
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'prior_test_type'
        ),

        prior_test_number as (
          select encounter_id,
                 integer_response[1] as prior_test_number
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'prior_test_number'
        ),

        prior_test_result as (
          select encounter_id,
                 string_response[1] as prior_test_result
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'prior_test_result'
        ),

        contact as (
          select encounter_id,
                 string_response as contact
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'contact'
        ),

        wash_hands as (
          select encounter_id,
                 string_response[1] as wash_hands
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'wash_hands'
        ),

        clean_surfaces as (
          select encounter_id,
                 string_response[1] as clean_surfaces
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'clean_surfaces'
        ),

        hide_cough as (
          select encounter_id,
                 string_response[1] as hide_cough
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'hide_cough'
        ),

        mask as (
          select encounter_id,
                 string_response[1] as mask
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'mask'
        ),

        distance as (
          select encounter_id,
                 string_response[1] as distance
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'distance'
        ),

        attend_event as (
          select encounter_id,
                 string_response[1] as attend_event
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'attend_event'
        ),

        wfh as (
          select encounter_id,
                 string_response[1] as wfh
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'wfh'
        ),

        industry as (
          select encounter_id,
                 string_response as industry
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'industry'
        ),

        illness_questionnaire_date as (
          select encounter_id,
                 date_response[1] as illness_questionnaire_date
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'illness_q_date'
        )

    select
        encounter_id,
        scan_study_arm,
        priority_code,
        symptoms,
        symptom_onset,
        symptoms_2,
        symptom_onset_2,
        vaccine,
        vaccine_date,
        race,
        insurance,
        hispanic_or_latino,
        travel_countries,
        countries,
        travel_states,
        states,
        pregnant,
        income,
        housing_type,
        house_members,
        clinical_care,
        hospital_where,
        hospital_visit_type,
        hospital_arrive,
        hospital_leave,
        smoking,
        chronic_illness,
        overall_risk_health,
        overall_risk_setting,
        long_term_type,
        ace_inhibitor,
        website_id,
        prior_test,
        prior_test_positive,
        prior_test_positive_date,
        prior_test_type,
        prior_test_number,
        prior_test_result,
        contact,
        wash_hands,
        clean_surfaces,
        hide_cough,
        mask,
        distance,
        attend_event,
        wfh,
        industry,
        illness_questionnaire_date

      from warehouse.encounter
      left join scan_study_arm using (encounter_id)
      left join priority_code using (encounter_id)
      left join symptoms using (encounter_id)
      left join symptoms_2 using (encounter_id)
      left join vaccine using (encounter_id)
      left join race using (encounter_id)
      left join insurance using (encounter_id)
      left join ethnicity using (encounter_id)
      left join travel_countries using (encounter_id)
      left join countries using (encounter_id)
      left join travel_states using (encounter_id)
      left join states using (encounter_id)
      left join pregnant using (encounter_id)
      left join income using (encounter_id)
      left join housing_type using (encounter_id)
      left join house_members using (encounter_id)
      left join clinical_care using (encounter_id)
      left join hospital_where using (encounter_id)
      left join hospital_visit_type using (encounter_id)
      left join hospital_arrive using (encounter_id)
      left join hospital_leave using (encounter_id)
      left join smoking using (encounter_id)
      left join chronic_illness using (encounter_id)
      left join overall_risk_health using (encounter_id)
      left join overall_risk_setting using (encounter_id)
      left join long_term_type using (encounter_id)
      left join ace using (encounter_id)
      left join website_id using (encounter_id)
      left join prior_test using (encounter_id)
      left join prior_test_positive using (encounter_id)
      left join prior_test_positive_date using (encounter_id)
      left join prior_test_type using (encounter_id)
      left join prior_test_number using (encounter_id)
      left join prior_test_result using (encounter_id)
      left join contact using (encounter_id)
      left join wash_hands using (encounter_id)
      left join clean_surfaces using (encounter_id)
      left join hide_cough using (encounter_id)
      left join mask using (encounter_id)
      left join distance using (encounter_id)
      left join attend_event using (encounter_id)
      left join wfh using (encounter_id)
      left join industry using (encounter_id)
      left join illness_questionnaire_date using (encounter_id)
  ;
comment on view shipping.fhir_encounter_details_v2 is
  'A v2 view of encounter details that are in FHIR format that includes all SCAN questionnaire answers';

revoke all
    on shipping.fhir_encounter_details_v2
  from "incidence-modeler";

grant select
   on shipping.fhir_encounter_details_v2
   to "incidence-modeler";


/******************** VIEWS FOR IDM MODELERS ********************/
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


create or replace view shipping.hcov19_observation_v1 as

    with hcov19_presence_absence_bbi as (
        select
          sample_id,
          hcov19_result_received_bbi,
          hcov19_present_bbi,
          array_agg("crt") as crt_values
        from (
            -- Collapse potentially multiple hCoV-19 results
            select distinct on (sample_id)
                sample_id,
                pa.created::date as hcov19_result_received_bbi,
                pa.present as hcov19_present_bbi,
                pa.details -> 'replicates' as replicates
            from
                warehouse.presence_absence as pa
                join warehouse.target using (target_id)
                join warehouse.organism using (organism_id)
            where
                organism.lineage <@ 'Human_coronavirus.2019'
                and not control
                and not pa.details @> '{"device" : "clinical"}'
            order by
                sample_id,
                presence_absence_id desc
        ) as deduplicated_hcov19_bbi
        left join jsonb_to_recordset(replicates) as r("crt" text) on true

        group by sample_id, hcov19_result_received_bbi, hcov19_present_bbi
    ),

    hcov19_presence_absence_uw as (
        -- Collapse potentially multiple hCoV-19 results
        select distinct on (sample_id)
            sample_id,
            pa.created::date as hcov19_result_received_uw,
            pa.present as hcov19_present_uw
        from
            warehouse.presence_absence as pa
            join warehouse.target using (target_id)
            join warehouse.organism using (organism_id)
        where
            organism.lineage <@ 'Human_coronavirus.2019'
            and not control
            and pa.details @> '{"device" : "clinical"}'
        order by
            sample_id,
            presence_absence_id desc
    )

    select
        sample_id,
        sample.identifier as sample,
        sample.collected,

        -- Lab testing-related columns for BBI
        hcov19_result_received_bbi,
        hcov19_present_bbi,
        crt_values,

        -- Lab testing-related columns for UW
        hcov19_result_received_uw,
        hcov19_present_uw,

        -- Encounter-related columns
        encounter_id,
        best_available_encounter_date as encountered,
        to_char(best_available_encounter_date, 'IYYY-"W"IW') as encountered_week,

        best_available_site as site,
        best_available_site_type as site_type,

        location.hierarchy->'tract' as census_tract,
        location.hierarchy->'puma' as puma,
        case location.scale
          -- Only include address identifiers as those are used to identify
          -- participants of the same household.
          when 'address' then location.identifier
        end as address_identifier,

        -- Individual-related columns
        individual_id,
        age_bin_fine_v2.range as age_range_fine,
        lower(age_bin_fine_v2.range) as age_range_fine_lower,
        upper(age_bin_fine_v2.range) as age_range_fine_upper,
        sex,

        -- Misc cruft
        sample.details->>'sample_origin' as manifest_origin

        /* Extra variables from SCAN REDCap project are available in
        * shipping.fhir_encounter_details_v2. Roy from IDM has opted
        * to join these two views locally to keep things running fast.
        * See slack:
        * https://seattle-flu-study.slack.com/archives/CCAA9RBFS/p1585186959003300?thread_ts=1585186062.003200&cid=CCAA9RBFS
        *     - Jover, 25 March 2020
        */
    from
        warehouse.sample
        left join shipping.sample_with_best_available_encounter_data_v1 using (sample_id)
        left join warehouse.encounter using (encounter_id)
        left join warehouse.individual using (individual_id)
        left join shipping.age_bin_fine_v2 on age_bin_fine_v2.range @> age
        left join warehouse.primary_encounter_location using (encounter_id)
        left join warehouse.location using (location_id)
        left join hcov19_presence_absence_bbi using (sample_id)
        left join hcov19_presence_absence_uw using (sample_id)
    where
        /* Helen recently asked us to include all samples collected since 1 Jan,
         * 2020 for the NEJM paper.
         *
         * Note that when comparing some row-valued X, the expressions "X is
         * not null" and "X is distinct from null" behave differently.  We want
         * the latter.
         */
        (hcov19_presence_absence_bbi is distinct from null
         or hcov19_presence_absence_uw is distinct from null
         or best_available_encounter_date >= '2020-01-01')

        /* Exclude environmental swabs.
         *
         * Note that the standard index on details which supports containment
         * (@>) can't/won't be used by the planner because of both the negated
         * condition ("not … @>") and coupled "is null" check.  If this
         * condition is slow, we could directly index
         * details->>'sample_origin'.
         */
        and (sample.details is null
            or sample.details ->> 'sample_origin' is null
            or sample.details->>'sample_origin' != 'es')
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


create or replace view shipping.scan_encounters_v1 as

    select
        encounter_id,
        scan_study_arm,
        priority_code,

        encountered,
        to_char(encountered, 'IYYY-"W"IW') as encountered_week,
        illness_questionnaire_date,

        site.identifier as site,
        site.details ->> 'type' as site_type,

        individual.identifier as individual,
        individual.sex,

        age_in_years(age) as age,

        age_bin_fine_v2.range as age_range_fine,
        age_in_years(lower(age_bin_fine_v2.range)) as age_range_fine_lower,
        age_in_years(upper(age_bin_fine_v2.range)) as age_range_fine_upper,

        age_bin_coarse_v2.range as age_range_coarse,
        age_in_years(lower(age_bin_coarse_v2.range)) as age_range_coarse_lower,
        age_in_years(upper(age_bin_coarse_v2.range)) as age_range_coarse_upper,

        location.hierarchy -> 'puma' as puma,
        location.hierarchy -> 'tract' as census_tract,
        location.hierarchy -> 'neighborhood_district' as neighborhood_district,

        symptoms,
        symptom_onset,
        symptoms_2,
        symptom_onset_2,
        race,
        hispanic_or_latino,
        travel_countries,
        countries,
        travel_states,
        states,
        pregnant,
        income,
        housing_type,
        house_members,
        clinical_care,
        hospital_where,
        hospital_visit_type,
        hospital_arrive,
        hospital_leave,
        smoking,
        chronic_illness,
        overall_risk_health,
        overall_risk_setting,
        long_term_type,
        ace_inhibitor,
        website_id,
        prior_test,
        prior_test_positive,
        prior_test_positive_date,
        prior_test_type,
        prior_test_number,
        prior_test_result,
        contact,
        wash_hands,
        clean_surfaces,
        hide_cough,
        mask,
        distance,
        attend_event,
        wfh,
        industry,

        sample.identifier as sample,
        sample.details @> '{"note": "never-tested"}' as never_tested

    from warehouse.encounter
    join warehouse.site using (site_id)
    join warehouse.individual using (individual_id)
    left join warehouse.primary_encounter_location using (encounter_id)
    left join warehouse.location using (location_id)
    left join shipping.age_bin_fine_v2 on age_bin_fine_v2.range @> age
    left join shipping.age_bin_coarse_v2 on age_bin_coarse_v2.range @> age
    left join shipping.fhir_encounter_details_v2 using (encounter_id)
    left join warehouse.sample using (encounter_id)
    where site.identifier = 'SCAN'
    -- Filter out follow up encounters
    and not encounter.details @> '{"reason": [{"system": "http://snomed.info/sct", "code": "390906007"}]}'
;

comment on view shipping.scan_encounters_v1 is
  'A view of encounter data that are from the SCAN project';

revoke all
    on shipping.scan_encounters_v1
  from "incidence-modeler";

grant select
   on shipping.scan_encounters_v1
   to "incidence-modeler";


create or replace view shipping.scan_follow_up_encounters_v1 as

    with
        fu_illness as (
          select encounter_id,
                 boolean_response as fu_illness
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_illness'
        ),

        fu_change as (
          select encounter_id,
                 boolean_response as fu_change
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_change'
        ),

        fu_fever as (
          select encounter_id,
                 string_response[1] as fu_feelingFeverish
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_fever'
        ),

        fu_headache as (
          select encounter_id,
                 string_response[1] as fu_headaches
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_headache'
        ),

        fu_cough as (
          select encounter_id,
                 string_response[1] as fu_cough
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_cough'
        ),

        fu_chills as (
          select encounter_id,
                 string_response[1] as fu_chillsOrShivering
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_chills'
        ),

        fu_sweat as (
          select encounter_id,
                 string_response[1] as fu_sweats
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_sweat'
        ),

        fu_throat as (
          select encounter_id,
                 string_response[1] as fu_soreThroat
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_throat'
        ),

        fu_nausea as (
          select encounter_id,
                 string_response[1] as fu_nauseaOrVomiting
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_nausea'
        ),

        fu_nose as (
          select encounter_id,
                 string_response[1] as fu_runnyOrStuffyNose
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_nose'
        ),

        fu_tired as (
          select encounter_id,
                 string_response[1] as fu_fatigue
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_tired'
        ),

        fu_ache as (
          select encounter_id,
                 string_response[1] as fu_muscleOrBodyAches
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_ache'
        ),

        fu_breathe as (
          select encounter_id,
                 string_response[1] as fu_increasedTroubleBreathing
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_breathe'
        ),

        fu_diarrhea as (
          select encounter_id,
                 string_response[1] as fu_diarrhea
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_diarrhea'
        ),

        fu_rash as (
          select encounter_id,
                 string_response[1] as fu_rash
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_rash'
        ),

        fu_ear as (
          select encounter_id,
                 string_response[1] as fu_earPainOrDischarge
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_ear'
        ),

        fu_eye as (
          select encounter_id,
                 string_response[1] as fu_eyePain
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_eye'
        ),

        fu_smell_taste as (
          select encounter_id,
                 string_response[1] as fu_lossOfSmellOrTaste
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_smell_taste'
        ),

        fu_feel_normal as (
          select encounter_id,
                 boolean_response as fu_feel_normal
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_feel_normal'
        ),

        fu_symptom_duration as (
          select encounter_id,
                 date_response[1] as fu_symptom_onset
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_symptom_duration'
        ),

        fu_care as (
          select encounter_id,
                 string_response as fu_care
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_care'
        ),

        fu_date_care as (
          select encounter_id,
                 date_response[1] as fu_date_care
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_date_care'
        ),

        fu_hospital_where as (
          select encounter_id,
                 string_response[1] as fu_hospital_where
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_hospital_where'
        ),

        fu_hospital_ed as (
          select encounter_id,
                 string_response[1] as fu_hospital_ed
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_hospital_ed'
        ),

        fu_work_school as (
          select encounter_id,
                 string_response[1] as fu_work_school
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_work_school'
        ),

        fu_activities as (
          select encounter_id,
                 string_response[1] as fu_activities
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_activities'
        ),

        fu_which_activities as (
          select encounter_id,
                 string_response as fu_which_activities
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_which_activites'
        ),

        fu_missed_activities as (
          select encounter_id,
                 string_response as fu_missed_activities
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_missed_activites'
        ),

        fu_test_result as (
          select encounter_id,
                 string_response[1] as fu_test_result
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_test_result'
        ),

        result_changes as (
          select encounter_id,
                 boolean_response as result_changes
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'result_changes'
        ),

        fu_behaviors_no as (
          select encounter_id,
                 string_response as fu_behaviors_no
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_behaviors_no'
        ),

        fu_behaviors_inconclusive as (
          select encounter_id,
                 string_response as fu_behaviors_inconclusive
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_behaviors_inconclusive'
        ),

        fu_behaviors as (
          select encounter_id,
                 string_response as fu_behaviors
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_behaviors'
        ),

        fu_household_sick as (
          select encounter_id,
                 boolean_response as fu_household_sick
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_household_sick'
        ),

        fu_number_sick as (
          select encounter_id,
                 integer_response[1] as fu_number_sick
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_number_sick'
        ),

        fu_1_date as (
          select encounter_id,
                 date_response[1] as household_1_illness_onset
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_1_date'
        ),

        fu_1_symptoms as (
          select encounter_id,
                 string_response as household_1_symptoms
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_1_symptoms'
        ),

        fu_1_test as (
          select encounter_id,
                 string_response[1] as household_1_tested
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_1_test'
        ),

        fu_1_result as (
          select encounter_id,
                 string_response[1] as household_1_result
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_1_result'
        ),

        fu_2_date as (
          select encounter_id,
                 date_response[1] as household_2_illness_onset
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_2_date'
        ),

        fu_2_symptoms as (
          select encounter_id,
                 string_response as household_2_symptoms
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_2_symptoms'
        ),

        fu_2_test as (
          select encounter_id,
                 string_response[1] as household_2_tested
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_2_test'
        ),

        fu_2_result as (
          select encounter_id,
                 string_response[1] as household_2_result
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_2_result'
        ),

        fu_3_date as (
          select encounter_id,
                 date_response[1] as household_3_illness_onset
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_3_date'
        ),

        fu_3_symptoms as (
          select encounter_id,
                 string_response as household_3_symptoms
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_3_symptoms'
        ),

        fu_3_test as (
          select encounter_id,
                 string_response[1] as household_3_tested
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_3_test'
        ),

        fu_3_result as (
          select encounter_id,
                 string_response[1] as household_3_result
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_3_result'
        ),

        fu_4_date as (
          select encounter_id,
                 date_response[1] as household_4_illness_onset
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_4_date'
        ),

        fu_4_symptoms as (
          select encounter_id,
                 string_response as household_4_symptoms
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_4_symptoms'
        ),

        fu_4_test as (
          select encounter_id,
                 string_response[1] as household_4_tested
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_4_test'
        ),

        fu_4_result as (
          select encounter_id,
                 string_response[1] as household_4_result
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_4_result'
        ),

        fu_healthy_test as (
          select encounter_id,
                 string_response[1] as household_healthy_tested
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_healthy_test'
        ),

        fu_healthy_result as (
          select encounter_id,
                 string_response[1] as household_healthy_result
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'fu_healthy_result'
        )

    select
        initial.encounter_id as encounter_id,
        encounter.encounter_id as fu_encounter_id,
        encounter.encountered as fu_encountered,
        individual.identifier as fu_individual,

        fu_illness,
        fu_change,
        fu_feelingFeverish,
        fu_headaches,
        fu_cough,
        fu_chillsOrShivering,
        fu_sweats,
        fu_soreThroat,
        fu_nauseaOrVomiting,
        fu_runnyOrStuffyNose,
        fu_fatigue,
        fu_muscleOrBodyAches,
        fu_increasedTroubleBreathing,
        fu_diarrhea,
        fu_rash,
        fu_earPainOrDischarge,
        fu_eyePain,
        fu_lossOfSmellOrTaste,
        fu_feel_normal,
        fu_symptom_onset,
        fu_care,
        fu_date_care,
        fu_hospital_where,
        fu_hospital_ed,
        fu_work_school,
        fu_activities,
        fu_which_activities,
        fu_missed_activities,
        fu_test_result,
        result_changes,
        fu_behaviors_no,
        fu_behaviors_inconclusive,
        fu_behaviors,
        fu_household_sick,
        fu_number_sick,
        household_1_illness_onset,
        household_1_symptoms,
        household_1_tested,
        household_1_result,
        household_2_illness_onset,
        household_2_symptoms,
        household_2_tested,
        household_2_result,
        household_3_illness_onset,
        household_3_symptoms,
        household_3_tested,
        household_3_result,
        household_4_illness_onset,
        household_4_symptoms,
        household_4_tested,
        household_4_result,
        household_healthy_tested,
        household_healthy_result

      from warehouse.encounter
      join warehouse.site using (site_id)
      join warehouse.individual using (individual_id)
      left join fu_illness using (encounter_id)
      left join fu_change using (encounter_id)
      left join fu_fever using (encounter_id)
      left join fu_headache using (encounter_id)
      left join fu_cough using (encounter_id)
      left join fu_chills using (encounter_id)
      left join fu_sweat using (encounter_id)
      left join fu_throat using (encounter_id)
      left join fu_nausea using (encounter_id)
      left join fu_nose using (encounter_id)
      left join fu_tired using (encounter_id)
      left join fu_ache using (encounter_id)
      left join fu_breathe using (encounter_id)
      left join fu_diarrhea using (encounter_id)
      left join fu_rash using (encounter_id)
      left join fu_ear using (encounter_id)
      left join fu_eye using (encounter_id)
      left join fu_smell_taste using (encounter_id)
      left join fu_feel_normal using (encounter_id)
      left join fu_symptom_duration using (encounter_id)
      left join fu_care using (encounter_id)
      left join fu_date_care using (encounter_id)
      left join fu_hospital_where using (encounter_id)
      left join fu_hospital_ed using (encounter_id)
      left join fu_work_school using (encounter_id)
      left join fu_activities using (encounter_id)
      left join fu_which_activities using (encounter_id)
      left join fu_missed_activities using (encounter_id)
      left join fu_test_result using (encounter_id)
      left join result_changes using (encounter_id)
      left join fu_behaviors_no using (encounter_id)
      left join fu_behaviors_inconclusive using (encounter_id)
      left join fu_behaviors using (encounter_id)
      left join fu_household_sick using (encounter_id)
      left join fu_number_sick using (encounter_id)
      left join fu_1_date using (encounter_id)
      left join fu_1_symptoms using (encounter_id)
      left join fu_1_test using (encounter_id)
      left join fu_1_result using (encounter_id)
      left join fu_2_date using (encounter_id)
      left join fu_2_symptoms using (encounter_id)
      left join fu_2_test using (encounter_id)
      left join fu_2_result using (encounter_id)
      left join fu_3_date using (encounter_id)
      left join fu_3_symptoms using (encounter_id)
      left join fu_3_test using (encounter_id)
      left join fu_3_result using (encounter_id)
      left join fu_4_date using (encounter_id)
      left join fu_4_symptoms using (encounter_id)
      left join fu_4_test using (encounter_id)
      left join fu_4_result using (encounter_id)
      left join fu_healthy_test using (encounter_id)
      left join fu_healthy_result using (encounter_id)
      left join warehouse.encounter initial on initial.identifier = encounter.details ->> 'part_of'

    where site.identifier = 'SCAN'
    and encounter.details @> '{"reason": [{"system": "http://snomed.info/sct", "code": "390906007"}]}'
;

comment on view shipping.scan_follow_up_encounters_v1 is
  'A view of follow-up encounter date that are from the SCAN project';

revoke all
    on shipping.scan_follow_up_encounters_v1
  from "incidence-modeler";

grant select
    on shipping.scan_follow_up_encounters_v1
  to "incidence-modeler";


/******************** VIEWS FOR GENOMIC DATA ********************/
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

revoke all
    on shipping.genomic_sequences_for_augur_build_v1
  from "augur-build-exporter";

grant select
    on shipping.genomic_sequences_for_augur_build_v1
  to "augur-build-exporter";


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

revoke all
    on shipping.metadata_for_augur_build_v2
  from "augur-build-exporter";

grant select
    on shipping.metadata_for_augur_build_v2
  to "augur-build-exporter";


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

revoke all
    on shipping.metadata_for_augur_build_v3
  from "augur-build-exporter";

grant select
    on shipping.metadata_for_augur_build_v3
  to "augur-build-exporter";


/******************** VIEWS FOR REPORTING RESULTS ********************/
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
        sample_id.barcode as sample_barcode,
        collection_id.barcode as collection_barcode,
        sample.details ->> 'clia_barcode' as clia_barcode,
        site.identifier as site,
        presence_absence.details->>'reporting_log' as reporting_log,
        sample.details->'_provenance'->>'workbook' as workbook,
        sample.details->'_provenance'->>'sheet' as sheet,
        sample.details->>'sample_origin' as sample_origin,
        sample.details->>'swab_site' as swab_site,
        encounter.details ->> 'language' as language,
        age_in_years(encounter.age) as age,
        case
          when present then 'positive'
          when present is null then 'inconclusive'
        end as result

    from warehouse.presence_absence
    join warehouse.target using (target_id)
    join warehouse.organism using (organism_id)
    join warehouse.sample using (sample_id)
    join warehouse.identifier sample_id on (sample.identifier = cast(sample_id.uuid as text))
    join warehouse.identifier collection_id on (sample.collection_identifier = cast(collection_id.uuid as text))
    join warehouse.identifier_set collection_id_set on (collection_id.identifier_set_id = collection_id_set.identifier_set_id)
    left join warehouse.encounter using (encounter_id)
    left join warehouse.site using (site_id)

    where organism.lineage <@ (table reportable)
    and (present or present is null)
     -- Only report on SCAN samples and SFS prospective samples
    -- We don't have to worry about SFS consent date because the
    -- clinical team checks this before they contact the participant.
    and collection_id_set.name in ('collections-scan',
                                   'collections-scan-kiosks',
                                   'collections-household-observation',
                                   'collections-household-intervention',
                                   'collections-swab&send',
                                   'collections-kiosks',
                                   'collections-self-test',
                                   'collections-swab&send-asymptomatic',
                                   'collections-kiosks-asymptomatic',
                                   'collections-environmental')
    and coalesce(encountered::date, date_or_null(sample.details ->> 'date')) >= '2020-01-01'
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

grant select
   on shipping.reportable_condition_v1
   to "reportable-condition-notifier";


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


create or replace view shipping.scan_return_results_v1 as

    with hcov19_presence_absence as (
        -- Collapse potentially multiple hCoV-19 results
        select distinct on (sample_id)
            sample_id,
            presence_absence_id,
            pa.present as hcov19_present,
            pa.modified::date as result_ts
        from
            warehouse.presence_absence as pa
            join warehouse.target using (target_id)
            join warehouse.organism using (organism_id)
        where
            organism.lineage <@ 'Human_coronavirus.2019'
            and pa.details @> '{"assay_type": "Clia"}'
            and not control
            -- We shouldn't be receiving these results from Samplify, but they
            -- sometimes sneak in. Be sure to block them from this view so as
            -- to not return inaccurate results to participants.
            and target.identifier not in ('COVID-19_Orf1b', 'COVID-19-S_gene')
        /*
          Keep only the most recent push. According to Lea, samples are only
          retested if there is a failed result. A positive, negative, or
          indeterminate result would not be retested.

          https://seattle-flu-study.slack.com/archives/CV1E2BC8N/p1584570226450500?thread_ts=1584569401.449800&cid=CV1E2BC8N
        */
        order by
            sample_id, presence_absence_id desc
    ),

    scan_samples as (
      select
        sample_id,
        barcode as qrcode,
        encountered::date as collect_ts,
        sample.details @> '{"note": "never-tested"}' as never_tested,
        sample.details ->> 'swab_type' as swab_type,
        -- The identifier set of the sample's collection identifier determines
        -- if the sample was collected under staff observation.
        -- In-person kiosk enrollment samples are collected under staff
        -- observation while mail-in samples are done without staff observation.
        --  Jover, 22 July 2020
        case identifier_set.name
          when 'collections-scan-kiosks' then true
          when 'collections-scan' then false
          else null
        end as staff_observed

      from
        warehouse.identifier
        join warehouse.identifier_set using (identifier_set_id)
        left join warehouse.sample on uuid::text = sample.collection_identifier
        left join warehouse.encounter using (encounter_id)
      where
        identifier_set.name in('collections-scan', 'collections-scan-kiosks')
        -- Add a date cutoff so that we only return results to participants
        -- that are in the SCAN research study (launch date: 2020-06-10)
        and encountered >= '2020-06-10 00:00:00 US/Pacific'
      order by encountered, barcode
    )

    select
        qrcode,
        collect_ts,
        case
            when sample_id is null then 'not-received'
            when never_tested then 'never-tested'
            when sample_id is not null and presence_absence_id is null then 'pending'
            when hcov19_present is true then 'positive'
            when hcov19_present is false then 'negative'
            when presence_absence_id is not null and hcov19_present is null then 'inconclusive'
        end as status_code,
        result_ts,
        swab_type,
        staff_observed
    from
      scan_samples
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


/******************** VIEWS FOR POWER BI DASHBOARDS ********************/
create or replace view shipping.scan_demographics_v1 as

    select
        encountered_week,
        sex,
        age_range_fine,
        age_range_fine_lower,
        age_range_fine_upper,
        race,
        hispanic_or_latino,
        income,
        puma
    from shipping.scan_encounters_v1
;

comment on view shipping.scan_demographics_v1 is
  'A view of basic demographic data from the SCAN project for Power BI dashboards.';

revoke all
    on shipping.scan_demographics_v1
  from "scan-dashboard-exporter";

grant select
    on shipping.scan_demographics_v1
    to "scan-dashboard-exporter";


create or replace view shipping.scan_hcov19_positives_v1 as

    with hcov19_presence_absence as (
        -- Collapse potentially multiple hCoV-19 results
        select distinct on (sample_id)
            sample.identifier as sample,
            presence_absence_id,
            pa.present as hcov19_present,
            pa.created::date as hcov19_result_release_date
        from
            warehouse.presence_absence as pa
            join warehouse.target using (target_id)
            join warehouse.organism using (organism_id)
            join warehouse.sample using (sample_id)
        where
            organism.lineage <@ 'Human_coronavirus.2019'
            and pa.details @> '{"assay_type": "Clia"}'
            and not control
            -- We shouldn't be receiving these results from Samplify, but they
            -- sometimes sneak in. Be sure to block them from this view so as
            -- to not return inaccurate results to participants.
            and target.identifier not in ('COVID-19_Orf1b', 'COVID-19-S_gene')
        /*
          Keep only the most recent push. According to Lea, samples are only
          retested if there is a failed result. A positive, negative, or
          indeterminate result would not be retested.

          https://seattle-flu-study.slack.com/archives/CV1E2BC8N/p1584570226450500?thread_ts=1584569401.449800&cid=CV1E2BC8N
        */
        order by
            sample_id, presence_absence_id desc
    ),

    scan_hcov19_results as (
      select
          case
              when hcov19_present is true then 'positive'
              when hcov19_present is false then 'negative'
              when hcov19_present is null then 'inconclusive'
          end as hcov19_result,
          hcov19_result_release_date,
          case
              when puma in ('5310901', '5310902') then 'yakima'
              when puma in ('5311701', '5311702', '5311703',
                            '5311704', '5311705', '5311706') then 'snohomish'
              when puma in ('5311601', '5311602', '5311603', '5311604',
                            '5311605', '5311606', '5311607', '5311608',
                            '5311609', '5311610', '5311611', '5311612',
                            '5311613', '5311614', '5311615', '5311616') then 'king'
              else null
          end as county

      from shipping.scan_encounters_v1
      join hcov19_presence_absence using (sample)

    )

    select
        hcov19_result_release_date,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive')) as total_hcov19_positives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county = 'king') as king_county_positives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county = 'snohomish') as snohomish_county_positives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county = 'yakima') as yakima_county_positives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county is null) as other_positives
    from scan_hcov19_results
    group by hcov19_result_release_date
;

comment on view shipping.scan_hcov19_positives_v1 is
  'A view of counts of hcov19 positives from the SCAN project grouped by date results were released.';

-- Even if it's just aggregate counts of hcov19 positives,
-- we should probably restrict access to this view to only hcov19-visibility
-- and scan-dashboard-exporter.
--  -Jover, 9 July 2020
revoke all on shipping.scan_return_results_v1 from reporter;

grant select
    on shipping.scan_return_results_v1
    to "hcov19-visibility";

revoke all
    on shipping.scan_hcov19_positives_v1
  from "scan-dashboard-exporter";

grant select
    on shipping.scan_hcov19_positives_v1
    to "scan-dashboard-exporter";


create or replace view shipping.seattle_neighborhood_districts_v1 as

    select
        identifier,
        st_assvg(point, rel=>1, maxdecimaldigits=>5) as centroid,
        st_assvg(polygon, rel=>1, maxdecimaldigits=>5) as svg_path
    from warehouse.location
    where scale = 'neighborhood_district'
    and hierarchy -> 'city' = 'seattle'
;

comment on view shipping.seattle_neighborhood_districts_v1 is
  'A view of Seattle neighborhood district polygons as SVG paths';

revoke all
    on shipping.seattle_neighborhood_districts_v1
  from "scan-dashboard-exporter";

grant select
    on shipping.seattle_neighborhood_districts_v1
    to "scan-dashboard-exporter";


create or replace view shipping.scan_enrollments_v1 as

    select
        illness_questionnaire_date,
        scan_study_arm,
        priority_code,
        puma,
        neighborhood_district,
        sample is not null or never_tested is not null as kit_received

    from shipping.scan_encounters_v1
;

comment on view shipping.scan_enrollments_v1 is
  'A view of enrollment data from the SCAN project for Power BI dashboards';

revoke all
    on shipping.scan_enrollments_v1
  from "scan-dashboard-exporter";

grant select
    on shipping.scan_enrollments_v1
    to "scan-dashboard-exporter";

commit;
