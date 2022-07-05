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
drop view if exists shipping.scan_redcap_enrollments_v1;
drop view if exists shipping.scan_enrollments_v1;
drop view if exists shipping.scan_hcov19_result_counts_v1;
drop view if exists shipping.scan_hcov19_result_counts_v2;
drop view if exists shipping.scan_demographics_v2;
drop view if exists shipping.scan_demographics_v1;

drop view if exists shipping.phskc_encounter_details_v1;

drop view if exists shipping.uw_priority_queue_v1;
drop view if exists shipping.__uw_priority_queue_v1;
drop materialized view if exists shipping.__uw_encounters;
drop view if exists shipping.uw_reopening_ehs_reporting_v1;
drop view if exists shipping.uw_reopening_encounters_v1;
drop view if exists shipping.uw_reopening_enrollment_fhir_encounter_details_v1;

drop view if exists shipping.scan_return_results_v1;
drop view if exists shipping.return_results_v3;
drop view if exists shipping.return_results_v2;
drop view if exists shipping.return_results_v1;
drop view if exists shipping.reportable_condition_v1;

drop view if exists shipping.metadata_for_augur_build_v3;
drop view if exists shipping.metadata_for_augur_build_v2;
drop view if exists shipping.genomic_sequences_for_augur_build_v1;
drop view if exists shipping.genome_submission_metadata_v1;
drop view if exists shipping.flu_assembly_jobs_v1;

drop view if exists shipping.scan_follow_up_encounters_v1;
drop view if exists shipping.scan_encounters_with_best_available_vaccination_data_v1;
drop materialized view if exists shipping.scan_encounters_v1;

drop view if exists shipping.observation_with_presence_absence_result_v3;
drop view if exists shipping.observation_with_presence_absence_result_v2;
drop view if exists shipping.observation_with_presence_absence_result_v1;

drop view if exists shipping.incidence_model_observation_v4;
drop view if exists shipping.incidence_model_observation_v3;
drop view if exists shipping.incidence_model_observation_v2;
drop view if exists shipping.incidence_model_observation_v1;

drop view if exists shipping.linelist_data_for_wa_doh_v1;
drop view if exists shipping.linelist_data_for_wa_doh_v2;

drop view if exists shipping.hcov19_observation_v1;
drop view if exists shipping.hcov19_presence_absence_result_v1;

drop view if exists shipping.fhir_encounter_details_v2;
drop view if exists shipping.fhir_encounter_details_v1;
drop materialized view if exists shipping.fhir_questionnaire_responses_v1;

drop view if exists shipping.sample_with_best_available_encounter_data_v1;


/******************** VIEWS FOR INTERNAL USE ********************/
create or replace view shipping.sample_with_best_available_encounter_data_v1 as

    with site_details as (
      select site_1.site_id,
        site_1.identifier as site,
        site_1.details ->> 'type'::text as site_type,
        site_1.details ->> 'category'::text as site_category,
        coalesce(site_1.details ->> 'swab_site'::text, site_1.details ->> 'sample_origin'::text) as manifest_regex
      from warehouse.site site_1
    ),
    samples_with_manifest_data as (
      select sample_1.sample_id,
        sample_1.identifier as sample,
        encounter.site_id,
        date_or_null(sample_1.details ->> 'date'::text) as collection_date,
        coalesce(encounter.encountered::date, date_or_null(sample_1.details ->> 'date'::text)) as best_available_encounter_date,
        coalesce(
            case
                when btrim(sample_1.details ->> 'sample_origin'::text, ' '::text) = 'es'::text then 'environmental'::text
                else btrim(sample_1.details ->> 'swab_site'::text, ' '::text)
            end, btrim(sample_1.details ->> 'sample_origin'::text, ' '::text)) as site_manifest_details,
        encounter.site_id is not null as has_encounter_data
      from warehouse.sample sample_1
        left join warehouse.encounter using (encounter_id)
    ),
    samples_with_site_match as (
      select samples_with_manifest_data.sample_id,
        samples_with_manifest_data.sample,
        samples_with_manifest_data.has_encounter_data,
        samples_with_manifest_data.best_available_encounter_date,
        samples_with_manifest_data.site_manifest_details,
        case
            when samples_with_manifest_data.best_available_encounter_date < '2019-10-01'::date then 'Y1'::text
            when samples_with_manifest_data.best_available_encounter_date < '2020-11-01'::date then 'Y2'::text
            when samples_with_manifest_data.best_available_encounter_date < '2021-11-01'::date then 'Y3'::text
            when samples_with_manifest_data.best_available_encounter_date < '2022-11-01'::date then 'Y4'::text
            else null::text
        end as season,
        site.site_id as best_available_site_id,
        site.identifier as best_available_site,
        site.details ->> 'type'::text as best_available_site_type,
        site.details ->> 'category'::text as best_available_site_category
      from samples_with_manifest_data
        left join warehouse.site on samples_with_manifest_data.site_id = site.site_id
    ),
    samples_with_site_match_like as (
      select sample_id,
        sample,
        has_encounter_data,
        best_available_encounter_date,
        site_manifest_details,
        season,
        coalesce(best_available_site_id, site_details.site_id) as best_available_site_id,
        coalesce(best_available_site, site_details.site) as best_available_site,
        coalesce(best_available_site_type, site_details.site_type) as best_available_site_type,
        coalesce(best_available_site_category, site_details.site_category) as best_available_site_category
      from samples_with_site_match
        left join site_details on lower(samples_with_site_match.site_manifest_details) ~~ site_details.manifest_regex
          and best_available_site_id is null),
    samples_with_site_match_like_regex as (
      select sample_id,
        sample,
        has_encounter_data,
        best_available_encounter_date,
        season,
        coalesce(best_available_site_id, site_details.site_id) as best_available_site_id,
        coalesce(best_available_site, site_details.site) as best_available_site,
        coalesce(best_available_site_type, site_details.site_type) as best_available_site_type,
        coalesce(best_available_site_category, site_details.site_category) as best_available_site_category
      from samples_with_site_match_like
        left join site_details on lower(samples_with_site_match_like.site_manifest_details) ~ similar_escape(site_details.manifest_regex, null::text)
          and best_available_site_id is null)
    select sample_id,
      sample,
      has_encounter_data,
      best_available_encounter_date,
      season,
      best_available_site_id,
      best_available_site,
      best_available_site_type,
      best_available_site_category
    from samples_with_site_match_like_regex;

comment on view shipping.sample_with_best_available_encounter_data_v1 is
    'Version 1 of view of warehoused samples and their best available encounter date and site details important for metrics calculations';

revoke all
    on shipping.sample_with_best_available_encounter_data_v1
  from "return-results-exporter";

grant select
   on shipping.sample_with_best_available_encounter_data_v1
   to "return-results-exporter";


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
           array_remove(array_agg("valueDecimal" order by "valueDecimal"), null) as numeric_response,
           array_remove(array_agg("code" order by "code"), null) as code_response
      from warehouse.encounter,
           jsonb_to_recordset(details -> 'QuestionnaireResponse') as q("item" jsonb),
           jsonb_to_recordset("item") as response("linkId" text, "answer" jsonb),
           jsonb_to_recordset("answer") as answer("valueString" text,
                                                  "valueBoolean" bool,
                                                  "valueDate" text,
                                                  "valueInteger" integer,
                                                  "valueDecimal" numeric,
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

        vaccine_month as (
          select encounter_id,
                 string_response[1] as vaccine_month
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vaccine_month'
        ),

        vaccine_year as (
          select encounter_id,
                 string_response[1] as vaccine_year
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vaccine_year'
        ),

        vaccine_doses_child as (
          select encounter_id,
                 string_response[1] as vaccine_doses_child
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vaccine_doses_child'
        ),

        vaccine_doses as (
          select encounter_id,
                 string_response[1] as vaccine_doses
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vaccine_doses'
        ),

        novax_hh as (
          select encounter_id,
                 string_response[1] as novax_hh
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'novax_hh'
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
        ),

        yakima as (
          select encounter_id,
                 integer_response[1] as yakima
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'yakima'
        ),

        pierce as (
          select encounter_id,
                 integer_response[1] as pierce
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'pierce'
        ),

        attend_event_jan2021 as (
          select encounter_id,
                 string_response[1] as attend_event_jan2021
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'attend_event_jan2021'
        ),

        indoor_facility as (
          select encounter_id,
                 string_response[1] as indoor_facility
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'indoor_facility'
        ),

        social_precautions as (
          select encounter_id,
                 string_response as social_precautions
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'social_precautions'
        ),

        no_mask as (
          select encounter_id,
                 string_response as no_mask
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'no_mask'
        ),

        high_risk_feb2021 as (
          select encounter_id,
                 string_response as high_risk_feb2021
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'high_risk_feb2021'
        ),

        overall_risk_jan2021 as (
          select encounter_id,
                 string_response as overall_risk_jan2021
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'overall_risk_jan2021'
        ),

        covid_vax as (
          select encounter_id,
                 string_response[1] as covid_vax
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'covid_vax'
        ),

        covid_doses as (
          select encounter_id,
                 string_response[1] as covid_doses
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'covid_doses'
        ),

        vac_name_1 as (
          select encounter_id,
                 string_response[1] as vac_name_1
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vac_name_1'
        ),

        vac_date as (
          select encounter_id,
                 date_response[1] as vac_date
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vac_date'
        ),

        vac_name_2 as (
          select encounter_id,
                 string_response[1] as vac_name_2
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vac_name_2'
        ),

        vac_date_2 as (
          select encounter_id,
                 date_response[1] as vac_date_2
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vac_date_2'
        ),

        why_participating as (
          select encounter_id,
               string_response as why_participating
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'why_participating'
        ),

        who_completing_survey as (
          select encounter_id,
                 string_response as who_completing_survey
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'who_completing_survey'
        ),

        contact_symptomatic as (
          select encounter_id,
                 string_response[1] as contact_symptomatic
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'contact_symptomatic'
        ),

        contact_vax as (
          select encounter_id,
                 string_response[1] as contact_vax
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'contact_vax'
        ),

        contact_symp_negative as (
          select encounter_id,
                 string_response[1] as contact_symp_negative
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'contact_symp_negative'
        ),

        vac_name_3 as (
          select encounter_id,
                 string_response[1] as vac_name_3
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vac_name_3'
        ),

        vac_date_3 as (
          select encounter_id,
                 date_response[1] as vac_date_3
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'vac_date_3'
        ),

        no_covid_vax_hh as (
          select encounter_id,
                 string_response[1] as no_covid_vax_hh
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'no_covid_vax_hh'
        ),

        gender_identity as (
          select encounter_id,
                 string_response[1] as gender_identity
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'gender_identity'
        ),

        education as (
          select encounter_id,
                 string_response[1] as education
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'education'
        ),

        hh_under_5 as (
          select encounter_id,
                 boolean_response as hh_under_5
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'hh_under_5'
        ),

        hh_5_to_12 as (
          select encounter_id,
                 boolean_response as hh_5_to_12
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'hh_5_to_12'
        ),

        overall_risk_oct2021 as (
          select encounter_id,
                 string_response as overall_risk_oct2021
            from shipping.fhir_questionnaire_responses_v1
          where link_id = 'overall_risk_oct2021'
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
        vaccine_month,
        vaccine_year,
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
        illness_questionnaire_date,
        yakima,
        pierce,
        attend_event_jan2021,
        indoor_facility,
        social_precautions,
        no_mask,
        high_risk_feb2021,
        overall_risk_jan2021,
        covid_vax,
        covid_doses,
        vac_name_1,
        vac_date,
        vac_name_2,
        vac_date_2,
        why_participating,
        who_completing_survey,
        contact_symptomatic,
        contact_vax,
        contact_symp_negative,
        vaccine_doses_child,
        vaccine_doses,
        novax_hh,
        vac_name_3,
        vac_date_3,
        no_covid_vax_hh,
        gender_identity,
        education,
        hh_under_5,
        hh_5_to_12,
        overall_risk_oct2021

      from warehouse.encounter
      left join scan_study_arm using (encounter_id)
      left join priority_code using (encounter_id)
      left join symptoms using (encounter_id)
      left join symptoms_2 using (encounter_id)
      left join vaccine using (encounter_id)
      left join vaccine_month using (encounter_id)
      left join vaccine_year using (encounter_id)
      left join vaccine_doses_child using (encounter_id)
      left join vaccine_doses using (encounter_id)
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
      left join yakima using (encounter_id)
      left join pierce using (encounter_id)
      left join attend_event_jan2021 using (encounter_id)
      left join indoor_facility using (encounter_id)
      left join social_precautions using (encounter_id)
      left join no_mask using (encounter_id)
      left join high_risk_feb2021 using (encounter_id)
      left join overall_risk_jan2021 using (encounter_id)
      left join covid_vax using (encounter_id)
      left join covid_doses using (encounter_id)
      left join vac_name_1 using (encounter_id)
      left join vac_date using (encounter_id)
      left join vac_name_2 using (encounter_id)
      left join vac_date_2 using (encounter_id)
      left join why_participating using (encounter_id)
      left join who_completing_survey using (encounter_id)
      left join contact_symptomatic using (encounter_id)
      left join contact_vax using (encounter_id)
      left join contact_symp_negative using (encounter_id)
      left join novax_hh using (encounter_id)
      left join vac_name_3 using (encounter_id)
      left join vac_date_3 using (encounter_id)
      left join no_covid_vax_hh using (encounter_id)
      left join gender_identity using (encounter_id)
      left join education using (encounter_id)
      left join hh_under_5 using (encounter_id)
      left join hh_5_to_12 using (encounter_id)
      left join overall_risk_oct2021 using (encounter_id);

comment on view shipping.fhir_encounter_details_v2 is
  'A v2 view of encounter details that are in FHIR format that includes all SCAN questionnaire answers';

revoke all
    on shipping.fhir_encounter_details_v2
  from "incidence-modeler";

grant select
   on shipping.fhir_encounter_details_v2
   to "incidence-modeler";


create or replace view shipping.phskc_encounter_details_v1 as

    with
        phskc_encounters as (
            select encounter_id,
                age,
                sex
            from warehouse.encounter
              left join warehouse.individual using (individual_id)
            where site_id = 569
            group by encounter_id, sex
        ),

        encounter_metadata as (
            select encounter_id,
                max (code_response[1]) filter ( where link_id = 'race' ) as race,
                max (string_response[1]) filter ( where link_id = 'survey_testing_because_exposed' ) as survey_testing_because_exposed,
                max (string_response[1]) filter ( where link_id = 'if_symptoms_how_long' ) as if_symptoms_how_long,
                max (string_response[1]) filter ( where link_id = 'vaccine_status' ) as vaccine_status,
                bool_or (boolean_response) filter ( where link_id = 'ethnicity' ) as hispanic_or_latino,
                bool_or (boolean_response) filter ( where link_id = 'inferred_symptomatic' ) as inferred_symptomatic,
                bool_or (boolean_response) filter ( where link_id = 'survey_have_symptoms_now' ) as survey_have_symptoms_now
            from shipping.fhir_questionnaire_responses_v1
            group by encounter_id
        ),

        location_info as (
            select encounter_id,
                hierarchy as loc_data
            from warehouse.encounter
              left join warehouse.encounter_location using (encounter_id)
              left join warehouse.location using (location_id)
        ),

        pa_result as (
            select encounter_id,
                present as present
            from warehouse.encounter
              left join warehouse.sample using (encounter_id)
              left join warehouse.presence_absence as pa using (sample_id)
            where target_id = 952
            group by encounter_id, present
        )

    select
        encounter_id,
        age_in_years(age) as age,
        sex,
        race,
        hispanic_or_latino,
        inferred_symptomatic,
        survey_testing_because_exposed,
        survey_have_symptoms_now,
        if_symptoms_how_long,
        vaccine_status,
        loc_data -> 'puma' as puma_code,
        loc_data -> 'tract' as census_tract,
        present as virology_result_hcov19_present
      from phskc_encounters
        left join encounter_metadata using (encounter_id)
        left join location_info using (encounter_id)
        left join pa_result using (encounter_id)
      order by encounter_id;

comment on view shipping.phskc_encounter_details_v1 is
  'Shipping view for encounter details of PHSKC Retrospective samples';


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


create or replace view shipping.incidence_model_observation_v4 as

    select encounter.identifier as encounter,

           to_char((encountered at time zone 'US/Pacific')::date, 'IYYY-"W"IW') as encountered_week,

           site.identifier as site,
           site.details->>'type' as site_type,

           individual.identifier as individual,
           individual.sex,

           age_bin_fine_v2.range as age_range_fine,
           age_in_years(lower(age_bin_fine_v2.range)) as age_range_fine_lower,
           age_in_years(upper(age_bin_fine_v2.range)) as age_range_fine_upper,

           age_bin_coarse_v2.range as age_range_coarse,
           age_in_years(lower(age_bin_coarse_v2.range)) as age_range_coarse_lower,
           age_in_years(upper(age_bin_coarse_v2.range)) as age_range_coarse_upper,

           address_identifier,
           residence_census_tract,
           residence_puma,

           coalesce(encounter_responses.flu_shot, fhir.vaccine) as flu_shot,
           coalesce(encounter_responses.symptoms, fhir.symptoms) as symptoms,

           sample.details->>'sample_origin' as manifest_origin,
           sample.details->>'swab_type' as swab_type,
           sample.details->>'collection_matrix' as collection_matrix,

           sample.identifier as sample

      from warehouse.sample
      left join warehouse.encounter using (encounter_id)
      left join warehouse.individual using (individual_id)
      left join warehouse.site using (site_id)
      left join shipping.age_bin_fine_v2 on age_bin_fine_v2.range @> age
      left join shipping.age_bin_coarse_v2 on age_bin_coarse_v2.range @> age
      left join shipping.fhir_encounter_details_v1 as fhir using (encounter_id)
      left join (
          select
            encounter_id,
            location.identifier as address_identifier,
            hierarchy->'tract' as residence_census_tract,
            hierarchy->'puma' as residence_puma
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

     order by encountered nulls last;

comment on view shipping.incidence_model_observation_v4 is
    'Version 4 of view of warehoused samples and important questionnaire responses for modeling and viz teams';

revoke all
    on shipping.incidence_model_observation_v4
  from "incidence-modeler";

grant select
   on shipping.incidence_model_observation_v4
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


create or replace view shipping.hcov19_presence_absence_result_v1 as

    -- Collapse potentially multiple hCoV-19 results
    select distinct on (sample_id)
        sample_id,
        presence_absence_id,
        present,
        -- On 6/8/2022, we started using the more accurate timestamp received from Samplify
        -- as the result date (result_ts). Prior to this, the presence_absence.modified date
        -- was used. Because this column is used in return of results, a date cutoff is being
        -- applied to avoid changing records that were processed before to this date. --drr
        case
          when pa.details ? 'result_timestamp' and pa.details ->> 'result_timestamp' > '2022-06-09' then
            (pa.details ->> 'result_timestamp')::date
          else
            pa.modified::date
        end as result_ts,
        pa.created::date as hcov19_result_release_date,
        pa.details as details,
        target.identifier as target,
        organism.lineage as organism
    from
        warehouse.presence_absence as pa
        join warehouse.target using (target_id)
        join warehouse.organism using (organism_id)
    where
        organism.lineage <@ 'Human_coronavirus.2019'
        -- We generally don't want to run queries with UW Retrospective test
        -- results. If we do, we can always write a query from scratch.
        -- Filtering out UW Retrospective (or potentially other clinical) test
        -- results now will simplify our dependent queries and make them
        -- consistent since here we're arbitrarily choosing the latest received
        -- sample when deduplicating samples with multiple hcov19 tests.
        and not pa.details @> '{"device": "clinical"}'
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
        sample_id,
        case pa.details ->> 'assay_type'
          when 'Clia' then 1
        end nulls last,
        presence_absence_id desc
;

comment on view shipping.hcov19_presence_absence_result_v1 is
  'Custom view of hCoV-19 samples with non-clinical presence-absence results';


create or replace view shipping.hcov19_observation_v1 as
    -- TODO figure out if it's possible to refactor the nested query inside this
    -- CTE to query shipping.hcov19_presence_absence_result_v1. I could not
    -- figure out how to do it without dropping some rows.  -- kfay
    with hcov19_presence_absence_bbi as (
        select
          sample_id,
          hcov19_result_received_bbi,
          hcov19_present_bbi,
          array_agg("crt") as crt_values,
          hcov19_device_bbi,
          hcov19_assay_type_bbi
        from (
            -- Collapse potentially multiple hCoV-19 results
            select distinct on (sample_id)
                sample_id,
                pa.created::date as hcov19_result_received_bbi,
                pa.present as hcov19_present_bbi,
                pa.details -> 'replicates' as replicates,
                pa.details -> 'device' as hcov19_device_bbi,
                pa.details -> 'assay_type' as hcov19_assay_type_bbi
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

        group by sample_id, hcov19_result_received_bbi, hcov19_present_bbi, hcov19_device_bbi, hcov19_assay_type_bbi
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
        hcov19_device_bbi,
        hcov19_assay_type_bbi,
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

        -- Sample detail columns
        sample.details->>'sample_origin' as manifest_origin,
        sample.details->>'swab_type' as swab_type,
        sample.details->>'collection_matrix' as collection_matrix

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

comment on view shipping.hcov19_observation_v1 is
  'Custom view of hCoV-19 samples with presence-absence results and best available encounter data';


create or replace view shipping.observation_with_presence_absence_result_v3 as
    with hcov19_pa as (
      select
        sample.identifier as sample,
        target,
        organism,
        present,
        present::int as presence,
        pa.details as details
      from shipping.hcov19_presence_absence_result_v1 as pa
      join warehouse.sample using (sample_id)
    )

    select
        target,
        present,
        present::int as presence,
        pa.details ->> 'device' as device,
        pa.details ->> 'assay_type' as assay_type,
        observation.*,
        organism
      from
        -- Combine our hCoV-19 and other presence-absence results into one table
        -- via a union.
        shipping.incidence_model_observation_v4 as observation
        left join (
          select
            sample,
            target,
            organism,
            present,
            present::int as presence,
            details
          from shipping.presence_absence_result_v2
            union
          select *
            from hcov19_pa
        ) as pa using (sample)
      where
        not pa.details @> '{"device": "clinical"}'
      order by site_type, encounter, sample, target;

comment on view shipping.observation_with_presence_absence_result_v3 is
  'Joined view of shipping.incidence_model_observation_v4, shipping.presence_absence_result_v2, and shipping.hcov19_observation_v1';


create materialized view shipping.scan_encounters_v1 as

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

        age_bin_decade_v1.range as age_range_decade,
        age_in_years(lower(age_bin_decade_v1.range)) as age_range_decade_lower,
        age_in_years(upper(age_bin_decade_v1.range)) as age_range_decade_upper,

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
        yakima,
        pierce,
        attend_event_jan2021,
        indoor_facility,
        social_precautions,
        no_mask,
        high_risk_feb2021,
        overall_risk_jan2021,
        covid_vax,
        covid_doses,
        vac_name_1,
        vac_date,
        vac_name_2,
        vac_date_2,
        case
          when covid_vax = 'no' then 'not_vaccinated'
          when covid_vax in (values('dont_know'), ('dont_say')) then 'unknown'
          when covid_vax = 'yes' then
            case
              when (vac_name_3 in (values('pfizer'), ('moderna')) and date_part('day', encountered::timestamp - vac_date_3::timestamp) >= 14)
                or (vac_name_1 = 'johnson' and vac_name_2 in (values('pfizer'), ('moderna'), ('johnson')) and date_part('day', encountered::timestamp - vac_date_2::timestamp) >= 14) then 'boosted'
              when vac_name_1 = 'johnson' and date_part('day', encountered::timestamp - vac_date::timestamp) >= 14 then 'fully_vaccinated'
              when vac_name_1 in (values('pfizer'), ('moderna')) and vac_name_2 in (values('pfizer'), ('moderna')) and date_part('day', encountered::timestamp - vac_date_2::timestamp) >= 14 then 'fully_vaccinated'
              when vac_name_1 in (values('pfizer'), ('moderna'), ('johnson')) and date_part('day', encountered::timestamp - vac_date::timestamp) >= 1 then 'partially_vaccinated'
              when date_part('day', encountered::timestamp - vac_date::timestamp) = 0 then 'not_vaccinated'
              else 'unknown'
            end
          else 'unknown'
        end as vaccination_status,
        case
          when vac_name_3 is not null then
            case
              when (vac_name_1 = vac_name_2 and vac_name_1 = vac_name_3) then vac_name_3
              else 'multiple'
            end
          when vac_name_2 is not null then
            case
              when vac_name_1 = vac_name_2 then vac_name_2
              else 'multiple'
            end
          else vac_name_1
        end as vaccine_manufacturer,
        case
          when covid_doses = '1' then '1_dose'
          when covid_doses = '2' then '2_doses'
          when covid_doses = '3' then '3_doses'
          when covid_vax = 'no' then '0_doses'
          else 'unknown'
        end as number_of_covid_doses,
        coalesce(vac_date_3, vac_date_2, vac_date) as date_last_covid_dose,
        why_participating,
        who_completing_survey,
        contact_symptomatic,
        contact_vax,
        contact_symp_negative,
        novax_hh,
        vac_name_3,
        vac_date_3,
        no_covid_vax_hh,
        gender_identity,
        education,
        hh_under_5,
        hh_5_to_12,
        overall_risk_oct2021,
        vaccine as flu_vaccine,
        vaccine_month as flu_vaccine_month,
        vaccine_year as flu_vaccine_year,
        vaccine_doses as flu_vaccine_doses,
        vaccine_doses_child as flu_vaccine_doses_child,

        sample.sample_id,
        sample.identifier as sample,
        sample.details @> '{"note": "never-tested"}' as never_tested

    from warehouse.encounter
    join warehouse.site using (site_id)
    join warehouse.individual using (individual_id)
    left join warehouse.primary_encounter_location using (encounter_id)
    left join warehouse.location using (location_id)
    left join shipping.age_bin_fine_v2 on age_bin_fine_v2.range @> age
    left join shipping.age_bin_coarse_v2 on age_bin_coarse_v2.range @> age
    left join shipping.age_bin_decade_v1 on age_bin_decade_v1.range @> age
    left join shipping.fhir_encounter_details_v2 using (encounter_id)
    left join warehouse.sample using (encounter_id)
    where site.identifier = 'SCAN'
    -- Filter out follow up encounters
    and not encounter.details @> '{"reason": [{"system": "http://snomed.info/sct", "code": "390906007"}]}'
;

-- Must have at least one unique index in order to refresh concurrently!
create unique index scan_encounters_unique_encounter_id on shipping.scan_encounters_v1 (sample_id);

comment on materialized view shipping.scan_encounters_v1 is
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


create or replace view shipping.scan_encounters_with_best_available_vaccination_data_v1 as

    with vac_1_most_frequent_valid_date as (
      select distinct on (individual)
      individual,
      vac_date,
      vac_name_1
      from shipping.scan_encounters_v1
      where vac_date >= '2020-12-01'
        and vac_date::date <= encountered::date
      group by individual, vac_date, vac_name_1
      having count(*) > 1
      order by individual, count(*) desc, min(encountered)
    ),
    vac_1_next_best_date as (
      select distinct on (individual)
      individual,
      vac_date,
      vac_name_1,
      case
        when vac_date < '2020-12-01' or vac_date::date > encountered::date then 1
      end as vac_date_1_out_of_range
      from shipping.scan_encounters_v1
      where individual not in (select individual from vac_1_most_frequent_valid_date)
        and vac_date is not null
      order by individual, vac_date_1_out_of_range nulls first, encountered
    ),
    vac_2_most_frequent_valid_date as (
      select distinct on (individual)
      individual,
      vac_date_2,
      vac_name_2
      from shipping.scan_encounters_v1
      where vac_date_2 >= '2020-12-01'
        and vac_date_2::date <= encountered::date
      group by individual, vac_date_2, vac_name_2
      having count(*) > 1
      order by individual, count(*) desc, min(encountered)
    ),
    vac_2_next_best_date as (
      select distinct on (individual)
      individual,
      vac_date_2,
      vac_name_2,
      case
        when vac_date_2 < '2020-12-01' or vac_date_2::date > encountered::date then 1
      end as vac_date_2_out_of_range
      from shipping.scan_encounters_v1
      where individual not in (select individual from vac_2_most_frequent_valid_date)
        and vac_date_2 is not null
      order by individual, vac_date_2_out_of_range nulls first, encountered
    ),
    vac_3_most_frequent_valid_date as (
      select distinct on (individual)
      individual,
      vac_date_3,
      vac_name_3
      from shipping.scan_encounters_v1
      where vac_date_3 >= '2020-12-01'
        and vac_date_3::date <= encountered::date
      group by individual, vac_date_3, vac_name_3
      having count(*) > 1
      order by individual, count(*) desc, min(encountered)
    ),
    vac_3_next_best_date as (
      select distinct on (individual)
      individual,
      vac_date_3,
      vac_name_3,
      case
        when vac_date_3 < '2020-12-01' or vac_date_3::date > encountered::date then 1
      end as vac_date_3_out_of_range
      from shipping.scan_encounters_v1
      where individual not in (select individual from vac_3_most_frequent_valid_date)
        and vac_date_3 is not null
      order by individual, vac_date_3_out_of_range nulls first, encountered
    ),
    encounters_with_best_available_vac as (
      select individual,
        encounter_id,
        encountered::date,
        covid_doses,
        covid_vax,
        coalesce(vac_1_most_frequent_valid_date.vac_date, vac_1_next_best_date.vac_date) as best_available_vac_date_1,
        coalesce(vac_1_most_frequent_valid_date.vac_name_1, vac_1_next_best_date.vac_name_1) as best_available_vac_name_1,
        coalesce(vac_2_most_frequent_valid_date.vac_date_2, vac_2_next_best_date.vac_date_2) as best_available_vac_date_2,
        coalesce(vac_2_most_frequent_valid_date.vac_name_2, vac_2_next_best_date.vac_name_2) as best_available_vac_name_2,
        coalesce(vac_3_most_frequent_valid_date.vac_date_3, vac_3_next_best_date.vac_date_3) as best_available_vac_date_3,
        coalesce(vac_3_most_frequent_valid_date.vac_name_3, vac_3_next_best_date.vac_name_3) as best_available_vac_name_3,
        vac_date_1_out_of_range,
        vac_date_2_out_of_range,
        vac_date_3_out_of_range,
        vaccination_status as old_vaccination_status
        from
        shipping.scan_encounters_v1
        left join vac_1_most_frequent_valid_date using (individual)
        left join vac_1_next_best_date using (individual)
        left join vac_2_most_frequent_valid_date using (individual)
        left join vac_2_next_best_date using (individual)
        left join vac_3_most_frequent_valid_date using (individual)
        left join vac_3_next_best_date using (individual)
    ),
    encounters_with_best_available_vac_applied as (
      select individual,
        encounter_id,
        encountered,
        covid_doses,
        covid_vax,
        case
          when best_available_vac_date_1::date <= encountered::date then array[best_available_vac_date_1, best_available_vac_name_1]
          else array[null,null]
        end as applied_vac_1,
        case
          when best_available_vac_date_2::date <= encountered::date then array[best_available_vac_date_2, best_available_vac_name_2]
          else array[null,null]
        end as applied_vac_2,
        case
          when best_available_vac_date_3::date <= encountered::date then array[best_available_vac_date_3, best_available_vac_name_3]
          else array[null,null]
        end as applied_vac_3,
        vac_date_1_out_of_range,
        vac_date_2_out_of_range,
        vac_date_3_out_of_range,
        case
          when best_available_vac_date_1 > best_available_vac_date_2
            or best_available_vac_date_2 > best_available_vac_date_3
            or best_available_vac_date_1 > best_available_vac_date_3
          then 1
        end as vac_dates_out_of_order,
        old_vaccination_status
      from encounters_with_best_available_vac
    ),
    encounters_with_best_available_vac_applied_dose_count as (
      select individual,
        encounter_id,
        encountered,
        covid_vax as pt_entered_covid_vax,
        covid_doses as pt_entered_covid_doses,
        case
          when applied_vac_3[1] is not null then 3
          when applied_vac_2[1] is not null then 2
          when applied_vac_1[1] is not null then 1
          when encountered::date < '2020-12-01' or covid_vax = 'no' then 0
          else null
        end as calculated_covid_doses,
        applied_vac_1[1] as calculated_vac_date_1,
        applied_vac_1[2] as calculated_vac_name_1,
        applied_vac_2[1] as calculated_vac_date_2,
        applied_vac_2[2] as calculated_vac_name_2,
        applied_vac_3[1] as calculated_vac_date_3,
        applied_vac_3[2] as calculated_vac_name_3,
        vac_date_1_out_of_range,
        vac_date_2_out_of_range,
        vac_date_3_out_of_range,
        old_vaccination_status
      from encounters_with_best_available_vac_applied
    )

    select
      individual,
      encounter_id,
      encountered,
      pt_entered_covid_vax,
      pt_entered_covid_doses,
      calculated_covid_doses,
      calculated_vac_date_1,
      calculated_vac_name_1,
      calculated_vac_date_2,
      calculated_vac_name_2,
      calculated_vac_date_3,
      calculated_vac_name_3,
      vac_date_1_out_of_range,
      vac_date_2_out_of_range,
      vac_date_3_out_of_range,
      case
        when encountered < '2020-12-01' then 'na'
        when pt_entered_covid_vax = 'no' then 'not_vaccinated'
          when (vac_date_3_out_of_range is null and  (calculated_vac_name_3 in (values('pfizer'), ('moderna')) and date_part('day', encountered::timestamp - calculated_vac_date_3::timestamp) >= 14))
            or (vac_date_2_out_of_range is null and calculated_vac_name_1 = 'johnson' and calculated_vac_name_2 in (values('pfizer'), ('moderna'), ('johnson')) and date_part('day', encountered::timestamp - calculated_vac_date_2::timestamp) >= 14) then 'boosted'
          when vac_date_1_out_of_range is null and calculated_vac_name_1 = 'johnson' and date_part('day', encountered::timestamp - calculated_vac_date_1::timestamp) >= 14 then 'fully_vaccinated'
          when vac_date_2_out_of_range is null and calculated_vac_name_1 in (values('pfizer'), ('moderna')) and calculated_vac_name_2 in (values('pfizer'), ('moderna')) and date_part('day', encountered::timestamp - calculated_vac_date_2::timestamp) >= 14 then 'fully_vaccinated'
          when vac_date_1_out_of_range is null and calculated_vac_name_1 in (values('pfizer'), ('moderna'), ('johnson')) and date_part('day', encountered::timestamp - calculated_vac_date_1::timestamp) >= 1 then 'partially_vaccinated'
          when vac_date_1_out_of_range is null and date_part('day', encountered::timestamp - calculated_vac_date_1::timestamp) = 0 then 'not_vaccinated'
        when coalesce(vac_date_1_out_of_range, vac_date_2_out_of_range, vac_date_3_out_of_range) = 1 then 'invalid'
          else 'unknown'
      end as best_available_vac_status
    from encounters_with_best_available_vac_applied_dose_count
;


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


create or replace view shipping.genome_submission_metadata_v1 as

    select
        sample.identifier as sfs_sample_identifier,
        sampleid.barcode as sfs_sample_barcode,
        collectionid.barcode as sfs_collection_barcode,
        -- Fall back on encountered date if the collected date is not available
        coalesce(sample.collected::date, encounter.encountered::date) as collection_date,
        case sample.details ->> 'swab_type'
          when 'ans' then 'Anterior nasal swab'
          -- New tiny swabs are still ANS swabs
          when 'tiny' then 'Anterior nasal swab'
          when 'mtb' then 'Mid-turbinate nasal swab'
          when 'np' then 'Nasopharyngeal swab'
          else null
        end as swab_type,
        -- Separate by study arm for easier reporting of VoCs to study leads
        case
          when identifier_set.name in ('collections-scan', 'collections-scan-kiosks') then 'SCAN'
          when identifier_set.name in ('collections-adult-family-home-outbreak', 'collections-workplace-outbreak', 'collections-workplace-outbreak-tiny-swabs') then 'AFH/Workplace'
          when identifier_set.name in ('collections-childcare') then 'Childcare'
          when identifier_set.name in ('collections-apple-respiratory', 'collections-apple-respiratory-serial') then 'Apple'
          when identifier_set.name in ('collections-household-general', 'collections-household-intervention',
                                       'collections-household-intervention-asymptomatic', 'collections-household-observation',
                                       'collections-household-observation-asymptomatic') then 'Households'
          when identifier_set.name in ('collections-kaiser') then 'Kaiser'
          when identifier_set.name in ('collections-kiosks', 'collections-kiosks-asymptomatic') then 'Shelters'
          when identifier_set.name in ('collections-school-testing-home', 'collections-school-testing-observed') then 'Snohomish Schools'
          when identifier_set.name in ('collections-seattleflu.org') then 'SCH'
          when identifier_set.name in ('collections-uw-home', 'collections-uw-observed', 'collections-uw-tiny-swabs',
                                       'collections-uw-tiny-swabs-home', 'collections-uw-tiny-swabs-observed') then 'HCT'
          when identifier_set.name in ('collections-radxup-yakima-schools-home', 'collections-radxup-yakima-schools-observed') then 'Yakima Schools'
          else 'SFS'
        end as source,
        location.hierarchy -> 'puma' as puma,
        -- Convert census tract to counties for Washington counties
        -- May be removed if county is added to `location.hierarchy`
        case left(location.hierarchy -> 'tract', 5)
          when '53001' then	'Adams'
          when '53003' then	'Asotin'
          when '53005' then	'Benton'
          when '53007' then	'Chelan'
          when '53009' then	'Clallam'
          when '53011' then	'Clark'
          when '53013' then	'Columbia'
          when '53015' then	'Cowlitz'
          when '53017' then	'Douglas'
          when '53019' then	'Ferry'
          when '53021' then	'Franklin'
          when '53023' then	'Garfield'
          when '53025' then	'Grant'
          when '53027' then	'Grays Harbor'
          when '53029' then	'Island'
          when '53031' then	'Jefferson'
          when '53033' then	'King'
          when '53035' then	'Kitsap'
          when '53037' then	'Kittitas'
          when '53039' then	'Klickitat'
          when '53041' then	'Lewis'
          when '53043' then	'Lincoln'
          when '53045' then	'Mason'
          when '53047' then	'Okanogan'
          when '53049' then	'Pacific'
          when '53051' then	'Pend Oreille'
          when '53053' then	'Pierce'
          when '53055' then	'San Juan'
          when '53057' then	'Skagit'
          when '53059' then	'Skamania'
          when '53061' then	'Snohomish'
          when '53063' then	'Spokane'
          when '53065' then	'Stevens'
          when '53067' then	'Thurston'
          when '53069' then	'Wahkiakum'
          when '53071' then	'Walla Walla'
          when '53073' then	'Whatcom'
          when '53075' then	'Whitman'
          when '53077' then	'Yakima'
          else null
        end as county,
        -- If location is null, assume the sample is from within Washington.
        coalesce(initcap(replace(location.hierarchy -> 'state', '_', ' ')), 'Washington') as state,
        case
          -- clinical residuals are all considered baseline samples
          when sample.details ->> 'sample_origin' in ('hmc_retro','nwh_retro','phskc_retro','sch_retro','uwmc_retro') then true
          -- SCAN samples not from groups or priority codes are considered baseline samples
          when identifier_set.name = 'collections-scan' and scan_study_arm != 'group_enroll_arm_4' and priority_code is null then true
          -- SCAN priority codes for shifting sampling frame to be more representative are baseline samples
          when priority_code in ('ACRS','JFS','OPENDOORS','PACISLWA','PICAWA-1','SCANCBO','SCANKIDS') then true
          -- All other studies are longitudinal or cluster/outbreak investigations so not considered baseline
          else false
        end as baseline_surveillance

    from warehouse.sample
    join warehouse.identifier sampleid on sampleid.uuid::text = sample.identifier
    left join warehouse.identifier collectionid on collectionid.uuid::text = sample.collection_identifier
    left join warehouse.identifier_set on collectionid.identifier_set_id = identifier_set.identifier_set_id
    left join warehouse.encounter using (encounter_id)
    left join shipping.scan_encounters_v1 using (encounter_id)
    left join warehouse.primary_encounter_location using (encounter_id)
    left join warehouse.location using (location_id);

comment on view shipping.genome_submission_metadata_v1 is
  'View of minimal metadata used for consensus genome submissions';

revoke all
    on shipping.genome_submission_metadata_v1
  from "assembly-exporter";

grant select
    on shipping.genome_submission_metadata_v1
  to "assembly-exporter";


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
        end as result,
        collection_id_set.name as collection_set_name

    from warehouse.presence_absence
    join warehouse.target using (target_id)
    join warehouse.organism using (organism_id)
    join warehouse.sample using (sample_id)
    join warehouse.identifier collection_id on (sample.collection_identifier = cast(collection_id.uuid as text))
    join warehouse.identifier_set collection_id_set on (collection_id.identifier_set_id = collection_id_set.identifier_set_id)
    left join warehouse.identifier sample_id on (sample.identifier = cast(sample_id.uuid as text))
    left join warehouse.encounter using (encounter_id)
    left join warehouse.site using (site_id)

    where organism.lineage <@ (table reportable)
    and (present or present is null)
     -- Only report on SCAN samples and SFS prospective samples
    -- We don't have to worry about SFS consent date because the
    -- clinical team checks this before they contact the participant.
    and collection_id_set.name in (values ('collections-scan'),
                                   ('collections-scan-kiosks'),
                                   ('collections-household-observation'),
                                   ('collections-household-intervention'),
                                   ('collections-swab&send'),
                                   ('collections-kiosks'),
                                   ('collections-self-test'),
                                   ('collections-swab&send-asymptomatic'),
                                   ('collections-kiosks-asymptomatic'),
                                   ('collections-environmental'),
                                   ('collections-uw-home'),
                                   ('collections-uw-observed'),
                                   ('collections-uw-tiny-swabs-home'),
                                   ('collections-uw-tiny-swabs-observed'),
                                   ('collections-household-general'),
                                   ('collections-childcare'),
                                   ('collections-adult-family-home-outbreak'),
                                   ('collections-workplace-outbreak'),
                                   ('collections-apple-respiratory'),
                                   ('collections-school-testing-home'),
                                   ('collections-school-testing-observed'),
                                   ('collections-radxup-yakima-schools-home'),
                                   ('collections-radxup-yakima-schools-observed'),
                                   ('collections-workplace-outbreak-tiny-swabs')
                                   )
    and coalesce(encountered::date, date_or_null(sample.details ->> 'date')) >= '2020-01-01'
    and presence_absence.details @> '{"assay_type": "Clia"}'
    order by encountered desc;

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


create or replace view shipping.return_results_v3 as

    with samples as (
      select
        sample_id,
        barcode as qrcode,
        case when encountered::date >= '2020-08-19' or encountered is null
            then collected
            else encountered::date
        end as collect_ts,
        sample.details @> '{"note": "never-tested"}' as never_tested,
        sample.details ->> 'swab_type' as swab_type,
        -- The identifier set of the sample's collection identifier determines
        -- if the sample was collected under staff observation.
        -- In-person kiosk enrollment samples are collected under staff
        -- observation while mail-in samples are done without staff observation.
        --  Jover, 22 July 2020
        case identifier_set.name
          when 'collections-scan-kiosks' then true
          when 'collections-uw-observed' then true
          when 'collections-scan' then false
          when 'collections-uw-home' then false
          when 'collections-uw-tiny-swabs-home' then false
          when 'collections-uw-tiny-swabs-observed' then true
          when 'collections-childcare' then false
          when 'collections-adult-family-home-outbreak' then true
          when 'collections-workplace-outbreak' then true
          when 'collections-apple-respiratory' then false
          when 'collections-school-testing-home' then false
          when 'collections-school-testing-observed' then true
          when 'collections-radxup-yakima-schools-home' then false
          when 'collections-radxup-yakima-schools-observed' then true
          when 'collections-workplace-outbreak-tiny-swabs' then true
          else null
        end as staff_observed,
        case when identifier_set.name in (
          'collections-adult-family-home-outbreak',
          'collections-workplace-outbreak',
          'collections-workplace-outbreak-tiny-swabs'
        ) then 'clinical' else 'IRB'
        end as pre_analytical_specimen_collection

      from
        warehouse.identifier
        join warehouse.identifier_set using (identifier_set_id)
        left join warehouse.sample on uuid::text = sample.collection_identifier
        left join warehouse.encounter using (encounter_id)
      where
        identifier_set.name in (
          'collections-scan',
          'collections-scan-kiosks',
          'collections-uw-home',
          'collections-uw-observed',
          'collections-uw-tiny-swabs-home',
          'collections-uw-tiny-swabs-observed',
          'collections-childcare',
          'collections-adult-family-home-outbreak',
          'collections-workplace-outbreak',
          'collections-apple-respiratory',
          'collections-school-testing-home',
          'collections-school-testing-observed',
          'collections-radxup-yakima-schools-home',
          'collections-radxup-yakima-schools-observed',
          'collections-workplace-outbreak-tiny-swabs'
        )
        -- Add a date cutoff so that we only return results from samples
        -- collected after the SCAN IRB study launched on 2020-06-10.
        and collected >= '2020-06-10 00:00:00 US/Pacific'

      order by collected, barcode
    )

    select
        qrcode,
        collect_ts,
        case
            when sample_id is null then 'not-received'
            when never_tested then 'never-tested'
            when sample_id is not null and presence_absence_id is null then 'pending'
            when hcov19_pa.present is true then 'positive'
            when hcov19_pa.present is false then 'negative'
            when presence_absence_id is not null and hcov19_pa.present is null then 'inconclusive'
        end as status_code,
        result_ts,
        swab_type,
        staff_observed,
        pre_analytical_specimen_collection
    from
      samples
      left join shipping.hcov19_presence_absence_result_v1 as hcov19_pa using (sample_id)
    where
      hcov19_pa.sample_id is null or hcov19_pa.details @> '{"assay_type": "Clia"}'
    ;

comment on view shipping.return_results_v3 is
  'View of barcodes and presence/absence results for SFS return of results on the UW Lab Med site';

revoke all
    on shipping.return_results_v3
    from "return-results-exporter";

grant select
    on shipping.return_results_v3
    to "return-results-exporter";


create or replace view shipping.scan_return_results_v1 as

    with scan_samples as (
      select
        sample_id,
        barcode as qrcode,
        case when encountered::date >= '2020-08-19'
            then collected
            else encountered::date
        end as collect_ts,
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
            when hcov19_pa.present is true then 'positive'
            when hcov19_pa.present is false then 'negative'
            when presence_absence_id is not null and hcov19_pa.present is null then 'inconclusive'
        end as status_code,
        result_ts,
        swab_type,
        staff_observed
    from
      scan_samples
      left join shipping.hcov19_presence_absence_result_v1 as hcov19_pa using (sample_id)
    where
      hcov19_pa.sample_id is null or hcov19_pa.details @> '{"assay_type": "Clia"}'
    ;

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


create or replace view shipping.scan_demographics_v2 as

    with hcov19_presence_absence as (
        select
          sample_id,
          case
            when present is true then 'positive'
            when present is false then 'negative'
            when present is null then 'positive'
          end as hcov19_result,
          presence_absence_id,
          hcov19_result_release_date
        from
          shipping.hcov19_presence_absence_result_v1 as pa
        where
          pa.details @> '{"assay_type": "Clia"}'
      ),

    king_county_demographics as (
        select
            encountered_week,
            sex,
            age_range_decade,
            age_range_decade_lower,
            age_range_decade_upper,
            race,
            hispanic_or_latino,
            income,
            sample_id
        from shipping.scan_encounters_v1
        -- Limit to King County participants only since the dashboard
        -- is only for King County.
        where puma in (
          '5311601', '5311602', '5311603', '5311604',
          '5311605', '5311606', '5311607', '5311608',
          '5311609', '5311610', '5311611', '5311612',
          '5311613', '5311614', '5311615', '5311616')
    )

    select
        encountered_week,
        sex,
        age_range_decade,
        age_range_decade_lower,
        age_range_decade_upper,
        race,
        hispanic_or_latino,
        income,
        hcov19_result
    from king_county_demographics
    left join hcov19_presence_absence using (sample_id)
;

comment on view shipping.scan_demographics_v2 is
  'A view of basic demographic data with hcov19 results from the SCAN project for Power BI dashboards.';

revoke all
    on shipping.scan_demographics_v2
  from "scan-dashboard-exporter";

grant select
    on shipping.scan_demographics_v2
    to "scan-dashboard-exporter";


create or replace view shipping.scan_hcov19_result_counts_v1 as

    with scan_hcov19_results as (
      select
          case
              when hcov19_pa.present is true then 'positive'
              when hcov19_pa.present is false then 'negative'
              when hcov19_pa.present is null then 'inconclusive'
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
      join shipping.hcov19_presence_absence_result_v1 as hcov19_pa using (sample_id)
      where
        hcov19_pa.sample_id is null or hcov19_pa.details @> '{"assay_type": "Clia"}'
    )

    select
        hcov19_result_release_date,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive')) as total_hcov19_positives,
        count(*) filter (where hcov19_result = 'negative') as total_hcov19_negatives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county = 'king') as king_county_positives,
        count(*) filter (where hcov19_result = 'negative' and county = 'king') as king_county_negatives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county = 'snohomish') as snohomish_county_positives,
        count(*) filter (where hcov19_result = 'negative' and county = 'snohomish') as snohomish_county_negatives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county = 'yakima') as yakima_county_positives,
        count(*) filter (where hcov19_result = 'negative' and county = 'yakima') as yakima_county_negatives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county is null) as other_positives,
        count(*) filter (where hcov19_result = 'negative' and county is null) as other_negatives
    from scan_hcov19_results
    group by hcov19_result_release_date
;

comment on view shipping.scan_hcov19_result_counts_v1 is
  'A view of counts of hcov19 results from the SCAN project grouped by date results were released.';

revoke all
    on shipping.scan_hcov19_result_counts_v1
  from "scan-dashboard-exporter";

grant select
    on shipping.scan_hcov19_result_counts_v1
    to "scan-dashboard-exporter";


create or replace view shipping.scan_hcov19_result_counts_v2 as

    with scan_hcov19_results as (
      select
          case
              when hcov19_pa.present is true then 'positive'
              when hcov19_pa.present is false then 'negative'
              when hcov19_pa.present is null then 'inconclusive'
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
          end as county,
          upper(priority_code) as priority_code,
          scan_study_arm

      from shipping.scan_encounters_v1
      join shipping.hcov19_presence_absence_result_v1 as hcov19_pa using (sample_id)
      where
        hcov19_pa.sample_id is null or hcov19_pa.details @> '{"assay_type": "Clia"}'
    )

    select
        hcov19_result_release_date,
        priority_code,
        scan_study_arm,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive')) as total_hcov19_positives,
        count(*) filter (where hcov19_result = 'negative') as total_hcov19_negatives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county = 'king') as king_county_positives,
        count(*) filter (where hcov19_result = 'negative' and county = 'king') as king_county_negatives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county = 'snohomish') as snohomish_county_positives,
        count(*) filter (where hcov19_result = 'negative' and county = 'snohomish') as snohomish_county_negatives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county = 'yakima') as yakima_county_positives,
        count(*) filter (where hcov19_result = 'negative' and county = 'yakima') as yakima_county_negatives,
        count(*) filter (where hcov19_result in ('positive', 'inconclusive') and county is null) as other_positives,
        count(*) filter (where hcov19_result = 'negative' and county is null) as other_negatives
    from scan_hcov19_results
    group by hcov19_result_release_date, priority_code, scan_study_arm
;

comment on view shipping.scan_hcov19_result_counts_v2 is
  'A view of counts of hcov19 results from the SCAN project grouped by date results were released, with priority codes.';

revoke all
    on shipping.scan_hcov19_result_counts_v2
  from "scan-dashboard-exporter";

grant select
    on shipping.scan_hcov19_result_counts_v2
    to "scan-dashboard-exporter";


create or replace view shipping.scan_enrollments_v1 as

    with location_names as (
        select
            scale,
            identifier,
            case scale
                when 'neighborhood_district' then details->>'name'
                when 'puma' then details->>'NAMELSAD10'
            end as geo_location_name
        from warehouse.location
        where scale in ('neighborhood_district', 'puma')
        and hierarchy @> 'state => washington'
    )

    select
        illness_questionnaire_date,
        scan_study_arm,
        priority_code,
        puma,
        neighborhood_district,
        geo_location_name,
        sample is not null or never_tested is not null as kit_received

    from shipping.scan_encounters_v1
    join location_names on coalesce(neighborhood_district,puma) = lower(location_names.identifier)

;

comment on view shipping.scan_enrollments_v1 is
  'A view of enrollment data from the SCAN project for Power BI dashboards';

revoke all
    on shipping.scan_enrollments_v1
  from "scan-dashboard-exporter";

grant select
    on shipping.scan_enrollments_v1
    to "scan-dashboard-exporter";


create or replace view shipping.scan_redcap_enrollments_v1 as

    select
        illness_questionnaire_date,
        scan_study_arm,
        puma,
        age_range_fine,
        age_range_fine_lower,
        age_range_fine_upper
    from shipping.scan_encounters_v1
;

comment on view shipping.scan_redcap_enrollments_v1 is
  'A view of SCAN REDCap enrollments for internal Power BI dashboards';

revoke all
    on shipping.scan_redcap_enrollments_v1
    from "scan-dashboard-exporter";

grant select
    on shipping.scan_redcap_enrollments_v1
    to "scan-dashboard-exporter";

/******************** VIEWS FOR UW REOPENING ********************/

-- Not linebreaking this wide text in order to make reading and reviewing easier.
create or replace view shipping.uw_reopening_enrollment_fhir_encounter_details_v1 as

    select
        encounter_id,

        --integer questions
        max (integer_response[1]) filter ( where link_id = 'weight' ) as weight,

        --date questions
        max (date_response[1]) filter ( where link_id = 'today_consent' ) as today_consent,
        max (date_response[1]) filter ( where link_id = 'enrollment_date_time' ) as enrollment_date_time,
        max (date_response[1]) filter ( where link_id = 'hospital_arrive_base' ) as hospital_arrive_base,
        max (date_response[1]) filter ( where link_id = 'hospital_leave_base' ) as hospital_leave_base,
        max (date_response[1]) filter ( where link_id = 'prior_test_positive_date_base' ) as prior_test_positive_date_base,

        --boolean questions
        bool_or (boolean_response) filter ( where link_id = 'study_area' ) as study_area,
        bool_or (boolean_response) filter ( where link_id = 'attend_uw' ) as attend_uw,
        bool_or (boolean_response) filter ( where link_id = 'english_speaking' ) as english_speaking,
        bool_or (boolean_response) filter ( where link_id = 'athlete' ) as athlete,
        bool_or (boolean_response) filter ( where link_id = 'uw_medicine_yesno' ) as uw_medicine_yesno,
        bool_or (boolean_response) filter ( where link_id = 'inperson_classes' ) as inperson_classes,
        bool_or (boolean_response) filter ( where link_id = 'uw_job' ) as uw_job,
        bool_or (boolean_response) filter ( where link_id = 'live_other_uw' ) as live_other_uw,
        bool_or (boolean_response) filter ( where link_id = 'uw_apt_yesno' ) as uw_apt_yesno,
        bool_or (boolean_response) filter ( where link_id = 'core_pregnant' ) as core_pregnant,
        bool_or (boolean_response) filter ( where link_id = 'core_latinx' ) as core_latinx,
        bool_or (boolean_response) filter ( where link_id = 'mobility' ) as mobility,
        bool_or (boolean_response) filter ( where link_id = 'vaccine_hx' ) as vaccine_hx,
        bool_or (boolean_response) filter ( where link_id = 'hall_health' ) as hall_health,
        bool_or (boolean_response) filter ( where link_id = 'prior_test_base' ) as prior_test_base,
        bool_or (boolean_response) filter ( where link_id = 'travel_countries_phs_base' ) as travel_countries_phs_base,
        bool_or (boolean_response) filter ( where link_id = 'travel_states_phs_base' ) as travel_states_phs_base,
        bool_or (boolean_response) filter ( where link_id = 'swab_and_send_calc' ) as swab_and_send_calc,
        bool_or (boolean_response) filter ( where link_id = 'kiosk_calc' ) as kiosk_calc,
        bool_or (boolean_response) filter ( where link_id = 'covid_test_week_base' ) as covid_test_week_base,

        --string questions
        max (string_response[1]) filter ( where link_id = 'text_or_email' ) as text_or_email,
        max (string_response[1]) filter ( where link_id = 'text_or_email_attestation' ) as text_or_email_attestation,
        max (string_response[1]) filter ( where link_id = 'campus_location' ) as campus_location,
        max (string_response[1]) filter ( where link_id = 'affiliation' ) as affiliation,
        max (string_response[1]) filter ( where link_id = 'student_level' ) as student_level,
        max (string_response[1]) filter ( where link_id = 'sea_employee_type' ) as sea_employee_type,
        max (string_response[1]) filter ( where link_id = 'core_house_members' ) as core_house_members,
        max (string_response[1]) filter ( where link_id = 'core_education' ) as core_education,
        max (string_response[1]) filter ( where link_id = 'core_income' ) as core_income,
        max (string_response[1]) filter ( where link_id = 'wfh_base' ) as wfh_base,
        max (string_response[1]) filter ( where link_id = 'core_housing_type' ) as core_housing_type,
        max (string_response[1]) filter ( where link_id = 'core_health_risk' ) as core_health_risk,
        max (string_response[1]) filter ( where link_id = 'core_tobacco_use' ) as core_tobacco_use,
        max (string_response[1]) filter ( where link_id = 'sought_care_base' ) as sought_care_base,
        max (string_response[1]) filter ( where link_id = 'hospital_where_base' ) as hospital_where_base,
        max (string_response[1]) filter ( where link_id = 'hospital_ed_base' ) as hospital_ed_base,
        max (string_response[1]) filter ( where link_id = 'prior_test_positive_base' ) as prior_test_positive_base,
        max (string_response[1]) filter ( where link_id = 'prior_test_type_base' ) as prior_test_type_base,
        max (string_response[1]) filter ( where link_id = 'prior_test_result_base' ) as prior_test_result_base,
        max (string_response[1]) filter ( where link_id = 'contact_base' ) as contact_base,
        max (string_response[1]) filter ( where link_id = 'wash_hands_base' ) as wash_hands_base,
        max (string_response[1]) filter ( where link_id = 'clean_surfaces_base' ) as clean_surfaces_base,
        max (string_response[1]) filter ( where link_id = 'hide_cough_base' ) as hide_cough_base,
        max (string_response[1]) filter ( where link_id = 'mask_base' ) as mask_base,
        max (string_response[1]) filter ( where link_id = 'distance_base' ) as distance_base,
        max (string_response[1]) filter ( where link_id = 'novax_reason' ) as novax_reason,
        max (string_response[1]) filter ( where link_id = 'covid_vaccine' ) as covid_vaccine,
        max (string_response[1]) filter ( where link_id = 'covid_novax_reason' ) as covid_novax_reason,
        max (string_response[1]) filter ( where link_id = 'alerts_off' ) as alerts_off,
        max (string_response[1]) filter ( where link_id = 'pronouns' ) as pronouns,
        max (string_response[1]) filter ( where link_id = 'on_campus_freq' ) as on_campus_freq,
        max (string_response[1]) filter ( where link_id = 'vaccine_method' ) as vaccine_method,
        max (string_response[1]) filter ( where link_id = 'vaccine_where' ) as vaccine_where,
        max (string_response[1]) filter ( where link_id = 'added_surveillance_groups' ) as added_surveillance_groups,

        --string question arrays
        max (string_response) filter ( where link_id = 'countries_visited_base' ) as countries_visited_base,
        max (string_response) filter ( where link_id = 'states_visited_base' ) as states_visited_base,

        --coded questions
        case
          when max(array_length(code_response, 1)) filter ( where link_id = 'core_race' ) is null
            then max (string_response) filter ( where link_id = 'core_race' )
          else
            max (code_response) filter ( where link_id = 'core_race' )
        end as core_race,

        --the current year's flu vaccine
        bool_or (boolean_response) filter ( where link_id = 'vaccine' ) as received_this_season_flu_vaccine,
        max (date_response[1]) filter ( where link_id = 'vaccine' ) as received_this_season_flu_vaccine_date

    from
      shipping.fhir_questionnaire_responses_v1 responses
    join warehouse.encounter using (encounter_id)
    where identifier ~~ 'https://hct.redcap.rit.uw.edu/45/%/enrollment_arm_1/'::text
    group by encounter_id
;

comment on view shipping.uw_reopening_enrollment_fhir_encounter_details_v1 is
  'For the UW reopening project, a view of enrollment details that are in FHIR format';

revoke all
  on shipping.uw_reopening_enrollment_fhir_encounter_details_v1
  from "incidence-modeler";

grant select
   on shipping.uw_reopening_enrollment_fhir_encounter_details_v1
   to "incidence-modeler";


create or replace view shipping.uw_reopening_encounters_v1 as
(
  with enrollments as
  (
  select encounter_id as enrollment_encounter_id
  , individual_id as enrollment_individual_id
  from warehouse.encounter
  where
      encounter.identifier like 'https://hct.redcap.rit.uw.edu/45/%/enrollment_arm_1/'
  ),

  encounters as
  (
  select encounter_id
  , encountered
  , age
  , individual_id
  , site_id
  , details
  from warehouse.encounter
  where
      encounter.identifier like 'https://hct.redcap.rit.uw.edu/45/%/encounter_arm_1/%'
  )

  select
  encounter.encounter_id
  , encounter.encountered
  , to_char(encounter.encountered, 'IYYY-"W"IW') as encountered_week
  , site.identifier as site
  , site.details ->> 'type' as site_type

  , individual.identifier as individual
  , individual.sex

  , age_in_years(encounter.age) as age

  , age_fine.range as age_range_fine
  , age_in_years(lower(age_fine.range)) as age_range_fine_lower
  , age_in_years(upper(age_fine.range)) as age_range_fine_upper

  , age_coarse.range as age_range_coarse
  , age_in_years(lower(age_coarse.range)) as age_range_coarse_lower
  , age_in_years(upper(age_coarse.range)) as age_range_coarse_upper

  , age_decade.range as age_range_decade
  , age_in_years(lower(age_decade.range)) as age_range_decade_lower
  , age_in_years(upper(age_decade.range)) as age_range_decade_upper

  , loc.hierarchy -> 'puma' as puma
  , loc.hierarchy -> 'tract' as census_tract
  , loc.hierarchy -> 'neighborhood_district' as neighborhood_district

  , sample.sample_id
  , sample.identifier as sample
  , sample.collection_identifier
  , sample.details @> '{"note": "never-tested"}' as never_tested

  , enroll_details.weight

  , enroll_details.today_consent
  , enroll_details.enrollment_date_time
  , enroll_details.hospital_arrive_base
  , enroll_details.hospital_leave_base
  , enroll_details.prior_test_positive_date_base

  , enroll_details.study_area
  , enroll_details.attend_uw
  , enroll_details.english_speaking
  , enroll_details.athlete
  , enroll_details.uw_medicine_yesno
  , enroll_details.inperson_classes
  , enroll_details.uw_job
  , enroll_details.live_other_uw
  , enroll_details.uw_apt_yesno
  , enroll_details.core_pregnant
  , enroll_details.core_latinx
  , enroll_details.mobility
  , enroll_details.vaccine_hx
  , enroll_details.hall_health
  , enroll_details.prior_test_base
  , enroll_details.travel_countries_phs_base
  , enroll_details.travel_states_phs_base
  , enroll_details.swab_and_send_calc
  , enroll_details.kiosk_calc
  , enroll_details.covid_test_week_base

  , enroll_details.text_or_email
  , enroll_details.text_or_email_attestation
  , enroll_details.campus_location
  , enroll_details.affiliation
  , enroll_details.student_level
  , enroll_details.sea_employee_type
  , enroll_details.core_house_members
  , enroll_details.core_education
  , enroll_details.core_income
  , enroll_details.wfh_base
  , enroll_details.core_housing_type
  , enroll_details.core_health_risk
  , enroll_details.core_tobacco_use
  , enroll_details.sought_care_base
  , enroll_details.hospital_where_base
  , enroll_details.hospital_ed_base
  , enroll_details.prior_test_positive_base
  , enroll_details.prior_test_type_base
  , enroll_details.prior_test_result_base
  , enroll_details.contact_base
  , enroll_details.wash_hands_base
  , enroll_details.clean_surfaces_base
  , enroll_details.hide_cough_base
  , enroll_details.mask_base
  , enroll_details.distance_base
  , enroll_details.novax_reason
  , enroll_details.covid_vaccine
  , enroll_details.covid_novax_reason
  , enroll_details.alerts_off
  , enroll_details.pronouns
  , enroll_details.on_campus_freq
  , enroll_details.vaccine_method
  , enroll_details.vaccine_where
  , enroll_details.added_surveillance_groups

  , enroll_details.countries_visited_base
  , enroll_details.states_visited_base

  , enroll_details.core_race

  , enroll_details.received_this_season_flu_vaccine
  , enroll_details.received_this_season_flu_vaccine_date

  from encounters encounter
  join enrollments enroll on encounter.individual_id = enroll.enrollment_individual_id
  join warehouse.site site on encounter.site_id = site.site_id
  join warehouse.individual individual on individual.individual_id = encounter.individual_id
  join shipping.uw_reopening_enrollment_fhir_encounter_details_v1 enroll_details on enroll_details.encounter_id = enroll.enrollment_encounter_id
  left join warehouse.primary_encounter_location enc_loc on enc_loc.encounter_id = encounter.encounter_id
  left join warehouse.location loc on enc_loc.location_id = loc.location_id
  left join shipping.age_bin_fine_v2 age_fine on age_fine.range @> encounter.age
  left join shipping.age_bin_coarse_v2 age_coarse on age_coarse.range @> encounter.age
  left join shipping.age_bin_decade_v1 age_decade on age_decade.range @> encounter.age
  left join warehouse.sample sample on sample.encounter_id = encounter.encounter_id
  -- Filter out follow up encounters
  where not encounter.details @> '{"reason": [{"system": "http://snomed.info/sct", "code": "390906007"}]}'
)

;

comment on view shipping.uw_reopening_encounters_v1 is
  'For the UW reopening project, a view that ties enrollment questionnaire data to encounter data';

revoke all
on shipping.uw_reopening_encounters_v1
from "incidence-modeler";

grant select
  on shipping.uw_reopening_encounters_v1
  to "incidence-modeler";

create or replace view shipping.uw_reopening_ehs_reporting_v1 as
(
  with encounters as
  (
  select ids.barcode
  , age as age_at_encounter
  , sex
  , pronouns as preferred_pronouns
  , text_or_email as preferred_contact_method_for_study
  , text_or_email_attestation as preferred_contact_method_for_attestations
  , enrollment_date_time as study_enrollment_date_time
  , campus_location
  , affiliation
  , athlete as is_student_athlete
  , student_level
  , sea_employee_type as employee_category
  , inperson_classes as is_taking_inperson_classes
  , uw_job as works_at_uw
  , on_campus_freq as on_campus_frequency_code
  , case
      when on_campus_freq = 'one_or_less' then 'One day a week or less'
      when on_campus_freq = 'two_or_more' then 'Two days a week or more'
      when on_campus_freq = 'not_on_campus' then 'Do not come to campus'
      else 'Unknown'
  end as on_campus_frequency_description
  , wfh_base as able_to_work_or_study_from_home_code
  , case
      when wfh_base = 'onsite' then 'No, I always have to be on-site for work or school'
      when wfh_base = 'only_wfh' then 'Yes, I have only worked or studied from home'
      when wfh_base = 'wfh_onsite' then 'I have worked or studied both from home AND on-site'
      when wfh_base = 'dont_say' then 'Prefer not to say'
      else 'Unknown'
  end as able_to_work_or_study_from_home_description
  , core_housing_type as housing_type
  , core_house_members as number_house_members
  , live_other_uw as lives_with_uw_students_or_employees
  , uw_apt_yesno as lives_in_uw_apartment
  , alerts_off
  from shipping.uw_reopening_encounters_v1
  join warehouse.identifier ids on ids.uuid::text = collection_identifier
  )
  , results as
  (
  select qrcode as barcode
  , collect_ts as sample_collection_date
  , status_code as test_result
  , result_ts as test_result_date
  from shipping.return_results_v3
  where qrcode in (select barcode from encounters)
  and status_code in ( values ('positive'), ('negative'), ('inconclusive'), ('never-tested'))
  )
  select * from results
  join encounters using (barcode)
)
;

comment on view shipping.uw_reopening_ehs_reporting_v1 is
  'For the UW reopening project, a view that combines encounter, enrollment, and hCoV-19 test result data';

  revoke all
  on shipping.uw_reopening_encounters_v1
  from "ehs-results-exporter";

  grant select
  on shipping.uw_reopening_ehs_reporting_v1
  to "ehs-results-exporter";


create materialized view shipping.__uw_encounters as (
  select
    encounter.encounter_id
    , individual.identifier as individual
    , encounter.encountered::date as encountered
    , jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'url' ) as redcap_url
    , jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'project_id' ) as redcap_project_id
    , jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'record_id' ) as redcap_record_id
    , jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'event_name' ) as redcap_event_name
    , jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'repeat_instance' ) as redcap_repeat_instance
    , q_screen_positive.boolean_response as screen_positive
    , q_daily_symptoms.boolean_response as daily_symptoms
    , case when q_daily_exposure.encounter_id is null then null when q_daily_exposure.string_response[1] in ('yes', 'yes_vac') then true else false end as daily_exposure
    , q_daily_exposure_known_pos.string_response[1] as daily_exposure_known_pos
    , q_daily_travel.string_response[1] as daily_travel
    , q_testing_trigger.boolean_response as testing_trigger
    , q_surge_selected_flag.boolean_response as surge_selected_flag
    , (select max(collected)::date from warehouse.sample where encounter_id = encounter.encounter_id group by encounter_id) as sample_collection_date
    , q_prior_test_positive_date.date_response[1]::date as prior_test_positive_date
	from warehouse.encounter
	join warehouse.individual using (individual_id)
	left join shipping.fhir_questionnaire_responses_v1 q_screen_positive on q_screen_positive.encounter_id = encounter.encounter_id and q_screen_positive.link_id = 'screen_positive'
	left join shipping.fhir_questionnaire_responses_v1 q_daily_symptoms on q_daily_symptoms.encounter_id = encounter.encounter_id and q_daily_symptoms.link_id = 'daily_symptoms'
	left join shipping.fhir_questionnaire_responses_v1 q_daily_exposure on q_daily_exposure.encounter_id = encounter.encounter_id and q_daily_exposure.link_id = 'daily_exposure'
	left join shipping.fhir_questionnaire_responses_v1 q_daily_exposure_known_pos on q_daily_exposure_known_pos.encounter_id = encounter.encounter_id and q_daily_exposure_known_pos.link_id = 'daily_exposure_known_pos'
	left join shipping.fhir_questionnaire_responses_v1 q_daily_travel on q_daily_travel.encounter_id = encounter.encounter_id and q_daily_travel.link_id = 'daily_travel'
	left join shipping.fhir_questionnaire_responses_v1 q_testing_trigger on q_testing_trigger.encounter_id = encounter.encounter_id and q_testing_trigger.link_id = 'testing_trigger'
	left join shipping.fhir_questionnaire_responses_v1 q_surge_selected_flag on q_surge_selected_flag.encounter_id = encounter.encounter_id and q_surge_selected_flag.link_id = 'surge_selected_flag'
	left join shipping.fhir_questionnaire_responses_v1 q_prior_test_positive_date on q_prior_test_positive_date.encounter_id = encounter.encounter_id and q_prior_test_positive_date.link_id = 'prior_test_positive_date'
	where
            encounter.identifier like 'https://hct.redcap.rit.uw.edu/45/%/encounter_arm_1/%'
)
;

create unique index __uw_encounters_unique_encounter_id on shipping.__uw_encounters (encounter_id);

comment on materialized view shipping.__uw_encounters is
  'Pull records from the encounter arm of the UW Reopening project. Include key questionnaire values.';


create or replace view shipping.__uw_priority_queue_v1 as (
    with uw_individual_summaries as (
	    select
		    individual,
		    max(encountered) filter (where testing_trigger is true) as latest_invite_date,
		    max(sample_collection_date) filter (where sample_collection_date is not null) as latest_collection_date,
		    count(*) filter (where testing_trigger is true) as invitation_count,
		    max(sample_collection_date) filter (where pa.present = true) as latest_positive_hcov19_collection_date,
		    max(prior_test_positive_date) filter (where prior_test_positive_date is not null) as latest_prior_test_positive_date
	    from shipping.__uw_encounters
	    left join warehouse.sample using (encounter_id)
	    left join shipping.hcov19_presence_absence_result_v1 pa using (sample_id)
	    group by individual
     ),

    uw_enrollments as (
        select
            encounter.encounter_id,
            individual.identifier as individual,
            encounter.encountered::date as encountered,
            jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'url' ) as redcap_url,
            jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'project_id' ) as redcap_project_id,
            jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'record_id' ) as redcap_record_id,
            jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'event_name' ) as redcap_event_name,
            jsonb_extract_path_text (encounter.details, '_provenance', 'redcap', 'repeat_instance' ) as redcap_repeat_instance,
            latest_invite_date,
            latest_collection_date,
            coalesce(invitation_count, 0) as invitation_count,
            latest_positive_hcov19_collection_date,
            latest_prior_test_positive_date,
            prior_test_positive_date_base::date as prior_test_positive_date_base,
            on_campus_freq,
            added_surveillance_groups,
            alerts_off
        from warehouse.encounter
        join warehouse.individual using (individual_id)
        join shipping.uw_reopening_enrollment_fhir_encounter_details_v1 using (encounter_id)
        left join uw_individual_summaries on uw_individual_summaries.individual = individual.identifier
        where encounter.identifier like 'https://hct.redcap.rit.uw.edu/45/%/enrollment_arm_1/'
    ),

    -- Select encounters for testing based on positive daily attestations
    positive_daily_attestations as (
        select
            __uw_encounters.redcap_url,
            __uw_encounters.redcap_project_id,
            __uw_encounters.redcap_record_id,
            __uw_encounters.redcap_event_name,
            __uw_encounters.redcap_repeat_instance,
            __uw_encounters.encountered,
            individual,
            latest_invite_date,
            latest_collection_date,
            case
                when daily_symptoms then 1

                /* Why 2 days after attestation to exposures? We don't want
                 * to offer testing too early after exposure and risk false
                 * negatives. Per Trevor, we expect that a viral load high
                 * enough to detect with PCR and be contagious is generally
                 * centered on day 3 after exposure, though some will be
                 * earlier and some later.  By offering testing at day 2, some
                 * people will get tested that same day (day 2), most will
                 * (hopefully) get tested the next day (day 3), and some others
                 * by the day after that (day 4). This puts results being known
                 * at days 3–5.
                 * -trs, 19 Oct 2020

                 * The `yes_now` option replaced `yes` in Winter Quarter 2021,
                 * with the wording for `yes_now` being `Yes, I was exposed to COVID-19
                 * in the last 48 hours`, but we kept both in the query so that the query
                 * would work before and after the option change.
                 */
                when daily_exposure_known_pos in ('yes', 'yes_now') and age(__uw_encounters.encountered) >= '2 days' then 1
                when daily_exposure and age(__uw_encounters.encountered) >= '2 days' then 1

                /* The `yes_late` option takes care of the 48 hours. The option label is
                 * `Yes, I was exposed to COVID-19 more than 48 hours ago`
                 */
                when daily_exposure_known_pos = 'yes_late' then 1
                when daily_travel in ('yes_state', 'yes_country') then 1
                else null -- The final SELECT at the end of the view drops records with a NULL priority
            end as priority,
            case
                when daily_symptoms then 'symptomatic'
                when daily_exposure_known_pos in ('yes', 'yes_now') and age(__uw_encounters.encountered) >= '2 days' then 'exposure_to_known_positive'
                when daily_exposure and age(__uw_encounters.encountered) >= '2 days' then 'gathering_over_10'
                when daily_exposure_known_pos = 'yes_late' then 'exposure_to_known_positive'
                when daily_travel in ('yes_state', 'yes_country') then 'travel'
                else null
            end as priority_reason,

            latest_positive_hcov19_collection_date,
            latest_prior_test_positive_date,
            prior_test_positive_date_base,
            alerts_off
        from shipping.__uw_encounters
        join uw_enrollments using (individual)
        -- Filter to encounters within the last 7 days so we don't send invites for old attestations
        where age(__uw_encounters.encountered) <= '7 days'
        -- Filter to encounters for participants whose last invite was over 3 days before encounter
        and (latest_invite_date is null or latest_invite_date < __uw_encounters.encountered - interval '3 days')
        -- Filter to encounters for participants who have never had a sample collected or their last sample collection was over 3 days before encounter
        and (latest_collection_date is null or latest_collection_date < __uw_encounters.encountered - interval '3 days')
        -- Filter for instances that do no already have testing_trigger filled
        and testing_trigger is null
        -- Filter for postive daily attestations only
        and screen_positive
    ),

    -- Select added_surveillance_groups if they are in the group for today
    added_surveillance_groups as (
        select
            redcap_url,
            redcap_project_id,
            redcap_record_id,
            redcap_event_name,
            redcap_repeat_instance,
            encountered,
            individual,
            latest_invite_date,
            latest_collection_date,
            2 as priority,
            'surveillance' as priority_reason,

            latest_positive_hcov19_collection_date,
            latest_prior_test_positive_date,
            prior_test_positive_date_base,
            alerts_off
        from uw_enrollments
        -- Filter to added_surveillance_groups for today.
        -- To_Char(current_date, 'day') returns a string padded to 9 characters, so trim it.
        -- Example: where 'tuesday' = 'tuesday'
        where added_surveillance_groups = trim(To_Char(current_date, 'day'))
        -- Filter for participants who haven't ever been invited or haven't been invited in 3 days
        and (latest_invite_date is null or latest_invite_date < current_date - interval '3 days')
        -- Filter to participants who have never had a sample collected or whose last sample collection was over 3 days before today
        and (latest_collection_date is null or latest_collection_date < current_date - interval '3 days')
	  ),

    -- Select enrollments for baseline testing
    baseline as (
        select
            redcap_url,
            redcap_project_id,
            redcap_record_id,
            redcap_event_name,
            redcap_repeat_instance,
            encountered,
            individual,
            latest_invite_date,
            latest_collection_date,
            4 as priority,
            'baseline' as priority_reason,

            latest_positive_hcov19_collection_date,
            latest_prior_test_positive_date,
            prior_test_positive_date_base,
            alerts_off
        from uw_enrollments
        -- Filter to participants who enrolled after this date when
        -- baseline test invitations were handled manually
        where encountered > '2021-11-15'
         -- Filter to enrollments have never had a sample collected
        and latest_collection_date is null
        -- Filter to enrollments that have been invited up to 2 times
        and invitation_count < 2
        -- Filter for participants who haven't ever been invited or haven't been invited in 3 days
        and (latest_invite_date is null or latest_invite_date < current_date - interval '3 days')
    ),

    -- Select enrollments for surveillance testing
    surveillance as (
        select
            redcap_url,
            redcap_project_id,
            redcap_record_id,
            redcap_event_name,
            redcap_repeat_instance,
            encountered,
            individual,
            latest_invite_date,
            latest_collection_date,
            5 as priority,
            'surveillance' as priority_reason,

            latest_positive_hcov19_collection_date,
            latest_prior_test_positive_date,
            prior_test_positive_date_base,
            alerts_off
        from uw_enrollments
        -- Filter to participants who come to campus
        where (on_campus_freq != 'not_on_campus')
        -- Filter to participants whose last invite was over 3 days before today
        and (latest_invite_date is null or latest_invite_date < current_date - interval '3 days')
        -- Filter to participants who have never had a sample collected or whose last sample collection was over 3 days before today
        and (latest_collection_date is null or latest_collection_date < current_date - interval '3 days')
    ),

    /**
    Select encounters for surge testing purposes.
    Handled separately from daily attestation encounters because we want to
    include all surge selected participants regardless of previous testing.
    **/
    surge_testing as (
        select
            __uw_encounters.redcap_url,
            __uw_encounters.redcap_project_id,
            __uw_encounters.redcap_record_id,
            __uw_encounters.redcap_event_name,
            __uw_encounters.redcap_repeat_instance,
            __uw_encounters.encountered,
            individual,
            latest_invite_date,
            latest_collection_date,
            3 as priority,
            'surge_testing' as priority_reason,

            latest_positive_hcov19_collection_date,
            latest_prior_test_positive_date,
            prior_test_positive_date_base,
            alerts_off
        from shipping.__uw_encounters
        join uw_enrollments using (individual)
        -- Filter for instances that have been selected with surge_selected_flag
        where surge_selected_flag is true
        -- Filter for instances that do no already have testing_trigger filled
        and testing_trigger is null
         -- Filter to encounters within the last 7 days so we don't send invites for old surges
        and age(__uw_encounters.encountered) <= '7 days'
    )

    -- The final SELECT to UNION ALL results from each prioritization category and to filter
    -- for clauses that are common across all prioritization categories.
    select redcap_url, redcap_project_id, redcap_record_id, redcap_event_name, redcap_repeat_instance,
    encountered, individual, latest_invite_date, latest_collection_date, priority,
    priority_reason
    from
    (
    select redcap_url, redcap_project_id, redcap_record_id, redcap_event_name, redcap_repeat_instance,
    encountered, individual, latest_invite_date, latest_collection_date, priority,
    priority_reason, latest_positive_hcov19_collection_date, latest_prior_test_positive_date,
    prior_test_positive_date_base, alerts_off
    from positive_daily_attestations
    where priority is not null

    union all

    select redcap_url, redcap_project_id, redcap_record_id, redcap_event_name, redcap_repeat_instance,
    encountered, individual, latest_invite_date, latest_collection_date, priority,
    priority_reason, latest_positive_hcov19_collection_date, latest_prior_test_positive_date,
    prior_test_positive_date_base, alerts_off
    from baseline

    union all

    select redcap_url, redcap_project_id, redcap_record_id, redcap_event_name, redcap_repeat_instance,
    encountered, individual, latest_invite_date, latest_collection_date, priority,
    priority_reason, latest_positive_hcov19_collection_date, latest_prior_test_positive_date,
    prior_test_positive_date_base, alerts_off
    from surveillance

    union all

    select redcap_url, redcap_project_id, redcap_record_id, redcap_event_name, redcap_repeat_instance,
    encountered, individual, latest_invite_date, latest_collection_date, priority,
    priority_reason, latest_positive_hcov19_collection_date, latest_prior_test_positive_date,
    prior_test_positive_date_base, alerts_off
    from surge_testing

    union all

    select redcap_url, redcap_project_id, redcap_record_id, redcap_event_name, redcap_repeat_instance,
    encountered, individual, latest_invite_date, latest_collection_date, priority,
    priority_reason, latest_positive_hcov19_collection_date, latest_prior_test_positive_date,
    prior_test_positive_date_base, alerts_off
    from added_surveillance_groups
    ) as a
    where
    -- Filter for participants who have not tested positive with us in the past 14 days
    (latest_positive_hcov19_collection_date is null or latest_positive_hcov19_collection_date < current_date - interval '14 days')

    -- Filter for participants who have not tested positive somewhere else (reported on daily attestation) in the past 14 days
    and (latest_prior_test_positive_date is null or latest_prior_test_positive_date < current_date - interval '14 days')

    -- Filter for participants who have not tested positive somewhere else (reported on enrollment) in the past 14 days
    and (prior_test_positive_date_base is null or prior_test_positive_date_base < current_date - interval '14 days')

    -- Filter for participants who will receive alerts from REDCap
    -- Because those with this set to 'off' won't receive alerts, these invitations to test would be wasted because the
    -- participants wouldn't know that they've been invited.
    and (alerts_off is null or alerts_off <> 'off')
)
;

comment on view shipping.__uw_priority_queue_v1 is
  'Identify all encounter instances which indicate need for testing by UW reopening study (contains duplicate individuals)';


create or replace view shipping.uw_priority_queue_v1 as (
    with distinct_individuals as (
        select distinct on (individual)
            redcap_url,
            redcap_project_id,
            redcap_record_id,
            redcap_event_name,
            redcap_repeat_instance,
            encountered,
            individual,
            latest_invite_date,
            latest_collection_date,
            priority,
            priority_reason
        from shipping.__uw_priority_queue_v1
        -- Use the highest priority encounter that is the latest instance
        order by individual, priority asc nulls last, redcap_repeat_instance desc
    )

    select *
    from distinct_individuals
    order by
        priority asc nulls last,                        -- Prioritize first by reason testing is indicated…
        age(latest_invite_date) desc nulls first,       -- …then, people who were least recently (if ever) offered a test
        age(latest_collection_date) desc nulls first,   -- …then, people who were least recently (if ever) tested
        encountered asc,                                -- …then, people who attested positive or enrolled the longest ago
        individual asc                                  -- …and finally, by an evenly-distributed value (SHA-256 hash).
)
;

comment on view shipping.uw_priority_queue_v1 is
  'Deduplicated indivduals that need to be tested by UW reopening study, ordered by priority';

revoke all
  on shipping.uw_priority_queue_v1
  from "uw-priority-queue-processor";

grant select
  on shipping.uw_priority_queue_v1
  to "uw-priority-queue-processor";



create or replace view shipping.linelist_data_for_wa_doh_v1 as (
    select distinct on (sample_id)
          -- The naming scheme of these columns is an artefact of how we
          -- originally submitted data to WaDoH from REDCap reports. Now they
          -- expect columns structured and named in a specific way, and we
          -- cannot modify those with our current linelist format. We can notify
          -- WaDoH of our intent to change our submitted data format (e.g.
          -- custom format or ELFF), but they'll need sufficient advanced
          -- notice. At that point, we can update this view with different
          -- column names.
          --
          -- kfay, 5 January 2021
          sample_id,
          sampleid.barcode as sample_barcode,
          collectionid.barcode as collection_barcode,
          sample.details ->> 'clia_barcode' as scan_id,
          case
              when present = 't' then 'positive'
              when present = 'f' then 'negative'
              else 'inconclusive'
          end as test_result,
          hcov19_result_release_date as date_tested,
          best_available_encounter_date as enrollment_date,
          collected as collection_date,
          best_available_site as site_name,
          best_available_site_type as site_context,
          sex as sex_new,
          case
            when hispanic_or_latino = 't' then 'yes'
            when hispanic_or_latino = 'f' then 'no'
          end as ethnicity,
          case
            when pregnant = 't' then 'yes'
            when pregnant = 'f' then 'no'
          end as pregnant_yesno,
          symptom_onset as symptom_duration
      from
          shipping.hcov19_presence_absence_result_v1
          join warehouse.sample using (sample_id)
          join shipping.sample_with_best_available_encounter_data_v1 using (sample_id)
          join warehouse.identifier sampleid on sampleid.uuid::text = sample.identifier
          join warehouse.identifier collectionid on collectionid.uuid::text = sample.collection_identifier
          left join warehouse.encounter using (encounter_id)
          left join warehouse.individual using (individual_id)
          left join shipping.fhir_encounter_details_v2 using (encounter_id)
      -- Add a date cutoff so that we only return results from samples
      -- collected after the SCAN IRB study launched on 2020-06-10.
      -- `shipping.return_results_v3` uses this same filter.
      where collected >= '2020-06-10 00:00:00 US/Pacific'
      and hcov19_presence_absence_result_v1.details @> '{"assay_type": "Clia"}'
      order by sample_id, encounter_id
);

comment on view shipping.linelist_data_for_wa_doh_v1 is
  'Custom view of hCoV-19 results for preparing linelists for Washington Department of Health';

revoke all
  on shipping.linelist_data_for_wa_doh_v1
  from "return-results-exporter";

grant select
  on shipping.linelist_data_for_wa_doh_v1
  to "return-results-exporter";


commit;
