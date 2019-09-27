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
        sample.details->'_provenance'->>'sheet' as sheet

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


create or replace view shipping.metadata_for_augur_build_v1 as

    select  sample as strain,
            encountered_date as date,
            'seattle' as region,
            residence_census_tract as location,
            'Seattle Flu Study' as authors,
            case
                when age < 18 then 'child'
                else 'adult'
            end as age_category,
            case
                when site_type in ('childrensHospital', 'childrensClinic', 'childrensHospital', 'clinic', 'hospital', 'retrospective') then 'clinical'
                when site_type in ('childcare' , 'collegeCampus' , 'homelessShelter' , 'port', 'publicSpace', 'workplace') then 'community'
            end as site_category,
            case
                when age >= 1 then concat(cast(age::int as text) ,'y')
                when age < 1 then concat(cast(round(age*12) as text), 'm')
            end as age,
            residence_census_tract,
            site,
            site_type,
            flu_shot,
            sex

			from shipping.incidence_model_observation_v2;

comment on view shipping.metadata_for_augur_build_v1 is
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

commit;
