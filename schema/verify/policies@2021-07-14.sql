-- Verify seattleflu/id3c-customizations:policies on pg
-- requires: seattleflu/schema:warehouse/presence_absence
-- requires: seattleflu/schema:roles/reporter
-- requires: seattleflu/schema:roles/fhir-processor/create
-- requires: seattleflu/schema:roles/presence-absence-processor
-- requires: roles/reportable-condition-notifier/create
-- requires: roles/hcov19-visibility/create

begin;

insert into warehouse.target (identifier, control, organism_id)
    values
        ('Measles', false, null),
        ('nCoV', false, (select organism_id from warehouse.organism where lineage = 'Human_coronavirus.2019')),
        ('COVID-19', false, (select organism_id from warehouse.organism where lineage = 'Human_coronavirus.2019'))
    on conflict (identifier) do update set
        organism_id = excluded.organism_id;

with test_sample as (
    insert into warehouse.sample (identifier)
        values (uuid_generate_v4())
        returning sample_id
)
insert into warehouse.presence_absence (identifier, sample_id, target_id, present)
    select
        uuid_generate_v4()::text,
        sample_id,
        target_id,
        true
    from
        warehouse.target,
        test_sample
    where
        target.identifier in ('Measles', 'nCoV', 'COVID-19')
;

do $$
declare
    expected_count integer;
begin
    select into expected_count count(*)
        from warehouse.presence_absence
        join warehouse.target using (target_id)
        where target.identifier in ('nCoV', 'COVID-19');

    assert expected_count >= 2;

    set local role reporter;

    assert 0 = (
        select count(*)
        from warehouse.presence_absence
        join warehouse.target using (target_id)
        where target.identifier in ('nCoV', 'COVID-19')
    );

    set local role "presence-absence-processor";

    assert expected_count = (
        select count(*)
        from warehouse.presence_absence
        join warehouse.target using (target_id)
        where target.identifier in ('nCoV', 'COVID-19')
    );
end
$$;

rollback;
