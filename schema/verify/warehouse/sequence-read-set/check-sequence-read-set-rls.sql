-- Verify seattleflu/id3c-customizations:warehouse/sequence-read-set/check-sequence-read-set-rls on pg

begin;

do $$
declare
    cascadia_sample_id int;
    other_sample_id int;
    organism int;
begin
    select organism_id into organism from warehouse.organism limit 1;

    insert into warehouse.sample (identifier, access_role)
        values (uuid_generate_v4(), 'cascadia') returning sample_id into cascadia_sample_id;

    insert into warehouse.sample (identifier)
        values (uuid_generate_v4()) returning sample_id into other_sample_id;

    insert into warehouse.sequence_read_set (sample_id, access_role, urls)
        values (cascadia_sample_id, 'cascadia', array[uuid_generate_v4()::text]);

    insert into warehouse.sequence_read_set (sample_id, urls)
        values (other_sample_id, array[uuid_generate_v4()::text]);

    -- these next two inserts should fail silently, with assert statement below to confirm zero count
    begin
        insert into warehouse.sequence_read_set (sample_id)
            values (cascadia_sample_id);
    exception
        when triggered_action_exception then null;
    end;

    begin
        insert into warehouse.sequence_read_set (sample_id, access_role, urls)
            values (other_sample_id, 'cascadia', array[uuid_generate_v4()::text]);
    exception
        when triggered_action_exception then null;
    end;

    -- check expected counts
    assert 2 = (
        select count(*)
        from warehouse.sample
        where sample_id in (cascadia_sample_id, other_sample_id)
    );

    assert 0 = (
        select count(*)
        from warehouse.sequence_read_set
        where (access_role is null and sample_id = cascadia_sample_id) or
            (access_role::text = 'cascadia' and sample_id = other_sample_id)
    );

    set local role cascadia;

    assert 2 = (
        select count(*)
        from warehouse.sequence_read_set
        where (access_role::text = 'cascadia' and sample_id = cascadia_sample_id) or
            (access_role is null and sample_id = other_sample_id)
    );

    set local role reporter;

    assert 1 = (
        select count(*)
        from warehouse.sequence_read_set
        where (access_role::text = 'cascadia' and sample_id = cascadia_sample_id) or
            (access_role is null and sample_id = other_sample_id)
    );

end
$$;


rollback;
