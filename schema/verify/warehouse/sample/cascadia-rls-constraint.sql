-- Verify seattleflu/id3c-customizations:warehouse/sample/cascadia-rls-constraint on pg

begin;

do $$
declare
    sample_number int;
begin
    insert into warehouse.sample (identifier, details, access_role)
        values (uuid_generate_v4(), '{"sample_origin": "cascadia"}'::jsonb, 'cascadia') returning sample_id into sample_number;

    set local role reporter;

    assert 0 = (
        select count(*)
        from warehouse.sample
        where sample_id = sample_number
    );

    set local role cascadia;

    assert 1 = (
        select count(*)
        from warehouse.sample
        where sample_id = sample_number
    );
end
$$;

rollback;
