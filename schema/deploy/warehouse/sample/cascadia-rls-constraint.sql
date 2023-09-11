-- Deploy seattleflu/id3c-customizations:warehouse/sample/cascadia-rls-constraint to pg
-- requires: roles/cascadia/create

begin;

alter table warehouse.sample
    drop constraint if exists cascadia_rls;

alter table warehouse.sample
    add constraint cascadia_rls check(
        not (details ? 'sample_origin') or
        (
            (lower(details ->> 'sample_origin') != 'cascadia' and coalesce(access_role::text,'') != 'cascadia'::text) or
            (lower(details ->> 'sample_origin') = 'cascadia' and coalesce(access_role::text,'') = 'cascadia'::text)
        )
    );

commit;
