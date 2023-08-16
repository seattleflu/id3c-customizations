-- Deploy seattleflu/id3c-customizations:warehouse/sample/cascadia-rls-constraint to pg
-- requires: roles/cascadia/create

begin;

alter table warehouse.sample
    add constraint cascadia_rls check(
        (lower(details ->> 'sample_origin') != 'cascadia') or
        (details ->> 'sample_origin' = 'cascadia' AND access_role::text = 'cascadia'::text)
    );

commit;
