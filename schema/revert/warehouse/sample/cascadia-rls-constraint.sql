-- Revert seattleflu/id3c-customizations:warehouse/sample/cascadia-rls-constraint from pg

begin;

alter table warehouse.sample
    drop constraint cascadia_rls;

commit;
