-- Revert seattleflu/id3c-customizations:roles/cascadia/create from pg

begin;

drop role cascadia;

commit;
