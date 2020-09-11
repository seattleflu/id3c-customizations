-- Revert seattleflu/id3c-customizations:operations/schema from pg

begin;

drop schema operations;

commit;
