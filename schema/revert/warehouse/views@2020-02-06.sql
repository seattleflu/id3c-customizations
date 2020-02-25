-- Revert seattleflu/id3c-customizations:views from pg

begin;

drop view warehouse.address;
drop view warehouse.tract;

commit;
