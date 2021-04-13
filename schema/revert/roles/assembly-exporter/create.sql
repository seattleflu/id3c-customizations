-- Revert seattleflu/id3c-customizations:roles/assembly-exporter/create from pg

begin;

drop role "assembly-exporter";

commit;
