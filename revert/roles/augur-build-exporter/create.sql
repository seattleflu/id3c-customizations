-- Revert seattleflu/id3c-customizations:roles/augur-build-exporter/create from pg

begin;

drop role "augur-build-exporter";

commit;
