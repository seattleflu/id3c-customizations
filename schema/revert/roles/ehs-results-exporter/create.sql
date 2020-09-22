-- Revert seattleflu/id3c-customizations:roles/ehs-results-exporter/create from pg

begin;

drop role "ehs-results-exporter";

commit;
