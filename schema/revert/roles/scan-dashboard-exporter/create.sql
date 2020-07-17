-- Revert seattleflu/id3c-customizations:roles/scan-dashboard-exporter/create from pg

begin;

drop role "scan-dashboard-exporter";

commit;
