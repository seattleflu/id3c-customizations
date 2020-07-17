-- Deploy seattleflu/id3c-customizations:roles/scan-dashboard-exporter/create to pg

begin;

create role "scan-dashboard-exporter";

comment on role "scan-dashboard-exporter" is
    'For exporting data for SCAN dashboards';

commit;
