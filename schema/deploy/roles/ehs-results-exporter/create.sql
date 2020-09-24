-- Deploy seattleflu/id3c-customizations:roles/ehs-results-exporter/create to pg

begin;

create role "ehs-results-exporter";

comment on role "ehs-results-exporter" is
    'Used to export data to EH&S for the uw-reopening project';

commit;
