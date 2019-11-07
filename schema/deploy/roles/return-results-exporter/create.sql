-- Deploy seattleflu/id3c-customizations:roles/return-results-exporter/create to pg
-- requires: shipping/views

begin;

create role "return-results-exporter";

comment on role "return-results-exporter" is
    'For exporting return of results for SFS website';

commit;
