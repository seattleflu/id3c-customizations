-- Deploy seattleflu/id3c-customizations:roles/latest-results-viewer/create to pg

begin;

create role "latest-results-viewer";

comment on role "latest-results-viewer" is
    'For read-only access to latest results table through API';

commit;
