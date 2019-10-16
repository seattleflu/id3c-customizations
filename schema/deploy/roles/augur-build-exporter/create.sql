-- Deploy seattleflu/id3c-customizations:roles/augur-build-exporter/create to pg

begin;

create role "augur-build-exporter";

comment on role "augur-build-exporter" is
    'For exporting metadata and genomic sequences to SFS augur build';

commit;
