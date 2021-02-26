-- Deploy seattleflu/id3c-customizations:roles/assembly-exporter/create to pg

begin;

create role "assembly-exporter";

comment on role "assembly-exporter" is
    'For exporting metadata for consensus genome assembly and submissions';

commit;
