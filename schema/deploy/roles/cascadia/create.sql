-- Deploy seattleflu/id3c-customizations:roles/cascadia/create to pg

begin;

create role "cascadia";
grant "reporter" to "cascadia";

grant "cascadia" to "consensus-genome-processor";

comment on role "cascadia" is
    'For row-level access to Cascadia data';

commit;
