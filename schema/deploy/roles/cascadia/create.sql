-- Deploy seattleflu/id3c-customizations:roles/cascadia/create to pg

begin;

create role "cascadia";

grant "cascadia" to "consensus-genome-processor";

comment on role "cascadia" is
    'For row-level access to Cascadia data';


create role "reporter-cascadia";

grant "cascadia" to "reporter-cascadia";
grant "reporter" to "reporter-cascadia";

comment on role "reporter-cascadia" is
    'For row-level read access to Cascadia data';

commit;
