-- Deploy seattleflu/id3c-customizations:roles/hcov19-visibility/create to pg

begin;

create role "hcov19-visibility";

comment on role "hcov19-visibility" is
    'Allows access to HCoV-19 presence/absence results';

commit;
