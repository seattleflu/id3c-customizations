-- Deploy seattleflu/id3c-customizations:roles/view-owner/create to pg

begin;

create role "view-owner";

comment on role "view-owner" is
    'A minimally privileged role to use as the owner of views';

commit;
