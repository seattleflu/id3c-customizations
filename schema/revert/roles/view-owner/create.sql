-- Revert seattleflu/id3c-customizations:roles/view-owner/create from pg

begin;

drop role "view-owner";

commit;
