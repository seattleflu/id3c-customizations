-- Revert seattleflu/id3c-customizations:roles/hcov19-visibility/create from pg

begin;

drop role "hcov19-visibility";

commit;
