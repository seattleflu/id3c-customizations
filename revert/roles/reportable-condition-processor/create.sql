-- Revert seattleflu/id3c-customizations:roles/reportable-condition-processor/create from pg

begin;

drop role "reportable-condition-processor";

commit;
