-- Revert seattleflu/id3c-customizations:roles/reportable-condition-notifier/create from pg

begin;

drop role "reportable-condition-notifier";

commit;
