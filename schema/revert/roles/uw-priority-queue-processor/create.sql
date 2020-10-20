-- Revert seattleflu/id3c-customizations:roles/uw-priority-queue-processor/create from pg

begin;

drop role "uw-priority-queue-processor";

commit;
