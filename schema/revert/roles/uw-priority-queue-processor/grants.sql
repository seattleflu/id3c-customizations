-- Revert seattleflu/id3c-customizations:roles/uw-priority-queue-processor/grants from pg

begin;

revoke all on database :"DBNAME" from "uw-priority-queue-processor";
revoke all on schema receiving, warehouse, shipping from "uw-priority-queue-processor";
revoke all on all tables in schema receiving, warehouse, shipping from "uw-priority-queue-processor";

revoke connect on database :"DBNAME" from "uw-priority-queue-processor";

commit;
