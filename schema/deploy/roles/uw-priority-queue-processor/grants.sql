-- Deploy seattleflu/id3c-customizations:roles/uw-priority-queue-processor/grants to pg

begin;

revoke all on database :"DBNAME" from "uw-priority-queue-processor";
revoke all on schema receiving, warehouse, shipping from "uw-priority-queue-processor";
revoke all on all tables in schema receiving, warehouse, shipping from "uw-priority-queue-processor";

grant connect on database :"DBNAME" to "uw-priority-queue-processor";

grant usage
    on schema shipping
    to "uw-priority-queue-processor";

commit;
