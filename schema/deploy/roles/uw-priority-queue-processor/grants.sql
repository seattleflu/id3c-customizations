-- Deploy seattleflu/id3c-customizations:roles/uw-priority-queue-processor/grants to pg

begin;

revoke all on database :"DBNAME" from "uw-priority-queue-processor";
revoke all on schema receiving, warehouse, shipping, operations from "uw-priority-queue-processor";
revoke all on all tables in schema receiving, warehouse, shipping, operations from "uw-priority-queue-processor";

grant connect on database :"DBNAME" to "uw-priority-queue-processor";

grant usage
    on schema receiving, shipping, operations
    to "uw-priority-queue-processor";

grant insert (document)
    on table receiving.redcap_det
    to "uw-priority-queue-processor";

grant select
    on table shipping.uw_priority_queue_v1, operations.test_quota
    to "uw-priority-queue-processor";

grant update (used)
    on table operations.test_quota
    to "uw-priority-queue-processor";

commit;
