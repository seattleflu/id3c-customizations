-- Deploy seattleflu/id3c-customizations:roles/reportable-condition-processor/grants to pg
-- requires: roles/reportable-condition-processor/create
-- requires: shipping/reportable_condition

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

grant connect on database :"DBNAME" to "reportable-condition-processor";

grant usage
    on schema warehouse, shipping
    to "reportable-condition-processor";

grant select
    on warehouse.encounter, warehouse.site, warehouse.sample,
        warehouse.presence_absence, warehouse.target, warehouse.organism
    to "reportable-condition-processor";

grant select, insert
    on shipping.reportable_condition
    to "reportable-condition-processor";

commit;
