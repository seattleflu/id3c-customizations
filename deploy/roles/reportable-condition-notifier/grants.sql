-- Deploy seattleflu/id3c-customizations:roles/reportable-condition-notifier/grants to pg
-- requires: roles/reportable-condition-notifier/create
-- requires: shipping/reportable_condition

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

grant connect on database :"DBNAME" to "reportable-condition-notifier";

grant usage
    on schema warehouse, shipping
    to "reportable-condition-notifier";

grant select
    on warehouse.site, shipping.reportable_condition
    to "reportable-condition-notifier";

grant update (processing_log)
    on shipping.reportable_condition
    to "reportable-condition-notifier";

commit;
