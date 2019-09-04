-- Deploy seattleflu/id3c-customizations:roles/reportable-condition-notifier/grants to pg
-- requires: roles/reportable-condition-notifier/create
-- requires: shipping/views

begin;

-- This change is designed to be sqitch rework-able to make it easier to update
-- the grants for this role.

grant connect on database :"DBNAME" to "reportable-condition-notifier";

grant usage
    on schema warehouse, shipping
    to "reportable-condition-notifier";

grant select
    on warehouse.site, warehouse.presence_absence, shipping.reportable_condition_v1
    to "reportable-condition-notifier";

grant update (details)
    on warehouse.presence_absence
    to "reportable-condition-notifier";

commit;
