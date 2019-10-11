-- Revert seattleflu/id3c-customizations:roles/reportable-condition-notifier/grants from pg
-- requires: roles/reportable-condition-notifier/create
-- requires: shipping/views

begin;

revoke update (details)
    on warehouse.presence_absence
  from "reportable-condition-notifier";

revoke select
    on warehouse.site, warehouse.presence_absence, shipping.reportable_condition_v1
  from "reportable-condition-notifier";

revoke usage
    on schema warehouse, shipping
  from "reportable-condition-notifier";

revoke connect on database :"DBNAME" from "reportable-condition-notifier";

commit;
