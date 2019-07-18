-- Revert seattleflu/id3c-customizations:roles/reportable-condition-notifier/grants from pg

begin;

revoke update (processing_log)
    on shipping.reportable_condition
  from "reportable-condition-notifier";

revoke select
    on warehouse.site, shipping.reportable_condition
  from "reportable-condition-notifier";

revoke usage
    on schema warehouse, shipping
  from "reportable-condition-notifier";

revoke connect on database :"DBNAME" from "reportable-condition-notifier";

commit;
