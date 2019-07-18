-- Revert seattleflu/id3c-customizations:roles/reportable-condition-processor/grants from pg

begin;

revoke select, insert
    on shipping.reportable_condition
  from "reportable-condition-processor";

revoke select
    on warehouse.encounter, warehouse.site, warehouse.sample,
       warehouse.presence_absence, warehouse.target, warehouse.organism
  from "reportable-condition-processor";

revoke usage
    on schema warehouse, shipping
  from "reportable-condition-processor";

revoke connect on database :"DBNAME" from "reportable-condition-processor";

commit;
