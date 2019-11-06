-- Revert seattleflu/id3c-customizations:roles/return-results-exporter/grants from pg
-- requires: shipping/views

begin;

revoke select
    on shipping.return_results_v1
  from "return-results-exporter";

revoke usage
    on schema shipping
  from "return-results-exporter";

revoke connect on database :"DBNAME" from "return-results-exporter";

commit;
