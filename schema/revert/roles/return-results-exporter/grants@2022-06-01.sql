-- Deploy seattleflu/id3c-customizations:roles/return-results-exporter/grants to pg
-- requires: shipping/views

begin;

revoke all on database :"DBNAME" from "return-results-exporter";
revoke all on schema receiving, warehouse, shipping, operations from "return-results-exporter";
revoke all on all tables in schema receiving, warehouse, shipping, operations from "return-results-exporter";

grant connect on database :"DBNAME" to "return-results-exporter";

grant usage
    on schema shipping, operations
    to "return-results-exporter";

grant select
    on shipping.return_results_v1
    to "return-results-exporter";

grant select
    on shipping.return_results_v2
    to "return-results-exporter";

grant select
    on shipping.return_results_v3
    to "return-results-exporter";

grant select
  on shipping.sample_with_best_available_encounter_data_v1
  to "return-results-exporter";

grant select
  on shipping.linelist_data_for_wa_doh_v1
  to "return-results-exporter";

grant delete, insert
    on shipping.latest_results
    to "return-results-exporter";

grant insert
    on operations.deliverables_log
    to "return-results-exporter";

commit;
