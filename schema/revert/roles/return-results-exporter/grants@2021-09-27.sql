-- Deploy seattleflu/id3c-customizations:roles/return-results-exporter/grants to pg
-- requires: shipping/views

begin;

revoke all on database :"DBNAME" from "return-results-exporter";
revoke all on schema receiving, warehouse, shipping from "return-results-exporter";
revoke all on all tables in schema receiving, warehouse, shipping from "return-results-exporter";

grant connect on database :"DBNAME" to "return-results-exporter";

grant usage
    on schema shipping
    to "return-results-exporter";

grant select
    on shipping.return_results_v1
    to "return-results-exporter";


commit;
