-- Deploy seattleflu/id3c-customizations:roles/ehs-results-exporter/grants to pg

begin;

revoke all on database :"DBNAME" from "ehs-results-exporter";
revoke all on schema receiving, warehouse, shipping from "ehs-results-exporter";
revoke all on all tables in schema receiving, warehouse, shipping from "ehs-results-exporter";

grant connect on database :"DBNAME" to "ehs-results-exporter";

grant usage
    on schema shipping
    to "ehs-results-exporter";

commit;
