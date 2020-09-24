-- Revert seattleflu/id3c-customizations:roles/ehs-results-exporter/grants from pg

begin;

revoke all on database :"DBNAME" from "ehs-results-exporter";
revoke all on schema receiving, warehouse, shipping from "ehs-results-exporter";
revoke all on all tables in schema receiving, warehouse, shipping from "ehs-results-exporter";

revoke connect on database :"DBNAME" from "ehs-results-exporter";

commit;
