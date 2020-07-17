-- Revert seattleflu/id3c-customizations:roles/scan-dashboard-exporter/grants from pg
-- requires: roles/scan-dashboard-exporter/create
-- requires: seattleflu/schema:shipping/schema

begin;

revoke all on database :"DBNAME" from "scan-dashboard-exporter";
revoke all on schema receiving, warehouse, shipping from "scan-dashboard-exporter";
revoke all on all tables in schema receiving, warehouse, shipping from "scan-dashboard-exporter";

commit;
