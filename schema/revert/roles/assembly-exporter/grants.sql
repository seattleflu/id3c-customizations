-- Revert seattleflu/id3c-customizations:roles/assembly-exporter/grants from pg

begin;

revoke all on database :"DBNAME" from "assembly-exporter";
revoke all on schema receiving, warehouse, shipping from "assembly-exporter";
revoke all on all tables in schema receiving, warehouse, shipping from "assembly-exporter";

revoke connect on database :"DBNAME" from "assembly-exporter";

commit;
