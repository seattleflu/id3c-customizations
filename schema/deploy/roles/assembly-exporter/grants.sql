-- Deploy seattleflu/id3c-customizations:roles/assembly-exporter/grants to pg

begin;

revoke all on database :"DBNAME" from "assembly-exporter";
revoke all on schema receiving, warehouse, shipping from "assembly-exporter";
revoke all on all tables in schema receiving, warehouse, shipping from "assembly-exporter";

grant connect on database :"DBNAME" to "assembly-exporter";

grant usage
    on schema shipping
    to "assembly-exporter";

commit;
