-- Deploy seattleflu/id3c-customizations:roles/augur-build-exporter/grants to pg

begin;

revoke all on database :"DBNAME" from "augur-build-exporter";
revoke all on schema receiving, warehouse, shipping from "augur-build-exporter";
revoke all on all tables in schema receiving, warehouse, shipping from "augur-build-exporter";

grant connect on database :"DBNAME" to "augur-build-exporter";

grant usage
    on schema shipping, warehouse
    to "augur-build-exporter";

grant select
    on shipping.metadata_for_augur_build_v3,
       shipping.metadata_for_augur_build_v2,
       shipping.genomic_sequences_for_augur_build_v1,
       warehouse.tract
    to "augur-build-exporter";

commit;
