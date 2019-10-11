-- Revert seattleflu/id3c-customizations:roles/augur-build-exporter/grants from pg

begin;

revoke select
    on shipping.metadata_for_augur_build_v1, shipping.genomic_sequences_for_augur_build_v1
  from "augur-build-exporter";

revoke usage
    on schema shipping
  from "augur-build-exporter";

revoke connect on database :"DBNAME" from "augur-build-exporter";

commit;
