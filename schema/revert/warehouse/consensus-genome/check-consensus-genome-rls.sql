-- Revert seattleflu/id3c-customizations:warehouse/consensus-genome/check-consensus-genome-rls from pg

begin;

drop trigger if exists check_consensus_genome_rls on warehouse.consensus_genome;
drop function if exists warehouse.check_consensus_genome_rls;

commit;
