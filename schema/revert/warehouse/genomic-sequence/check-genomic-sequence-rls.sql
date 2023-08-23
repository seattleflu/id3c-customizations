-- Revert seattleflu/id3c-customizations:warehouse/genomic-sequence/check-genomic-sequence-rls from pg

begin;

drop trigger if exists check_genomic_sequence_rls on warehouse.genomic_sequence;
drop function if exists warehouse.check_genomic_sequence_rls;

commit;
