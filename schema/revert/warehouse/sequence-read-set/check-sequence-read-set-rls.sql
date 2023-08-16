-- Revert seattleflu/id3c-customizations:warehouse/sequence-read-set/check-sequence-read-set-rls from pg

begin;

drop trigger if exists check_sequence_read_set_rls on warehouse.sequence_read_set;
drop function if exists warehouse.check_sequence_read_set_rls;

commit;
