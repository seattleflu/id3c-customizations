-- Deploy seattleflu/id3c-customizations:warehouse/genomic-sequence/check-genomic-sequence-rls to pg

begin;

create or replace function warehouse.check_genomic_sequence_rls() returns trigger as $$
	begin
		if (new.access_role is null and
            exists(
                select *
                from warehouse.consensus_genome
                where consensus_genome_id = new.consensus_genome_id and access_role is null)) or
		(new.access_role is not null and
            exists(
                select *
                from warehouse.consensus_genome
                where consensus_genome_id = new.consensus_genome_id and access_role::text = new.access_role::text)) then
			return new;
		else
			raise exception 'consensus_genome_id %: access_role value for consensus genome id and genomic sequence must match', new.consensus_genome_id using errcode = 'triggered_action_exception';
		end if;
	end;

$$
language plpgsql
stable;

create trigger check_genomic_sequence_rls before insert or update on warehouse.genomic_sequence
for each row execute procedure warehouse.check_genomic_sequence_rls();

commit;
