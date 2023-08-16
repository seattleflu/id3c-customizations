-- Deploy seattleflu/id3c-customizations:warehouse/consensus-genome/check-consensus-genome-rls to pg

begin;

create or replace function warehouse.check_consensus_genome_rls() returns trigger as $$
	begin
		if (new.access_role is null and exists(select * from warehouse.sample where sample_id = new.sample_id and access_role is null)) or
		(new.access_role is not null and exists(select * from warehouse.sample where sample_id = new.sample_id and access_role::text = new.access_role::text)) then
			return new;
		else
			raise exception 'sample_id %: access_role value for sample and consensus_genome must match', new.sample_id using errcode = 'triggered_action_exception';
		end if;
	end;

$$
language plpgsql
stable;

create trigger check_consensus_genome_rls before insert or update on warehouse.consensus_genome
for each row execute procedure warehouse.check_consensus_genome_rls();

commit;
