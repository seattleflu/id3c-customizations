-- Deploy seattleflu/id3c-customizations:warehouse/sequence-read-set/check-sequence-read-set-rls to pg

begin;

create or replace function warehouse.check_sequence_read_set_rls() returns trigger as $$
	begin
		if (new.access_role is null and exists(select * from warehouse.sample where sample_id = new.sample_id and access_role is null)) or
		(new.access_role is not null and exists(select * from warehouse.sample where sample_id = new.sample_id and access_role::text = new.access_role::text)) then
			return new;
		else
			raise exception 'sample_id %: access_role value for sample and sequence_read_set must match', new.sample_id using errcode = 'triggered_action_exception';
		end if;
	end;

$$
language plpgsql
stable;

create trigger check_sequence_read_set_rls before insert or update on warehouse.sequence_read_set
for each row execute procedure warehouse.check_sequence_read_set_rls();

commit;
