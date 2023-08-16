-- Verify seattleflu/id3c-customizations:warehouse/genomic-sequence/check-genomic-sequence-rls on pg

begin;

do $$
declare
    cascadia_sample_id int;
    cascadia_consensus_genome_id int;
    other_sample_id int;
    other_consensus_genome_id int;
    organism int;
begin
    select organism_id into organism from warehouse.organism limit 1;

    insert into warehouse.sample (identifier, access_role)
        values (uuid_generate_v4(), 'cascadia') returning sample_id into cascadia_sample_id;

    insert into warehouse.sample (identifier)
        values (uuid_generate_v4()) returning sample_id into other_sample_id;

    insert into warehouse.consensus_genome (sample_id, organism_id, access_role)
        values (cascadia_sample_id, organism, 'cascadia') returning consensus_genome_id into cascadia_consensus_genome_id;

    insert into warehouse.consensus_genome (sample_id, organism_id)
        values (other_sample_id, organism) returning consensus_genome_id into other_consensus_genome_id;

    insert into warehouse.genomic_sequence (consensus_genome_id, identifier, segment, seq, access_role)
        values (cascadia_consensus_genome_id, uuid_generate_v4()::text, '', '', 'cascadia');

    insert into warehouse.genomic_sequence (consensus_genome_id, identifier, segment, seq)
        values (other_consensus_genome_id, uuid_generate_v4()::text, '', '');

    -- these next two inserts should fail silently, with assert statement below to confirm zero count
    begin
        insert into warehouse.genomic_sequence (consensus_genome_id, identifier, segment, seq)
            values (cascadia_consensus_genome_id, uuid_generate_v4()::text, '', '');
    exception
        when triggered_action_exception then null;
    end;

    begin
        insert into warehouse.genomic_sequence (consensus_genome_id, identifier, segment, seq, access_role)
            values (other_consensus_genome_id, uuid_generate_v4()::text, '', '', 'cascadia');
    exception
        when triggered_action_exception then null;
    end;

    -- check expected counts
    assert 2 = (
        select count(*)
        from warehouse.sample
        where sample_id in (cascadia_sample_id, other_sample_id)
    );

    assert 0 = (
        select count(*)
        from warehouse.genomic_sequence
        where (access_role is null and consensus_genome_id = cascadia_consensus_genome_id) or
            (access_role::text = 'cascadia' and consensus_genome_id = other_consensus_genome_id)
    );

    set local role cascadia;

    assert 2 = (
        select count(*)
        from warehouse.consensus_genome
        where (access_role::text = 'cascadia' and sample_id = cascadia_sample_id) or
            (access_role is null and sample_id = other_sample_id)
    );

    assert 2 = (
        select count(*)
        from warehouse.genomic_sequence
        where (access_role::text = 'cascadia' and consensus_genome_id = cascadia_consensus_genome_id) or
            (access_role is null and consensus_genome_id = other_consensus_genome_id)
    );

    set local role reporter;

    assert 1 = (
        select count(*)
        from warehouse.consensus_genome
        where (access_role::text = 'cascadia' and sample_id = cascadia_sample_id) or
            (access_role is null and sample_id = other_sample_id)
    );

    assert 1 = (
        select count(*)
        from warehouse.genomic_sequence
        where (access_role::text = 'cascadia' and consensus_genome_id = cascadia_consensus_genome_id) or
            (access_role is null and consensus_genome_id = other_consensus_genome_id)
    );

end
$$;


rollback;
