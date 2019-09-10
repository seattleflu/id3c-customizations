-- Deploy seattleflu/id3c-data:warehouse/target/data to pg
-- requires: seattleflu/schema:warehouse/target/organism

begin;

insert into warehouse.organism (lineage, identifiers, details)
    values
        ('Bordetella_pertussis',    'NCBITAXON => 520',         '{"report_to_public_health": true}'),
        ('Influenza',               null,                       null),
        ('Influenza.A',             'NCBITAXON => 11320',       null),
        ('Influenza.A.H1N1',        'NCBITAXON => 114727',      null),
        ('Influenza.A.H3N2',        'NCBITAXON => 119210',      null),
        ('Influenza.B',             'NCBITAXON => 11520',       null),
        ('Influenza.B.Vic',         null,                       null),
        ('Influenza.B.Yam',         null,                       null),
        ('Influenza.C',             'NCBITAXON => 11552',       null),
        ('Measles',                 'NCBITAXON => 11234',       '{"report_to_public_health": true}'),
        ('Mumps',                   'NCBITAXON => 1979165',     '{"report_to_public_health": true}'),
        ('RSV',                     'NCBITAXON => 11250',       null),
        ('RSV.A',                   'NCBITAXON => 208893',      null),
        ('RSV.B',                   'NCBITAXON => 208895',      null)

    on conflict (lineage) do update
        set identifiers = coalesce(organism.identifiers, '') || EXCLUDED.identifiers,
            details     = coalesce(organism.details, '{}')   || EXCLUDED.details
;

with target_lineage (identifier, lineage) as (
    values
        ('B.pertussis', 'Bordetella_pertussis'::ltree),
        ('Flu_A_H1',    'Influenza.A.H1N1'),
        ('Flu_A_H3',    'Influenza.A.H3N2'),
        ('Flu_A_pan',   'Influenza.A'),
        ('Flu_B_pan',   'Influenza.B'),
        ('Measles',     'Measles'),
        ('Mumps',       'Mumps'),
        ('RSVA',        'RSV.A'),
        ('RSVB',        'RSV.B')
)
insert into warehouse.target (identifier, organism_id, control)

    select identifier, organism_id, false
      from target_lineage
      join warehouse.organism using (lineage)

    on conflict (identifier) do update
        set organism_id = EXCLUDED.organism_id
;

commit;
