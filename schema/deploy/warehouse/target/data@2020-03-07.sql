-- Deploy seattleflu/id3c-data:warehouse/target/data to pg
-- requires: seattleflu/schema:warehouse/target/organism

begin;

insert into warehouse.organism (lineage, identifiers, details)
    values
        ('Adenovirus',              'NCBITAXON => 10508',       null),
        ('Bordetella_pertussis',    'NCBITAXON => 520',         '{"report_to_public_health": true}'),
        ('Chlamydophila_pneumoniae','NCBITAXON => 83558',       null),
        ('Enterovirus',             'NCBITAXON => 12059',       null),
        ('Enterovirus.D',           'NCBITAXON => 138951',      null),
        ('Enterovirus.D.68',        'NCBITAXON => 42789',       null),
        ('Rhinovirus',              null,                       null),
        ('Human_bocavirus',         'NCBITAXON => 329641',      null),
        ('Human_coronavirus',       null,                       null),
        ('Human_coronavirus.HKU1',  'NCBITAXON => 290028',      null),
        ('Human_coronavirus.NL63',  'NCBITAXON => 277944',      null),
        ('Human_coronavirus.229E',  'NCBITAXON => 11137',       null),
        ('Human_coronavirus.OC43',  'NCBITAXON => 31631',       null),
        ('Human_coronavirus.2019',  'NCBITAXON => 2697049',     '{"report_to_public_health": true}'),
        ('Human_metapneumovirus',   'NCBITAXON => 162145',      null),
        ('Human_parainfluenza',     null,                       null),
        ('Human_parainfluenza.1',   'NCBITAXON => 12730',       null),
        ('Human_parainfluenza.2',   'NCBITAXON => 1979160',     null),
        ('Human_parainfluenza.3',   'NCBITAXON => 11216',       null),
        ('Human_parainfluenza.4',   'NCBITAXON => 1979161',     null),
        ('Human_parechovirus',      'NCBITAXON => 1803956',     null),
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
        ('Mycoplasma_pneumoniae',   'NCBITAXON => 2104',        null),
        ('RSV',                     'NCBITAXON => 11250',       null),
        ('RSV.A',                   'NCBITAXON => 208893',      null),
        ('RSV.B',                   'NCBITAXON => 208895',      null),
        ('Streptococcus_pneumoniae','NCBITAXON => 1313',        null)


    on conflict (lineage) do update
        set identifiers = coalesce(organism.identifiers, '') || EXCLUDED.identifiers,
            details     = coalesce(organism.details, '{}')   || EXCLUDED.details
;

delete from warehouse.organism
    where lineage = 'Human_coronavirus.2019_nCoV';

with target_lineage (identifier, lineage) as (
    values
        -- Targets reported by NWGC from Thermo Fisher's OpenArray cards
        ('Adenovirus_pan_1',            'Adenovirus'::ltree),
        ('Adenovirus_pan_2',            'Adenovirus'),
        ('AdV_1of2',                    'Adenovirus'),
        ('AdV_2of2',                    'Adenovirus'),
        ('AI20U8U',                     'Bordetella_pertussis'),
        ('B.pertussis',                 'Bordetella_pertussis'),
        ('B.Pertussis',                 'Bordetella_pertussis'),
        ('AI1RW2H',                     'Chlamydophila_pneumoniae'),
        ('C_pneumoniae',                'Chlamydophila_pneumoniae'),
        ('C. pneumoniae',               'Chlamydophila_pneumoniae'),
        ('C. pneumoniae_AI1RW2H',       'Chlamydophila_pneumoniae'),
        ('C.pneumoniae',                'Chlamydophila_pneumoniae'),
        ('AP7DPVF',                     'Enterovirus'),
        ('EnterovirusA_B 1_AP7DPVF',    'Enterovirus'),
        ('Enterovirus_pan',             'Enterovirus'),
        ('Ev_pan',                      'Enterovirus'),
        ('EV_pan',                      'Enterovirus'),
        ('APFVK4U',                     'Enterovirus.D.68'),
        ('enterovirus-D_APFVK4U',       'Enterovirus.D.68'),
        ('Enterovirus-D_APFVK4U',       'Enterovirus.D.68'),
        ('EV_D68',                      'Enterovirus.D.68'),
        ('Bocavirus',                   'Human_bocavirus'),
        ('HBoV',                        'Human_bocavirus'),
        ('CoV_229E_CoV_OC43',           'Human_coronavirus'),
        ('CoV_HKU1_CoV_NL63',           'Human_coronavirus'),
        ('COVID-19',                    'Human_coronavirus.2019'),
        ('nCoV',                        'Human_coronavirus.2019'),
        ('CoV_HKU1',                    'Human_coronavirus.HKU1'),
        ('CoV_NL63',                    'Human_coronavirus.NL63'),
        ('CoV_229E',                    'Human_coronavirus.229E'),
        ('CoV_OC43',                    'Human_coronavirus.OC43'),
        ('hMPV',                        'Human_metapneumovirus'),
        ('hPIV1_hPIV2',                 'Human_parainfluenza'),
        ('hPIV3_hPIV4',                 'Human_parainfluenza'),
        ('hPIV1',                       'Human_parainfluenza.1'),
        ('hPIV2',                       'Human_parainfluenza.2'),
        ('hPIV3',                       'Human_parainfluenza.3'),
        ('hPIV4',                       'Human_parainfluenza.4'),
        ('HPeV',                        'Human_parechovirus'),
        ('flu_A_pan',                   'Influenza.A'),
        ('Flu_A_pan',                   'Influenza.A'),
        ('Flu_A_H1',                    'Influenza.A.H1N1'),
        ('Flu_A_H3',                    'Influenza.A.H3N2'),
        ('Flu_b_pan',                   'Influenza.B'),
        ('Flu_B_pan',                   'Influenza.B'),
        ('Influenza_B',                 'Influenza.B'),
        ('AP324NU',                     'Influenza.C'),
        ('Measles',                     'Measles'),
        ('APKA3DE',                     'Mumps'),
        ('Mumps',                       'Mumps'),
        ('AI5IRK5',                     'Mycoplasma_pneumoniae'),
        ('M_pneumoniae',                'Mycoplasma_pneumoniae'),
        ('M. pneumoniae',               'Mycoplasma_pneumoniae'),
        ('M. pneumoniae_AI5IRK5',       'Mycoplasma_pneumoniae'),
        ('M.pneumoniae',                'Mycoplasma_pneumoniae'),
        ('11 Rhinovirus_pan_1',         'Rhinovirus'),
        ('12 Rhinovirus_pan_2',         'Rhinovirus'),
        ('RV_1of2',                     'Rhinovirus'),
        ('RV_2of2',                     'Rhinovirus'),
        ('RSVA',                        'RSV.A'),
        ('RSVB',                        'RSV.B'),
        ('APZTD4A',                     'Streptococcus_pneumoniae'),
        ('S_pneumoniae',                'Streptococcus_pneumoniae'),
        ('S. pneumoniae',               'Streptococcus_pneumoniae'),
        ('S. pneumoniae_APZTD4A',       'Streptococcus_pneumoniae'),
        ('S.pneumoniae',                'Streptococcus_pneumoniae'),

        -- Targets reported by Ellume & Cepheid using SNOMED CT
        ('http://snomed.info/id/181000124108', 'Influenza.A'),
        ('http://snomed.info/id/441345003',    'Influenza.B'),
        ('http://snomed.info/id/441278007',    'RSV')
)
insert into warehouse.target (identifier, organism_id, control)

    select identifier, organism_id, false
      from target_lineage
      join warehouse.organism using (lineage)

    on conflict (identifier) do update
        set organism_id = EXCLUDED.organism_id
;


insert into warehouse.target (identifier, control)
    values
        ('Hs04930436_g1',   't'),
        ('Ac00010014_a1',   't')

    on conflict (identifier) do update
        set control = EXCLUDED.control
;

commit;
