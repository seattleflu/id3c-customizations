-- Deploy seattleflu/id3c-customizations:warehouse/site/data to pg
-- requires: seattleflu/schema:warehouse/site

begin;

insert into warehouse.site(identifier, details)
    values
        ('CapitolHillLightRailStation',             '{"category": "community",  "type": "publicSpace"}'),
        ('ChildrensHospitalBellevue',               '{"category": "clinic",     "type": "clinic"}'),
        ('ChildrensHospitalSeattle',                '{"category": "clinic",     "type": "clinic"}'),
        ('ChildrensSeaMar',                         '{"category": "clinic",     "type": "clinic"}'),
        ('ColumbiaCenter',                          '{"category": "community",  "type": "workplace"}'),
        ('Costco',                                  '{"category": "community",  "type": "workplace"}'),
        ('DESC',                                    '{"category": "community",  "type": "shelter"}'),
        ('FredHutchLobby',                          '{"category": "community",  "type": "workplace"}'),
        ('Harborview',                              '{"category": "hospital",   "type": "longitudinalInpatient"}'),
        ('HarborviewLobby',                         '{"category": "community",  "type": "workplace"}'),
        ('HealthSciencesLobby',                     '{"category": "community",  "type": "collegeCampus"}'),
        ('HealthSciencesRotunda',                   '{"category": "community",  "type": "collegeCampus"}'),
        ('HUB',                                     '{"category": "community",  "type": "collegeCampus"}'),
        ('HutchKids',                               '{"category": "community",  "type": "childcare"}'),
        ('KaiserPermanente',                        '{"category": "clinic",     "type": "clinic"}'),
        ('KingStreetStation',                       '{"category": "community",  "type": "publicSpace"}'),
        ('PioneerSquare',                           '{"category": "clinic",     "type": "clinic"}'),
        ('RetrospectiveChildrensHospitalSeattle',   '{"category": "hospital",   "type": "retrospective"}'),
        ('RetrospectiveHarborview',                 '{"category": "hospital",   "type": "retrospective"}'),
        ('RetrospectiveNorthwest',                  '{"category": "hospital",   "type": "retrospective"}'),
        ('RetrospectiveUWMedicalCenter',            '{"category": "hospital",   "type": "retrospective"}'),
        ('SCHSeaMar',                               '{"category": "clinic",     "type": "clinic"}'),
        ('SeaTacDomestic',                          '{"category": "community",  "type": "publicSpace"}'),
        ('SeaTacInternational',                     '{"category": "community",  "type": "publicSpace"}'),
        ('SeattleCenter',                           '{"category": "community",  "type": "publicSpace"}'),
        ('self-test',                               '{"category": "community",  "type": "self-test"}'),
        ('StMartins',                               '{"category": "community",  "type": "shelter"}'),
        ('swabNSend',                               '{"category": "community",  "type": "swab-n-send"}'),
        ('UWDaycare',                               '{"category": "community",  "type": "childcare"}'),
        ('UWHallHealth',                            '{"category": "clinic",     "type": "clinic"}'),
        ('UWSeaMar',                                '{"category": "clinic",     "type": "clinic"}'),
        ('UWSuzzalloLibrary',                       '{"category": "community",  "type": "collegeCampus"}'),
        ('WestCampusChildCareCenter',               '{"category": "community",  "type": "childcare"}'),
        ('WestlakeLightRailStation',                '{"category": "community",  "type": "publicSpace"}'),
        ('WestlakeMall',                            '{"category": "community",  "type": "publicSpace"}')

    on conflict (identifier) do update
        set details = coalesce(site.details, '{}') || EXCLUDED.details
;

commit;
