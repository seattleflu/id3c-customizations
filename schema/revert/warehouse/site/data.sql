-- Deploy seattleflu/id3c-customizations:warehouse/site/data to pg
-- requires: seattleflu/schema:warehouse/site

begin;

insert into warehouse.site(identifier, details)
    values
        ('CapitolHillLightRailStation',             '{"category": "community",  "type": "publicSpace",
            "swab_site": "%cap(it[ao]l)?_hill%"}'),
        ('ChildrensHospitalBellevue',               '{"category": "clinic",     "type": "clinic"}'),
        ('ChildrensHospitalSeattle',                '{"category": "clinic",     "type": "clinic",
            "swab_site": "bal%_clinic|sch_ed"}'),
        ('ChildrensSeaMar',                         '{"category": "clinic",     "type": "clinic"}'),
        ('ColumbiaCenter',                          '{"category": "community",  "type": "workplace",
            "swab_site": "columbia_center"}'),
        ('Costco',                                  '{"category": "community",  "type": "workplace"}'),
        ('DESC',                                    '{"category": "community",  "type": "shelter",
            "swab_site": "desc"}'),
        ('FredHutchLobby',                          '{"category": "community",  "type": "workplace",
            "swab_site": "fred_hutch"}'),
        ('Harborview',                              '{"category": "hospital",   "type": "longitudinalInpatient"}'),
        ('HarborviewLobby',                         '{"category": "community",  "type": "workplace",
            "swab_site": "hmc_lobby"}'),
        ('HealthSciencesLobby',                     '{"category": "community",  "type": "collegeCampus"}'),
        ('HealthSciencesRotunda',                   '{"category": "community",  "type": "collegeCampus"}'),
        ('HUB',                                     '{"category": "community",  "type": "collegeCampus",
            "swab_site": "hub"}'),
        ('HutchKids',                               '{"category": "community",  "type": "childcare"}'),
        ('KaiserPermanente',                        '{"category": "clinic",     "type": "clinic"}'),
        ('KingStreetStation',                       '{"category": "community",  "type": "publicSpace",
            "swab_site": "king_street%"}'),
        ('PioneerSquare',                           '{"category": "clinic",     "type": "clinic"}'),
        ('RetrospectiveChildrensHospitalSeattle',   '{"category": "hospital",   "type": "retrospective",
            "sample_origin": "sch_retro"}'),
        ('RetrospectiveHarborview',                 '{"category": "hospital",   "type": "retrospective",
            "sample_origin": "hmc_retro"}'),
        ('RetrospectiveNorthwest',                  '{"category": "hospital",   "type": "retrospective",
            "sample_origin": "nwh_retro"}'),
        ('RetrospectiveUWMedicalCenter',            '{"category": "hospital",   "type": "retrospective",
            "sample_origin": "uwmc_retro"}'),
        ('SeaTacDomestic',                          '{"category": "community",  "type": "publicSpace",
            "swab_site": "seatac_airport"}'),
        ('SeaTacInternational',                     '{"category": "community",  "type": "publicSpace"}'),
        ('SeattleCenter',                           '{"category": "community",  "type": "publicSpace"}'),
        ('self-test',                               '{"category": "community",  "type": "self-test"}'),
        ('StMartins',                               '{"category": "community",  "type": "shelter"}'),
        ('swabNSend',                               '{"category": "community",  "type": "swab-n-send",
            "swab_site": "swab_and_send"}'),
        ('UWDaycare',                               '{"category": "community",  "type": "childcare"}'),
        ('UWHallHealth',                            '{"category": "clinic",     "type": "clinic",
            "swab_site": "hall_health"}'),
        ('UWSeaMar',                                '{"category": "clinic",     "type": "clinic",
            "swab_site": "sea_mar"}'),
        ('UWSuzzalloLibrary',                       '{"category": "community",  "type": "collegeCampus",
            "swab_site": "%suzzal+o"}'),
        ('WestCampusChildCareCenter',               '{"category": "community",  "type": "childcare"}'),
        ('WestlakeLightRailStation',                '{"category": "community",  "type": "publicSpace"}'),
        ('WestlakeMall',                            '{"category": "community",  "type": "publicSpace"}')

    on conflict (identifier) do update
        set details = EXCLUDED.details
;

delete from warehouse.site
  where identifier = 'ChildrensHospitalSeattleOutpatientClinic'
;

commit;
