-- Deploy seattleflu/id3c-customizations:warehouse/site/data to pg
-- requires: seattleflu/schema:warehouse/site

begin;

insert into warehouse.site(identifier, details)
    values
        ('BirchTreeAcademy',                        '{"category": "community",  "type": "childcare"}'),
        ('CapitolHillLightRailStation',             '{"category": "community",  "type": "publicSpace",
            "swab_site": "%cap(it[ao]l)?_hill%"}'),
        ('ChildcareCenter70thAndSandPoint',         '{"category": "community",  "type": "childcare",
            "swab_site": "cc_sand_point"}'),
        ('ChildcareSwabNSend',                      '{"category": "community",  "type": "childcare-study"}'),
        ('ChildrensHospitalBellevue',               '{"category": "clinic",     "type": "clinic"}'),
        ('ChildrensHospitalSeattle',                '{"category": "clinic",     "type": "clinic",
            "swab_site": "sch_ed"}'),
        ('ChildrensHospitalSeattleOutpatientClinic','{"category": "clinic",     "type": "clinic",
            "swab_site": "ballard_clinic|community_clinic"}'),
        ('ChildrensSeaMar',                         '{"category": "clinic",     "type": "clinic"}'),
        ('ColumbiaCenter',                          '{"category": "community",  "type": "workplace",
            "swab_site": "columbia_center"}'),
        ('Costco',                                  '{"category": "community",  "type": "workplace"}'),
        ('DeniseLouieBeaconHill',                   '{"category": "community",  "type": "childcare"}'),
        ('DeniseLouieMercyMagnusonPl',              '{"category": "community",  "type": "childcare"}'),
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
        ('KaiserPermanente',                        '{"category": "clinic",     "type": "clinic",
            "sample_origin": "kp"}'),
        ('KingStreetStation',                       '{"category": "community",  "type": "publicSpace",
            "swab_site": "king_street%"}'),
        ('MightyKidz',                              '{"category": "community",  "type": "childcare"}'),
        ('MinorAvenueChildrensHouse',               '{"category": "community",  "type": "childcare"}'),
        ('MothersPlace',                            '{"category": "community",  "type": "childcare"}'),
        ('PICAWA',                                  '{"category": "community",  "type": "publicSpace"}'),
        ('PioneerSquare',                           '{"category": "clinic",     "type": "clinic"}'),
        ('RetrospectiveChildrensHospitalSeattle',   '{"category": "hospital",   "type": "retrospective",
            "sample_origin": "sch_retro"}'),
        ('RetrospectiveHarborview',                 '{"category": "hospital",   "type": "retrospective",
            "sample_origin": "hmc_retro"}'),
        ('RetrospectiveNorthwest',                  '{"category": "hospital",   "type": "retrospective",
            "sample_origin": "nwh_retro"}'),
        ('RetrospectivePHSKC',                      '{"category": "community",  "type": "retrospective",
            "sample_origin": "phskc_retro"}'),
        ('RetrospectiveUWMedicalCenter',            '{"category": "hospital",   "type": "retrospective",
            "sample_origin": "uwmc_retro"}'),
        ('SCAN',                                    '{"category": "community", "type": "SCAN",
            "sample_origin": "scan"}'),
        ('SeaTacDomestic',                          '{"category": "community",  "type": "publicSpace",
            "swab_site": "seatac_airport"}'),
        ('SeaTacInternational',                     '{"category": "community",  "type": "publicSpace"}'),
        ('SeattleCenter',                           '{"category": "community",  "type": "publicSpace"}'),
        ('self-test',                               '{"category": "community",  "type": "self-test",
            "swab_site": "home_test"}'),
        ('StMartins',                               '{"category": "community",  "type": "shelter"}'),
        ('swabNSend',                               '{"category": "community",  "type": "swab-n-send",
            "swab_site": "swab_and_send"}'),
        ('TinyTotsDevelopmentCenterEast',           '{"category": "community",  "type": "childcare"}'),
        ('TinyTotsDevelopmentCenterMain',           '{"category": "community",  "type": "childcare"}'),
        ('UWBothell',                               '{"category": "community",  "type": "collegeCampus"}'),
        ('UWChildrensCenterLaurelVillage',          '{"category": "community",  "type": "childcare"}'),
        ('UWChildrensCenterPortageBay',             '{"category": "community",  "type": "childcare"}'),
        ('UWChildrensCenterRadfordCourt',           '{"category": "community",  "type": "childcare",
            "swab_site": "cc_radford"}'),
        ('UWChildrensCenterWestCampus',             '{"category": "community",  "type": "childcare"}'),
        ('UWClub',                                  '{"category": "community",  "type": "collegeCampus"}'),
        ('UWDaycare',                               '{"category": "community",  "type": "childcare"}'),
        ('UWGreek',                                 '{"category": "community",  "type": "collegeCampus",
            "swab_site": "uw_greek"}'),
        ('UWHallHealth',                            '{"category": "clinic",     "type": "clinic",
            "swab_site": "hall_health"}'),
        ('UWOdegaardLibrary',                       '{"category": "community",  "type": "collegeCampus"}'),
        ('UWSeaMar',                                '{"category": "clinic",     "type": "clinic",
            "swab_site": "sea_mar"}'),
        ('UWSouthLakeUnion',                        '{"category": "community",  "type": "collegeCampus"}'),
        ('UWSuzzalloLibrary',                       '{"category": "community",  "type": "collegeCampus",
            "swab_site": "%suzzal+o"}'),
        ('UWReopeningSwabNSend',                    '{"category": "community",  "type": "uw-reopening"}'),
        ('UWTacoma',                                '{"category": "community",  "type": "collegeCampus"}'),
        ('WestCampusChildCareCenter',               '{"category": "community",  "type": "childcare"}'),
        ('WestlakeLightRailStation',                '{"category": "community",  "type": "publicSpace"}'),
        ('WestlakeMall',                            '{"category": "community",  "type": "publicSpace"}')

    on conflict (identifier) do update
        set details = EXCLUDED.details
;
    
commit;
