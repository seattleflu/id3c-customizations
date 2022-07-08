-- Deploy seattleflu/id3c-customizations:warehouse/site/data to pg
-- requires: seattleflu/schema:warehouse/site

begin;

insert into warehouse.site(identifier, details)
    values
        ('AIRSSwabNSend',                           '{"category": "community",  "type": "swab-n-send",
            "sample_origin": "airs"}'),
        ('Apple',                                   '{"category": "community",  "type": "apple-study", 
            "sample_origin": "apple"}'),
        ('AvalonFederalWayCareCenter',              '{"category": "community",  "type": "skilledNursingFacility", 
            "swab_site": "avalon"}'),
        ('BirchTreeAcademy',                        '{"category": "community",  "type": "childcare"}'),
        ('BoeingField',                             '{"category": "community",  "type": "shelter", 
            "swab_site": "boeing"}'),
        ('BurienNursingAndRehab',                   '{"category": "community",  "type": "skilledNursingFacility", 
            "swab_site": "burien_nursing"}'),
        ('CapitolHillLightRailStation',             '{"category": "community",  "type": "publicSpace",
            "swab_site": "%cap(it[ao]l)?_hill%"}'),
        ('ChildcareCenter70thAndSandPoint',         '{"category": "community",  "type": "childcare",
            "swab_site": "cc_sand_point"}'),
        ('ChildcareSwabNSend',                      '{"category": "community",  "type": "childcare-study",
            "sample_origin": "childcare"}'),
        ('ChildrensHospitalBellevue',               '{"category": "clinic",     "type": "clinic"}'),
        ('ChildrensHospitalSeattle',                '{"category": "clinic",     "type": "clinic",
            "swab_site": "sch_ed"}'),
        ('ChildrensHospitalSeattleOutpatientClinic','{"category": "clinic",     "type": "clinic",
            "swab_site": "ballard_clinic|community_clinic"}'),
        ('ChildrensSeaMar',                         '{"category": "clinic",     "type": "clinic"}'),
        ('ClementPlace',                            '{"category": "community",  "type": "shelter",
            "swab_site": "clement"}'),
        ('ClinicalAdultFamilyHomes',                '{"category": "clinic",     "type": "adultFamilyHome",
            "sample_origin": "clinical_afh"}'),
        ('ClinicalWorkplace',                       '{"category": "clinic",     "type": "workplace",
            "sample_origin": "clinical_workplace"}'),
        ('ColumbiaLutheranHome',                    '{"category": "community",  "type": "skilledNursingFacility", 
            "swab_site": "columbia_lutheran"}'),
        ('CompassBlaineCenter',                     '{"category": "community",  "type": "shelter",
            "swab_site": "blaine_center"}'),
        ('CompassFirstPresbyterian',                '{"category": "community",  "type": "shelter",
            "swab_site": "first_presbyterian"}'),
        ('CompassJanAndPetersPlace',                '{"category": "community",  "type": "shelter",
            "swab_site": "jan_and_peters_place"}'),
        ('CompassOttosPlace3rdFloor',               '{"category": "community",  "type": "shelter", 
            "swab_site": "otto_3"}'),
        ('CompassOttosPlace4thAnd5thFloor',         '{"category": "community",  "type": "shelter", 
            "swab_site": "otto_4_5"}'),
        ('ColumbiaCenter',                          '{"category": "community",  "type": "workplace",
            "swab_site": "columbia_center"}'),
        ('Costco',                                  '{"category": "community",  "type": "workplace"}'),
        ('CristaRehabAndSkilledNursingCare',        '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "crista"}'),
        ('DeniseLouieBeaconHill',                   '{"category": "community",  "type": "childcare"}'),
        ('DeniseLouieMercyMagnusonPl',              '{"category": "community",  "type": "childcare"}'),
        ('DESC',                                    '{"category": "community",  "type": "shelter",
            "swab_site": "desc"}'),
        ('ExhibitionHall',                          '{"category": "community",  "type": "shelter",
            "swab_site": "exhibition_hall"}'),
        ('FredHutchLobby',                          '{"category": "community",  "type": "workplace",
            "swab_site": "fred_hutch"}'),
        ('FryeApartments',                          '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "frye"}'),
        ('KlineGallandHome',                        '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "kline_galland"}'),
        ('Harborview',                              '{"category": "hospital",   "type": "longitudinalInpatient"}'),
        ('HarborviewLobby',                         '{"category": "community",  "type": "workplace",
            "swab_site": "hmc_lobby"}'),
        ('HealthSciencesLobby',                     '{"category": "community",  "type": "collegeCampus"}'),
        ('HealthSciencesRotunda',                   '{"category": "community",  "type": "collegeCampus"}'),
        ('Household',                               '{"category": "community",  "type": "household",
            "swab_site": "household|hh_%"}'),
        ('HUB',                                     '{"category": "community",  "type": "collegeCampus",
            "swab_site": "hub"}'),
        ('HutchKids',                               '{"category": "community",  "type": "childcare"}'),
        ('IssaquahNursingAndRehab',                 '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "issaquah_nursing"}'),
        ('JudsonPark',                              '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "judson_park"}'),
        ('JunctionPoint',                           '{"category": "community",  "type": "shelter",
            "swab_site": "the_junction|junction_point"}'),
        ('KaiserPermanente',                        '{"category": "clinic",     "type": "clinic",
            "sample_origin": "kp"}'),
        ('KingStreetStation',                       '{"category": "community",  "type": "publicSpace",
            "swab_site": "king_street%"}'),
        ('Lazarus',                                 '{"category": "community",  "type": "shelter",
            "swab_site": "lazarus"}'),
        ('LifeCareCenter',                          '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "life_care"}'),
        ('Marys_PlaceBurien',                       '{"category": "community",  "type": "shelter",
            "swab_site": "marys_place_burien"}'),
        ('Marys_PlaceNorthSeattle',                 '{"category": "community",  "type": "shelter",
            "swab_site": "marys_place_north_seattle"}'),
        ('Marys_PlaceNorthshore',                   '{"category": "community",  "type": "shelter",
            "swab_site": "marys_place_northshore"}'),
        ('MarysPlaceRegrade',                       '{"category": "community",  "type": "shelter", 
            "swab_site": "regrade"}'),
        ('MarysPlaceWhiteCenter',                  '{"category": "community",   "type": "shelter",
            "swab_site": "marys_place_white_center"}'),
        ('Marys_PlaceYesler',                       '{"category": "community",  "type": "shelter",
            "swab_site": "marys_place_yesler"}'),
        ('MightyKidz',                              '{"category": "community",  "type": "childcare"}'),
        ('MinorAvenueChildrensHouse',               '{"category": "community",  "type": "childcare"}'),
        ('MissionHealthcare',                       '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "mission"}'),
        ('MothersPlace',                            '{"category": "community",  "type": "childcare"}'),
        ('Oaks',                                    '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "oaks"}'),
        ('ParkWestCareCenter',                      '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "park_west_care"}'),
        ('PICAWA',                                  '{"category": "community",  "type": "publicSpace"}'),
        ('PioneerSquare',                           '{"category": "clinic",     "type": "clinic"}'),
        ('ProvidenceMountStVincent',                '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "providence"}'),
        ('RedLionHotel',                            '{"category": "community",  "type": "shelter",
            "swab_site": "red_lion"}'),
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
        ('ROOTS',                                   '{"category": "community",  "type": "shelter",
            "swab_site": "roots"}'),
        ('ROSEY',                                   '{"category": "community",  "type": "school",
            "sample_origin": "radxup_yakima"}'),
        ('QueenAnneHealthcare',                     '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "queen_anne_healthcare"}'),
        ('SCAN',                                    '{"category": "community",  "type": "SCAN",
            "sample_origin": "scan"}'),
        ('SeaTacDomestic',                          '{"category": "community",  "type": "publicSpace",
            "swab_site": "seatac_airport"}'),
        ('SeaTacInternational',                     '{"category": "community",  "type": "publicSpace"}'),
        ('SeattleCenter',                           '{"category": "community",  "type": "publicSpace"}'),
        ('SeattleMedicalPostAcuteCare',             '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "seattle_medical"}'),
        ('self-test',                               '{"category": "community",  "type": "self-test",
            "swab_site": "home_test"}'),
        ('ShorelineHealthAndRehab',                 '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "shoreline_health"}'),
        ('SnohomishSchoolDistrict',                 '{"category": "community",  "type": "school",
            "sample_origin": "snohomish_schools"}'),
        ('Spruce',                                  '{"category": "community",  "type": "shelter",
            "swab_site": "spruce"}'),
        ('StaffordHealthcare',                      '{"category": "community",  "type": "skilledNursingFacility",
            "swab_site": "stafford"}'),
        ('StMartins',                               '{"category": "community",  "type": "shelter",
            "swab_site": "saint_martins"}'),
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
        ('UWClub',                                  '{"category": "community",  "type": "collegeCampus",
            "swab_site": "uw_club"}'),
        ('UWDaycare',                               '{"category": "community",  "type": "childcare"}'),
        ('UWGreek',                                 '{"category": "community",  "type": "collegeCampus",
            "swab_site": "uw_greek"}'),
        ('UWHallHealth',                            '{"category": "clinic",     "type": "clinic",
            "swab_site": "hall_health"}'),
        ('UWOdegaardLibrary',                       '{"category": "community",  "type": "collegeCampus",
            "swab_site": "ode"}'),
        ('UWSeaMar',                                '{"category": "clinic",     "type": "clinic",
            "swab_site": "sea_mar"}'),
        ('UWSouthLakeUnion',                        '{"category": "community",  "type": "collegeCampus",
            "swab_site": "uw_slu"}'),
        ('UWSuzzalloLibrary',                       '{"category": "community",  "type": "collegeCampus",
            "swab_site": "%suzzal+o"}'),
        ('UWReopeningSwabNSend',                    '{"category": "community",  "type": "uw-reopening"}'),
        ('UWReopeningDropbox',                      '{"category": "community",  "type": "uw-reopening",
            "swab_site": "uw_box"}'),
        ('UWTacoma',                                '{"category": "community",  "type": "collegeCampus"}'),
        ('WestCampusChildCareCenter',               '{"category": "community",  "type": "childcare"}'),
        ('WestlakeLightRailStation',                '{"category": "community",  "type": "publicSpace"}'),
        ('WestlakeMall',                            '{"category": "community",  "type": "publicSpace"}'),
        ('YouthCare',                               '{"category": "community",  "type": "shelter",                                 
            "swab_site": "youthcare"}')

    on conflict (identifier) do update
        set details = EXCLUDED.details
;

commit;