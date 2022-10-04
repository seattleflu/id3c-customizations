"""
Process DETs for the Fred Hutch AIRS study into the FHIR receiving table
"""
import logging
import os

from dateutil.relativedelta import relativedelta
from enum import Enum
from id3c.cli.command.etl import redcap_det
from id3c.cli.redcap import is_complete, Record as REDCapRecord
from id3c.db import find_identifier
from seattleflu.id3c.cli.command import age_ceiling
from .redcap import *
from .redcap_map import UnknownSexError

LOG = logging.getLogger(__name__)


class AIRSProject():
    id: int
    lang: str
    command_name: str

    def __init__(self, project_id: int, lang: str, command_name: str) -> None:
        self.id = project_id
        self.lang = lang
        self.command_name = command_name


REDCAP_URL  = os.environ.get('FH_AIRS_REDCAP_API_URL') # REDCap server hostname
REDCAP_PID  = 1372                                 # REDCap project ID
REDCAP_LANG = 'en'                                 # Survey language
ETL_COMMAND = 'fh-airs'                            # Run as 'id3c etl redcap-det <ETL_COMMAND>'

PROJECTS = [
        AIRSProject(REDCAP_PID, REDCAP_LANG, ETL_COMMAND)
    ]

if not REDCAP_URL:
    REDCAP_URL = 'https://redcap.fredhutch.org/'

LANGUAGE_CODE = {
project.id: project.lang
    for project in PROJECTS }

class EventType(Enum):
    ENROLLMENT = 'enrollment'
    ENCOUNTER = 'encounter'

class EncounterType(Enum):
    WEEKLY = 'weekly'
    FOLLOW_UP = 'follow-up'

class SwabKitInstrumentSet(Enum):
    FIRST = 1
    SECOND = 2

REVISION = 2

INTERNAL_SYSTEM = "https://seattleflu.org"
ENROLLMENT_EVENT_NAME = "screening_and_enro_arm_1"
ENCOUNTER_EVENT_NAMES = [
    "week_01_arm_1",
    "week_02_arm_1",
    "week_03_arm_1",
    "week_04_arm_1",
    "week_05_arm_1",
    "week_06_arm_1",
    "week_07_arm_1",
    "week_08_arm_1",
    "week_09_arm_1",
    "week_10_arm_1",
    "week_11_arm_1",
    "week_11_arm_1",
    "week_12_arm_1",
    "week_13_arm_1",
    "week_14_arm_1",
    "week_15_arm_1",
    "week_16_arm_1",
    "week_17_arm_1",
    "week_18_arm_1",
    "week_19_arm_1",
    "week_20_arm_1",
    "week_21_arm_1",
    "week_22_arm_1",
    "week_23_arm_1",
    "week_24_arm_1",
    "week_25_arm_1",
    "week_26_arm_1",
]

SWAB_AND_SEND_SITE = 'AIRSSwabNSend'

REQUIRED_ENROLLMENT_INSTRUMENTS = [
    'screening',
    'screening_call',
    'informed_consent',
    'enrollment',
]


def command_for_each_project(function):
    """
    A decorator to register one redcap-det subcommand per REDCap project, each
    calling the same base *function*.

    Used for side-effects only; the original *function* is unmodified.
    """

    for project in PROJECTS:
        help_message = "Process REDCap DETs for Fred Hutch AIRS study"

        redcap_det.command_for_project(
            name = project.command_name,
            redcap_url = REDCAP_URL,
            project_id = project.id,
            raw_coded_values = True,
            revision = REVISION,
            help = help_message,
            include_incomplete = True)(function)

    return function


@command_for_each_project
def redcap_det_fh_airs(*, db: DatabaseSession, cache: TTLCache, det: dict,
    redcap_record_instances: List[REDCapRecord]) -> Optional[dict]:

    if redcap_record_instances is None or len(redcap_record_instances) == 0:
        LOG.warning(f"There are no record instances. Skipping record.")
        return None

    enrollments = [record for record in redcap_record_instances if record.event_name == ENROLLMENT_EVENT_NAME]

    if not len(enrollments) == 1:
        LOG.warning(f"There are {len(enrollments)} enrollment instances for record: {redcap_record_instances[0].get('subject_id')}. Skipping record.")
        return None

    enrollment = enrollments[0]

    incomplete_enrollment_instruments = {
                instrument
                    for instrument
                    in REQUIRED_ENROLLMENT_INSTRUMENTS
                    if not is_complete(instrument, enrollment)
            }

    if incomplete_enrollment_instruments:
        LOG.debug(f"The following required enrollment instruments «{incomplete_enrollment_instruments}» are not yet marked complete.")
        return None

    patient_entry, patient_reference = airs_build_patient(enrollment)

    if not patient_entry:
        LOG.warning(f"Skipping record {enrollment.get('subject_id')} with insufficient information to construct patient")
        return None

    birthdate = parse_date_from_string(enrollment.get('enr_dob'))
    if not birthdate:
        LOG.warning(f"Record {enrollment.get('subject_id')} has an invalid or missing `enr_dob` value")

    location_resource_entries = airs_build_location_info(db, cache, enrollment)

    persisted_resource_entries = [patient_entry, *location_resource_entries]

    for redcap_record_instance in redcap_record_instances:

        event_type = None

        if redcap_record_instance.event_name == ENROLLMENT_EVENT_NAME:
            event_type = EventType.ENROLLMENT

        elif redcap_record_instance.event_name in ENCOUNTER_EVENT_NAMES:
            if is_complete('weekly', redcap_record_instance):
                event_type = EventType.ENCOUNTER

                # check to make sure there are not more swab kits than expected
                swab_kits_sent = [is_complete('back_end_mail_scans', redcap_record_instance),  is_complete('back_end_mail_scans_2', redcap_record_instance)].count(True)
                swab_kits_triggered = [redcap_record_instance.get('ss_nasal_swab_needed'), redcap_record_instance.get('wk_nasal_swab_needed')].count("1")

                if swab_kits_sent > swab_kits_triggered:
                    LOG.warning(f"Skipping record id: {redcap_record_instance.id}, "
                        f"encounter: {redcap_record_instance.event_name}; "
                        f"{swab_kits_sent} sets of swab kit instruments detected, but only {swab_kits_triggered} triggered")

                    continue
            else:
                LOG.debug(f"Skipping record id: {redcap_record_instance.id}, "
                    f"encounter: {redcap_record_instance.event_name}; "
                    "insufficient information to construct encounter")
                continue
        else:
            LOG.info(f"Skipping event: {redcap_record_instance.event_name!r} for record "
            f"{redcap_record_instance.get('subject_id')} because the event is not one "
            "that we process")
            continue

        site_reference = create_site_reference(
            location = None,
            site_map = None,
            default_site = SWAB_AND_SEND_SITE,
            system_identifier = INTERNAL_SYSTEM)

        collection_code = CollectionCode.HOME_HEALTH

        # Handle various symptoms.
        contained: List[dict] = []
        diagnosis: List[dict] = []

        if event_type == EventType.ENCOUNTER:
            contained, diagnosis = airs_build_contained_and_diagnosis(
                patient_reference = patient_reference,
                record = redcap_record_instance,
                encounter_type = EncounterType.WEEKLY,
                system_identifier = INTERNAL_SYSTEM)

        encounter_date = airs_get_encounter_date(redcap_record_instance, event_type)

        initial_encounter_entry, initial_encounter_reference = create_encounter(
            encounter_id = create_encounter_id(redcap_record_instance, False),
            encounter_date = encounter_date,
            patient_reference = patient_reference,
            site_reference = site_reference,
            locations = location_resource_entries,
            diagnosis = diagnosis,
            contained = contained,
            collection_code = collection_code,
            system_identifier = INTERNAL_SYSTEM,
            record = redcap_record_instance)

        # Skip the entire record if we can't create the enrollment encounter.
        # Otherwise, just skip the record instance.
        if not initial_encounter_entry:
            if event_type == EventType.ENROLLMENT:
                LOG.warning("Skipping record because we could not create the enrollment encounter for record: "
                    f"{redcap_record_instance.get('subject_id')}")
                return None
            else:
                LOG.warning("Skipping record instance with insufficient information to construct the initial encounter "
                    f"for record: {redcap_record_instance.get('subject_id')}, instance: "
                    f"{redcap_record_instance.get('event_type')}")
                continue

        computed_questionnaire_entry = None
        enrollment_questionnaire_entry = None
        weekly_questionnaire_entry = None
        follow_up_encounter_entry = None
        follow_up_questionnaire_entry = None
        follow_up_computed_questionnaire_entry = None
        specimen_entry, specimen_observation_entry = None, None
        specimen_entry_v2, specimen_observation_entry_v2 = None, None

        computed_questionnaire_entry = airs_create_computed_questionnaire_response(
            redcap_record_instance,
            patient_reference,
            initial_encounter_reference,
            birthdate,
            parse_date_from_string(
                initial_encounter_entry['resource']['period']['start'])
            )

        if event_type == EventType.ENROLLMENT:
            enrollment_questionnaire_entry = airs_create_enrollment_questionnaire_response(
            enrollment, patient_reference, initial_encounter_reference)
        else:
            weekly_questionnaire_entry = airs_create_weekly_questionnaire_response(
                    redcap_record_instance, patient_reference, initial_encounter_reference)

            # build specimen and observation entries if swab kit was triggered, sent, and received
            if redcap_record_instance.get('wk_nasal_swab_needed') == "1" and \
                is_complete('back_end_mail_scans', redcap_record_instance) and \
                is_complete('post_collection_data_entry_qc', redcap_record_instance):
                    (specimen_entry, specimen_observation_entry) = airs_build_specimens(
                        db,
                        patient_reference,
                        initial_encounter_reference,
                        redcap_record_instance,
                        SwabKitInstrumentSet.FIRST,
                    )

            # For the AIRS study, the symptom instrument is being used for the follow-up encounter
            if is_complete('symptom', redcap_record_instance):
                contained, diagnosis = airs_build_contained_and_diagnosis(
                    patient_reference = patient_reference,
                    record = redcap_record_instance,
                    encounter_type = EncounterType.FOLLOW_UP,
                    system_identifier = INTERNAL_SYSTEM)

                # Don't set locations because the weekly survey doesn't ask for home address.
                follow_up_encounter_entry, follow_up_encounter_reference = create_encounter(
                    encounter_id = create_encounter_id(redcap_record_instance, True),
                    encounter_date = extract_date_from_survey_timestamp(redcap_record_instance, 'symptom'),
                    patient_reference = patient_reference,
                    site_reference = site_reference,
                    diagnosis = diagnosis,
                    contained = contained,
                    collection_code = CollectionCode.HOME_HEALTH,
                    parent_encounter_reference = initial_encounter_reference,
                    encounter_reason_code = follow_up_encounter_reason_code(),
                    encounter_identifier_suffix = "_follow_up",
                    system_identifier = INTERNAL_SYSTEM,
                    record = redcap_record_instance)

                follow_up_questionnaire_entry = airs_create_follow_up_questionnaire_response(
                    redcap_record_instance, patient_reference, follow_up_encounter_reference)
                follow_up_computed_questionnaire_entry = airs_create_computed_questionnaire_response(
                    redcap_record_instance, patient_reference, follow_up_encounter_reference,
                    birthdate, parse_date_from_string(follow_up_encounter_entry['resource']['period']['start']))


                # Generate specimen and observation entries for samples triggered by follow-up encounter
                if redcap_record_instance.get('ss_nasal_swab_needed') == "1":

                    # Determine which set of swab kit forms to use. If no kit was triggered by the initial weekly
                    # encounter, the first set of swab kit forms is used by the follow-up encounter
                    if redcap_record_instance.get('wk_nasal_swab_needed') == "1":
                        swab_kit_instrument_set = SwabKitInstrumentSet.SECOND
                    else:
                        swab_kit_instrument_set = SwabKitInstrumentSet.FIRST

                    if (swab_kit_instrument_set == SwabKitInstrumentSet.FIRST and \
                            is_complete('back_end_mail_scans', redcap_record_instance) and \
                            is_complete('post_collection_data_entry_qc', redcap_record_instance)) \
                        or \
                        (swab_kit_instrument_set == SwabKitInstrumentSet.SECOND and \
                            is_complete('back_end_mail_scans_2', redcap_record_instance) and \
                            is_complete('post_collection_data_entry_qc_2', redcap_record_instance)):

                            (specimen_entry_v2, specimen_observation_entry_v2) = airs_build_specimens(
                                db,
                                patient_reference,
                                follow_up_encounter_reference,
                                redcap_record_instance,
                                swab_kit_instrument_set,
                            )



        current_instance_entries = [
            initial_encounter_entry,
            computed_questionnaire_entry,
            enrollment_questionnaire_entry,
            weekly_questionnaire_entry,
            specimen_entry,
            specimen_observation_entry,
            specimen_entry_v2,
            specimen_observation_entry_v2,
            follow_up_encounter_entry,
            follow_up_questionnaire_entry,
            follow_up_computed_questionnaire_entry
        ]

        persisted_resource_entries.extend(list(filter(None, current_instance_entries)))

    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{REDCAP_URL}{enrollment.project.id}/{enrollment.id}",
        entries = list(filter(None, persisted_resource_entries))
    )


def airs_get_encounter_date(record: REDCapRecord, event_type: EventType, follow_up: bool = False) -> Optional[str]:
    encounter_date = None

    if event_type == EventType.ENCOUNTER:
        if follow_up:
            encounter_date = extract_date_from_survey_timestamp(record, 'symptom') \
                or record.get('ss_date')
        else:
            encounter_date = extract_date_from_survey_timestamp(record, 'weekly') \
                or record.get('wk_date')

    elif event_type == EventType.ENROLLMENT:
        encounter_date = extract_date_from_survey_timestamp(record, 'enrollment') \
            or record.get('enr_date_complete')

    else:
        # We should never get here, but we should also never have an
        #  if/elif without an else
        LOG.error(f"Invalid date: {record.event_name!r} for record "
                  f"{record.get('subject_id')} contains event_type {event_type}, "
                  "but that is not one that we process.")

    return encounter_date


def create_encounter_id(record: REDCapRecord, is_followup_encounter: bool) -> str:
    """
    Create the encounter_id from the REDCap *record*.
    """
    if record.event_name:
        redcap_event_name = record.event_name
    else:
        redcap_event_name = ''

    if is_followup_encounter:
        encounter_identifier_suffix = "_follow_up"
    else:
        encounter_identifier_suffix = ''

    return f'{record.project.base_url}{record.project.id}/{record.id}/{redcap_event_name}/' + \
        f'{encounter_identifier_suffix}'


def airs_get_collection_date(record: REDCapRecord, swab_kit_instrument_set: SwabKitInstrumentSet) -> Optional[str]:
    """
    Determine sample/specimen collection date from the given REDCap *record*.
    """
    # For all surveys, try the survey _timestamp field (which is in Pacific time)
    # before custom fields because the custom fields aren't always populated and when
    # they are populated they use the browser's time zone.

    # The swab_kit_instrument_set argument is used to indicate which swab kit instruments to
    # use. The AIRS REDCap project has two matching sets of instruments in each weekly event,
    # with the second set used when two kits are triggered in the same week from the weekly
    # and symptom instruments.
    collection_date = None

    if swab_kit_instrument_set == SwabKitInstrumentSet.SECOND:
        collection_date = record.get("date_on_tube_v2") \
            or extract_date_from_survey_timestamp(record, "airs_kit_activation_2") \
            or record.get("kit_reg_date_v2") \
            or extract_date_from_survey_timestamp(record, "back_end_mail_scans_2") \
            or record.get("back_end_scan_date_v2")
    else:
        collection_date = record.get("date_on_tube") \
            or extract_date_from_survey_timestamp(record, "airs_kit_activation") \
            or record.get("kit_reg_date") \
            or extract_date_from_survey_timestamp(record, "back_end_mail_scans") \
            or record.get("back_end_scan_date")

    return collection_date


def airs_create_enrollment_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the enrollment
    encounter (i.e. encounter of enrollment into the study)
    """
    codebook_map = {
        'enr_outside_hour': {
            '1': 'less_than_10_hours',
            '2': '10_to_less_than_20_hours',
            '3': '20_to_less_than_30_hours',
            '4': '30_to_less_than_40_hours',
            '5': '40_hours_or_more',
            '': None,
        },
        'enr_outside_mask': {
            '1': 'always',
            '2': 'mostly',
            '3': 'sometimes',
            '4': 'rarely',
            '5': 'never',
            '': None,
        },
        'enr_sex': {
            '1': 'male',
            '2': 'female',
            '3': 'other',
            '': None,
        },
        'enr_gender': {
            '1': 'cisgender_man',
            '2': 'cisgender_woman',
            '3': 'genderqueer_non_binary',
            '4': 'man',
            '5': 'transgender_man',
            '6': 'transgender_woman',
            '7': 'woman',
            '8': 'other',
            '': None,
        },
        'enr_degree': {
            '1': 'less_than_high_school',
            '2': 'high_school_or_equivalent',
            '3': 'ged_associate_or_technical_degree',
            '4': 'bachelors_degree',
            '5': 'graduate_degree',
            '6': 'dont_say',
            '': None,
        },
        'enr_ethnicity': {
            '1': 'yes',
            '2': 'no',
            '3': 'dont_say',
            '': None,
        },
        'enr_living_group': {
            '1': 'dormitory_group',
            '2': 'home',
            '3': 'homeless_shelter',
            '4': 'long_term_care_or_skilled_nursing_facility',
            '5': 'other',
            '': None,
        },
        'enr_indoor_no_mask': {
            '1': 'less_than_5_people',
            '2': '5_to_less_than_10_people',
            '3': '10_to_less_than_15_people',
            '4': '15_to_less_than_20_people',
            '5': '20_to_less_than_30_people',
            '6': '30_to_less_than_40_people',
            '7': '40_to_less_than_50_people',
            '8': '50_people_or_more',
            '': None,
        },
        'enr_outdoor_no_mask': {
            '1': 'less_than_5_people',
            '2': '5_to_less_than_10_people',
            '3': '10_to_less_than_15_people',
            '4': '15_to_less_than_20_people',
            '5': '20_to_less_than_30_people',
            '6': '30_to_less_than_40_people',
            '7': '40_to_less_than_50_people',
            '8': '50_people_or_more',
            '': None,
        },
        'scr_vacc_infl': {
            '1': 'yes',
            '2': 'no',
            '3': 'unknown',
            '': None
        }
    }

    # transform integer to string values for the mapped fields
    for fieldname in codebook_map.keys():
        if fieldname not in record or record[fieldname] not in codebook_map[fieldname]:
            raise Exception(f"Unexpected value for codebook mapping (fieldname: {fieldname}, value: {record[fieldname]})")
        else:
            record[fieldname] = codebook_map[fieldname][record[fieldname]]


    integer_questions = [
        'scr_age',
        'scr_num_doses',        # covid vaccine
        'enr_living_population',
    ]

    string_questions = [
        'scr_dose1_man',
        'scr_dose1_man_other',
        'scr_dose2_man',
        'scr_dose2_man_other',
        'scr_dose3_man',
        'scr_dose3_man_other',
        'enr_other_race',
        'enr_other_group',
        'enr_mask_type',
    ]
    string_questions += codebook_map.keys()

    date_questions = [
        'scr_dose1_date',
        'scr_dose2_date',
        'scr_dose3_date',
    ]

    boolean_questions = [
        'scr_contact_pub',
        'enr_living_alone',
        'enr_living_area',
    ]

    decimal_questions: List[str] = [
        # none so far, but maybe in future
    ]

    coding_questions = [
        'enr_race',
    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'enr_race',
        'enr_mask_type',
    ]

    question_categories = {
        'valueCoding': coding_questions,
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueString': string_questions,
        'valueDate': date_questions,
        #'valueDecimal': decimal_questions (none so far)
    }

    for field in checkbox_fields:
        record[field] = airs_combine_checkbox_answers(record, field)


    flu_vaccine_item = create_flu_vaccine_item(record['scr_vacc_infl'], record['scr_vacc_infl_date'])

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM,
        additional_items = [flu_vaccine_item])


def create_flu_vaccine_item(vaccine_status: str, vaccine_date: str) -> Optional[dict]:
    """
    Return a questionnaire response item with the flu vaccine response(s) encoded.
    """

    vaccine_status_bool = map_vaccine(vaccine_status)
    if vaccine_status_bool is None:
        return None

    answers: List[Dict[str, Any]] = [{ 'valueBoolean': vaccine_status_bool }]

    if vaccine_status_bool and vaccine_date:
        answers.append({ 'valueDate': vaccine_date })

    return create_questionnaire_response_item('scr_vacc_infl', answers)


def airs_create_weekly_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the initial weekly encounter.
    """

    date_questions = [
        'wk_date',
        'wk_symp_start_date',
        'wk_symp_stop_date',
    ]

    string_questions = []

    # sic--this misspelling is in the redcap form.
    new_covid_dose = record.get('wk_does')
    vacc_name_field, vacc_other_field = None, None

    # weekly instrument only asks for date and manufacturer of the most recent dose if received in the past week
    if new_covid_dose == '1':
        date_questions.append('wk_vacc1_date')
        vacc_name_field = 'wk_vacc_name'
        vacc_other_field = 'wk_vacc_other'
    elif new_covid_dose == '2':
        date_questions.append('wk_vacc1_date_2')
        vacc_name_field = 'wk_vacc_name_2'
        vacc_other_field = 'wk_vacc_other_2'
    elif new_covid_dose == '3':
        date_questions.append('wk_vacc1_date_3')
        vacc_name_field = 'wk_vacc_name_3'
        vacc_other_field = 'wk_vacc_other_3'
    elif new_covid_dose == '4':
        date_questions.append('wk_vacc1_date_4')
        vacc_name_field = 'wk_vacc_name_4'
        vacc_other_field = 'wk_vacc_other_4'

    # map vaccine manufacturer values in weekly instrument to match those used in screening instrument
    # plus moderna and pfizer bivalent which were added later to the weekly instrument
    vaccine_manufacturer_map = {
        '1':    'moderna',
        '2':    'novovax',
        '3':    'astrazeneca',
        '4':    'pfizer',
        '5':    'other',
        '6':    'dont_know',
        '7':    'moderna_bivalent',
        '8':    'pfizer_bivalent',
    }
    # Replace vaccine manufacturer integers with str values to match screening instrument
    if vacc_name_field and vacc_other_field:
        if record[vacc_name_field] in vaccine_manufacturer_map:
            record[vacc_name_field] = vaccine_manufacturer_map[record[vacc_name_field]]
            string_questions.append(vacc_name_field)
            if record[vacc_name_field] == 'other':
                string_questions.append(vacc_other_field)
        else:
            raise Exception(f"Unknown vaccine manufacturer: {record[vacc_name_field]}; in record: {record.id}, event: {record.event_name}; field: {vacc_name_field}.")

    symptom_severity_map = {
        '0':    'none',
        '1':    'mild',
        '2':    'moderate',
        '3':    'severe',
    }

    wk_symptom_fields = {
        'wk_congestion',
        'wk_nasal_drip',
        'wk_runny_nose',
        'wk_sinus_pain',
        'wk_sneezing',
        'wk_chest_pain',
        'wk_cough',
        'wk_sob',
        'wk_sputum',
        'wk_wheeze',
        'wk_smell',
        'wk_taste',
        'wk_chill',
        'wk_fatigue',
        'wk_fever',
        'wk_headache',
        'wk_sleeping',
        'wk_myalgia',
        'wk_skin_rash',
        'wk_sweats',
        'wk_ear_congestion',
        'wk_ear_pain',
        'wk_eye_pain',
        'wk_hoarse',
        'wk_sore_throat',
        'wk_diarrhea',
        'wk_nausea',
        'wk_stomach_pain',
        'wk_vomiting',
    }

    # Replace severity integer values with corresponding text before including as string questions
    for symptom_field in wk_symptom_fields:
        if record[symptom_field] in symptom_severity_map:
            record[symptom_field] = symptom_severity_map[record[symptom_field]]
            string_questions.append(symptom_field)

    question_categories = {
        'valueDate': date_questions,
        'valueString': string_questions,
    }

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)


def airs_create_follow_up_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the weekly follow-up encounter
    """

    date_questions = [
        'ss_date',
    ]
    string_questions = []

    symptom_severity_map = {
        '0':    'none',
        '1':    'mild',
        '2':    'moderate',
        '3':    'severe',
    }

    ss_symptom_fields = {
        'ss_congestion',
        'ss_nasal_drip',
        'ss_runny_nose',
        'ss_sinus_pain',
        'ss_sneezing',
        'ss_chest_pain',
        'ss_cough',
        'ss_sob',
        'ss_sputum',
        'ss_wheeze',
        'ss_smell',
        'ss_taste',
        'ss_chill',
        'ss_fatigue',
        'ss_fever',
        'ss_headache',
        'ss_sleeping',
        'ss_myalgia',
        'ss_skin_rash',
        'ss_sweats',
        'ss_ear_congestion',
        'ss_ear_pain',
        'ss_eye_pain',
        'ss_hoarse',
        'ss_sore_throat',
        'ss_diarrhea',
        'ss_nausea',
        'ss_stomach_pain',
        'ss_vomiting',
    }

    # Replace severity integer values with corresponding text before including as string questions
    for symptom_field in ss_symptom_fields:
        if record[symptom_field] in symptom_severity_map:
            record[symptom_field] = symptom_severity_map[record[symptom_field]]
            string_questions.append(symptom_field)

    question_categories = {
        'valueDate': date_questions,
        'valueString': string_questions,
    }

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)


def airs_create_computed_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict, birthdate: datetime, encounter_date: datetime) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for a
    computed questionnaire. This "computed questionnaire" produces
    answers that do not appear directly in an actual REDCap survey.
    For example, a computed question captures the participant's age
    on the date of the encounter.
    """
    # A birthdate of None will return a falsy relativedelta() object
    delta = relativedelta(encounter_date, birthdate)

    if not delta:
        age = None
    else:
        # Age Ceiling
        age = age_ceiling(delta.years)

    record['age'] = age

    integer_questions = [
        'age'
    ]

    question_categories = {
        'valueInteger': integer_questions
    }

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)


def airs_map_sex(airs_sex: str) -> str:
    sex_map = {
        "1": "male",
        "2": "female",
        "3": "other",
    }

    if airs_sex in sex_map:
        return sex_map[airs_sex]
    else:
        raise UnknownSexError(airs_sex)


def airs_build_patient(enrollment: REDCapRecord) -> tuple:
    """
    Build a patient record using demographics. The data we need is scattered
    across several different instruments, so they'll all need to be completed
    in order to do this correctly.
    """
    return create_patient_using_demographics(
        sex = airs_map_sex(enrollment['enr_sex']),
        preferred_language = LANGUAGE_CODE[enrollment.project.id],
        first_name = enrollment['scr_first_name'],
        last_name = enrollment['scr_last_name'],
        birth_date = enrollment['enr_dob'],
        zipcode = enrollment['enr_mail_zip'],
        record = enrollment,
        system_identifier = INTERNAL_SYSTEM)


def airs_build_location_info(db: DatabaseSession, cache: TTLCache,
                             enrollment: dict) -> list:
    """
    Build location info from AIRS. The forms use multiple (conditional)
    questions to check this, and different codes than what redcap.py's
    `build_residential_location_resources()` expects, so we've got to
    map them to one of the allowed values.
    """
    # Is this group housing? If so, map it to a value that results in
    #  build_residential_location_resources() assigning it a 'lodging'
    #  housing_type. This doesn't map cleanly between AIRS and
    #  what is expected in `build_residential_location_resources()`.
    group_housing = {
                        # AIRS group home type:
        '1': 'none',    # Dormitory
        '2': 'none',    # Home
        '3': 'shelter', # Homeless Shelter,
        '4': 'ltc',     # Long-term shelter or skilled nursing facility
        '5': 'none',    # Other (specified elsewhere, but there's no facility to handle that in redcap.py)
    }
    if enrollment.get('enr_living_area') == '1':
        housing = group_housing[enrollment.get('enr_living_group')]
    elif enrollment.get('enr_living_area') == '2':
        housing = 'single-family-home'
    else:
        # enr_living_area is a radio button so this should never happen,
        # but when has that stopped us?
        LOG.error(f"Invalid value {enrollment.get('enr_living_area')}"
                  " for enrollment['enr_living_area']")
        return None

    return build_residential_location_resources(
        db = db,
        cache = cache,
        housing_type = housing,
        primary_street_address = enrollment['enr_mail_street_address'],
        secondary_street_address = enrollment['enr_mail_street_address_2'],
        city = enrollment['enr_mail_city'],
        state = enrollment['enr_mail_state'],
        zipcode = enrollment['enr_mail_zip'],
        system_identifier = INTERNAL_SYSTEM)


def airs_build_specimens(db,
    patient_reference: dict,
    encounter_reference: dict,
    redcap_record_instance: REDCapRecord,
    swab_kit_instrument_set: SwabKitInstrumentSet) -> tuple:

    specimen_entry = None
    specimen_observation_entry = None
    specimen_identifier = None

    if swab_kit_instrument_set == SwabKitInstrumentSet.FIRST:
        prioritized_barcodes = [
            redcap_record_instance["results_barcode"],
            redcap_record_instance["return_utm_barcode"],
            redcap_record_instance["pre_scan_barcode"],
        ]
        sample_received_time = redcap_record_instance['samp_process_date']
        able_to_test = redcap_record_instance['able_to_test']
    elif swab_kit_instrument_set == SwabKitInstrumentSet.SECOND:
        prioritized_barcodes = [
            redcap_record_instance["results_barcode_v2"],
            redcap_record_instance["return_utm_barcode_v2"],
            redcap_record_instance["pre_scan_barcode_v2"],
        ]
        sample_received_time = redcap_record_instance['samp_process_date_v2']
        able_to_test = redcap_record_instance['able_to_test_v2']
    else:
        raise Exception(f"Unknown encounter type (weekly or follow-up) for redcap record/event: {redcap_record_instance.get('subject_id')}/{redcap_record_instance.event_name}.")


    # Check if specimen barcode is valid
    specimen_barcode = None
    for barcode in prioritized_barcodes:
        specimen_barcode = barcode.strip()
        if specimen_barcode:
            # Disable logging when calling find_identifier here to suppess alerts
            logging.disable(logging.WARNING)
            specimen_identifier = find_identifier(db, specimen_barcode)
            logging.disable(logging.NOTSET)

            break

    if specimen_identifier:
        specimen_entry, specimen_reference = create_specimen(
            prioritized_barcodes = prioritized_barcodes,
            patient_reference = patient_reference,
            collection_date = airs_get_collection_date(redcap_record_instance, swab_kit_instrument_set),
            sample_received_time = sample_received_time,
            able_to_test = able_to_test,
            system_identifier = INTERNAL_SYSTEM)

        specimen_observation_entry = create_specimen_observation_entry(
            specimen_reference = specimen_reference,
            patient_reference = patient_reference,
            encounter_reference = encounter_reference)
    else:
        LOG.debug(f"No identifier found for barcode «{specimen_barcode}»")
        LOG.info("No identifier found for barcode. Creating encounter for record instance without sample")

    # Do not log warning for unrecognized barcodes, which are being tracked outside of ID3C.
    if specimen_identifier and not specimen_entry:
        LOG.warning("Skipping record instance. We think the specimen was received, "
             "but we're unable to create the specimen_entry for record: "
             f"{redcap_record_instance.get('subject_id')}, event: {redcap_record_instance.event_name}"
        )

    return (specimen_entry, specimen_observation_entry)


def airs_build_contained_and_diagnosis(patient_reference: dict, record: REDCapRecord,
        encounter_type: EncounterType, system_identifier: str) -> Tuple[list, list]:

    def build_condition(patient_reference: dict, symptom_code: str,
        onset_date: str, system_identifier: str) -> Optional[dict]:
        """ Returns a FHIR Condition resource. """

        # XXX TODO: Define this as a TypedDict when we upgrade from Python 3.6 to
        # 3.8.  Until then, there's no reasonable way to type this data structure
        # better than Any.
        #   -trs, 24 Oct 2019
        condition: Any = {
            "resourceType": "Condition",
            "id": f'{symptom_code}',
            "code": {
                "coding": [
                    {
                        "system": f"{system_identifier}/symptom",
                        "code": symptom_code
                    }
                ]
            },
            "subject": patient_reference
        }

        if onset_date:
            condition["onsetDateTime"] = onset_date

        return condition


    def build_diagnosis(symptom_code: str) -> Optional[dict]:
        #mapped_symptom_code = map_symptom_to_sfs(symptom)
        #if not mapped_symptom_code:
        #    return None
        return { "condition": { "reference": f"#{symptom_code}" } }


    if encounter_type == EncounterType.WEEKLY:
        redcap_field_prefix = 'wk_'
        onset_date = record['wk_symp_start_date']
        no_symptoms = record['wk_symp_curr'] != '1' and record['wk_symp_past_week'] != '1'
    elif encounter_type == EncounterType.FOLLOW_UP:
        redcap_field_prefix = 'ss_'
        # sic--this misspelling is in the redcap form.
        onset_date = record['ss_ealiest_date']
        no_symptoms = record['ss_symptoms'] != '1'
    else:
        raise ValueError(f"Unknown EncounterType: {encounter_type}")


    contained = []
    diagnosis = []

    # Symptom map with ID3C standard symptom codes as keys, and list of associated REDCap fields as values. If any of the
    # listed REDCap fields has a value greater than '0' (None), Condition/Diagnosis entries are created using the
    # standard code.
    symptom_map = {
        'runnyOrStuffyNose':            ['congestion', 'nasal_drip', 'runny_nose', 'sinus_pain', 'sneezing'],
        'feelingFeverish':              ['fever'],
        'headaches':                    ['headache'],
        'cough':                        ['cough'],
        'chillsOrShivering':            ['chill'],
        'sweats':                       ['sweats'],
        'soreThroat':                   ['sore_throat'],
        'nauseaOrVomiting':             ['nausea', 'vomiting'],
        'fatigue':                      ['fatigue'],
        'muscleOrBodyAches':            ['myalgia'],
        'diarrhea':                     ['diarrhea'],
        'earPainOrDischarge':           ['ear_pain'],
        'rash':                         ['skin_rash'],
        'increasedTroubleBreathing':    ['sob', 'wheeze'],
        'eyePain':                      ['eye_pain'],
        'lossOfSmellOrTaste':           ['smell', 'taste'],
        'other':                        ['chest_pain', 'sputum', 'sleeping', 'ear_congestion', 'hoarse', 'stomach_pain']
    }

    if no_symptoms:
        contained.append(build_condition(patient_reference, 'none', None, system_identifier))
        diagnosis.append(build_diagnosis('none'))
    else:
        for k,v in symptom_map.items():
            for redcap_field_name in v:
                # if any of the associated REDCap fields has value > '0', then create the entry
                if record[redcap_field_prefix + redcap_field_name] > '0':
                    contained.append(build_condition(patient_reference, k, onset_date, system_identifier))
                    diagnosis.append(build_diagnosis(k))
                    break

    return contained, diagnosis


def airs_race(races: Optional[Any]) -> list:
    """
    Given one or more *races* values, returns the matching race identifier found in
    Audere survey data.

    Single values may be passed:

    >>> airs_race("6")
    ['other']

    A list of values may also be passed:

    >>> airs_race(["2", "3", "5"])
    ['asian', 'blackOrAfricanAmerican', 'white']

    >>> airs_race(None)
    [None]

    An Exception is raised when an unknown value is
    encountered:

    >>> airs_race("0")
    Traceback (most recent call last):
        ...
    Exception: Unknown race value «0»
    """

    if races is None:
        LOG.debug("No race response found.")
        return [None]

    race_map = {
        "1": "americanIndianOrAlaskaNative",
        "2": "asian",
        "3": "blackOrAfricanAmerican",
        "4": "nativeHawaiian",
        "5": "white",
        "6": "other",
        "7": None,
    }

    def standardize_race(race):
        try:
            return race_map[race]
        except KeyError:
            raise Exception(f"Unknown race value «{race}»") from None

    return list(map(standardize_race, races))


def map_mask_type(mask_type: str) -> Optional[str]:

    mask_type_map = {
        '1':    'n95_or_kn95',
        '2':    'face_sheild',
        '3':    'cloth_or_paper',
        '4':    'not_sure',
        '':     None,
    }

    if mask_type not in mask_type_map:
        raise Exception(f"Unknown mask type value «{mask_type}»")

    return mask_type_map[mask_type]


def airs_combine_checkbox_answers(record: dict, coded_question: str) -> Optional[List]:
    """
    Handles the combining "select all that apply"-type checkbox
    responses into one list.

    Uses AIRS specific mapping for race and mask type
    """
    regex = rf'{re.escape(coded_question)}___[\w]*$'
    empty_value = '0'
    answered_checkboxes = list(filter(lambda f: filter_fields(f, record[f], regex, empty_value), record))
    # REDCap checkbox fields have format of {question}___{answer}
    answers = list(map(lambda k: k.replace(f"{coded_question}___", ""), answered_checkboxes))

    if coded_question == 'enr_race':
        return airs_race(answers)

    if coded_question == 'enr_mask_type':
        return list(map(lambda a: map_mask_type(a), answers))

    return answers
