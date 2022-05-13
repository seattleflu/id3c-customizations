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
from .redcap_map import *

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

class CollectionMethod(Enum):
    SWAB_AND_SEND = 'swab_and_send'

class EventType(Enum):
    ENROLLMENT = 'enrollment'
    ENCOUNTER = 'encounter'

REVISION = 1

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
        collection_method = None

        if redcap_record_instance.event_name == ENROLLMENT_EVENT_NAME:
            event_type = EventType.ENROLLMENT

        elif redcap_record_instance.event_name in ENCOUNTER_EVENT_NAMES:
            if is_complete('weekly', redcap_record_instance):
                event_type = EventType.ENCOUNTER
                collection_method = CollectionMethod.SWAB_AND_SEND
            else:
                LOG.debug("Skipping record id: "
                    f"{redcap_record_instance.get('subject_id')}, "
                    "encounter: "
                    f"{redcap_record_instance.get('event_name')}: "
                    "insufficient information to construct encounter")
                continue
        else:
            LOG.info(f"Skipping event: {redcap_record_instance.event_name!r} for record "
            f"{redcap_record_instance.get('subject_id')} because the event is not one "
            "that we process")
            continue

        if event_type == EventType.ENCOUNTER:
            if not is_complete('weekly', redcap_record_instance) \
                or not collection_method:
                    LOG.debug("Skipping record id: "
                        f"{redcap_record_instance.get('subject_id')}, "
                        " encounter: "
                        f"{redcap_record_instance.get('event_name')}, "
                        ": insufficient information to construct encounter")
                    continue

        site_reference = create_site_reference(
            location = None,
            site_map = None,
            default_site = SWAB_AND_SEND_SITE,
            system_identifier = INTERNAL_SYSTEM)

        # Handle various symptoms.
        contained: List[dict] = []
        diagnosis: List[dict] = []

        # Map the various symptoms variables to their onset date.
        if event_type == EventType.ENCOUNTER:
            symptom_onset_map = {
                # sic--this misspelling is in the redcap form.
                'symptoms': redcap_record_instance['ss_ealiest_date'],
            }
        # Irrelevant because no symptom questions are present in enrollment,
        # but I'd like to keep the condition that event_type == ENROLLMENT
        # separate in case that changes. It should also not be included in
        # the same case that an invalid event_type is passed.
        elif event_type == EventType.ENROLLMENT:
            symptom_onset_map = {}
        # Should never get here due to the event_type check at the top of the loop
        else:
            LOG.error(f"Invalid event_type {event_type} fell through")

        contained, diagnosis = build_contained_and_diagnosis(
            patient_reference = patient_reference,
            record = redcap_record_instance,
            symptom_onset_map = symptom_onset_map,
            system_identifier = INTERNAL_SYSTEM)

        collection_code = None
        if event_type == EventType.ENROLLMENT or collection_method == CollectionMethod.SWAB_AND_SEND:
            collection_code = CollectionCode.HOME_HEALTH
        else:
            LOG.error(f"Invalid collection_method {collection_method} fell through")

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

        (specimen_entry,
         specimen_observation_entry,
         specimen_entry_v2,
         specimen_observation_entry_v2) = airs_build_specimens(
            db,
            patient_reference,
            initial_encounter_reference,
            redcap_record_instance,
            collection_method
        )

        computed_questionnaire_entry = None
        enrollment_questionnaire_entry = None
        symptom_questionnaire_entry = None
        weekly_encounter_entry = None
        weekly_questionnaire_entry = None
        weekly_computed_questionnaire_entry = None

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
            symptom_questionnaire_entry = airs_create_symptom_questionnaire_response(
                redcap_record_instance, patient_reference, initial_encounter_reference)

            if is_complete('weekly', redcap_record_instance):
                # Don't set locations because the weekly survey doesn't ask for home address.
                weekly_encounter_entry, weekly_encounter_reference = create_encounter(
                    encounter_id = create_encounter_id(redcap_record_instance, True),
                    encounter_date = extract_date_from_survey_timestamp(redcap_record_instance, 'weekly'),
                    patient_reference = patient_reference,
                    site_reference = site_reference,
                    collection_code = CollectionCode.HOME_HEALTH,
                    parent_encounter_reference = initial_encounter_reference,
                    encounter_reason_code = follow_up_encounter_reason_code(),
                    encounter_identifier_suffix = "_weekly",
                    system_identifier = INTERNAL_SYSTEM,
                    record = redcap_record_instance)

                weekly_questionnaire_entry = airs_create_weekly_questionnaire_response(
                    redcap_record_instance, patient_reference, weekly_encounter_reference)
                weekly_computed_questionnaire_entry = airs_create_computed_questionnaire_response(
                    redcap_record_instance, patient_reference, weekly_encounter_reference,
                    birthdate, parse_date_from_string(weekly_encounter_entry['resource']['period']['start']))

        current_instance_entries = [
            initial_encounter_entry,
            computed_questionnaire_entry,
            enrollment_questionnaire_entry,
            symptom_questionnaire_entry,
            specimen_entry,
            specimen_observation_entry,
            specimen_entry_v2,
            specimen_observation_entry_v2,
            weekly_encounter_entry,
            weekly_questionnaire_entry,
            weekly_computed_questionnaire_entry
        ]

        persisted_resource_entries.extend(list(filter(None, current_instance_entries)))

    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{REDCAP_URL}{enrollment.project.id}/{enrollment.id}",
        entries = list(filter(None, persisted_resource_entries))
    )


def airs_get_encounter_date(record: REDCapRecord, event_type: EventType) -> Optional[str]:
    encounter_date = None

    if event_type == EventType.ENCOUNTER:
        encounter_date = extract_date_from_survey_timestamp(record, 'weekly')
    elif event_type == EventType.ENROLLMENT:
        encounter_date = record.get('enr_date_complete') and \
            datetime.strptime(record.get('enr_date_complete'), '%Y-%m-%d').strftime('%Y-%m-%d')
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

    if record.repeat_instance:
        redcap_repeat_instance = str(record.repeat_instance)
    else:
        redcap_repeat_instance = ''

    if is_followup_encounter:
        encounter_identifier_suffix = "_follow_up"
    else:
        encounter_identifier_suffix = ''

    return f'{record.project.base_url}{record.project.id}/{record.id}/{redcap_event_name}/' + \
        f'{redcap_repeat_instance}{encounter_identifier_suffix}'


def get_collection_date(record: REDCapRecord, collection_method: CollectionMethod) -> Optional[str]:
    """
    Determine sample/specimen collection date from the given REDCap *record*.
    """
    # For all surveys, try the survey _timestamp field (which is in Pacific time)
    # before custom fields because the custom fields aren't always populated and when
    # they are populated they use the browser's time zone.
    collection_date = None

    if collection_method == CollectionMethod.SWAB_AND_SEND:
        collection_date = record.get("date_on_tube") \
            or extract_date_from_survey_timestamp(record, "husky_test_kit_registration") \
            or record.get("kit_reg_date") \
            or extract_date_from_survey_timestamp(record, "test_fulfillment_form") \
            or record.get("back_end_scan_date")
    else:
        raise ValueError(f"Record {record.id}: invalid collection_method {collection_method}")

    return collection_date


def airs_create_enrollment_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the enrollment
    encounter (i.e. encounter of enrollment into the study)
    """

    integer_questions = [
        'scr_age',
        'scr_num_doses'
        'scr_vacc_infl',
        'enr_outside_hour',
        'enr_outside_mask',
        'enr_sex',
        'enr_gender',
        'enr_degree',
        'enr_ethnicity',
        'enr_living_group',
        'enr_living_population',
        'enr_outside_mask',
        'enr_indoor_no_mask',
        'enr_outdoor_no_mask',
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
    ]

    date_questions = [
        'scr_dose1_date',
        'scr_dose2_date',
        'scr_dose3_date',
        'scr_vacc_infl_date',
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
        'enr_race'
        'enr_mask_type'
    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'enr_race',
        'enr_mask_type'
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
        record[field] = combine_checkbox_answers(record, field)

    if record.get('scr_num_doses') and int(record['scr_num_doses']) > 0:
        vacc_covid = '1'
    else:
        vacc_covid = '0'
    vacc_year = record.get('scr_dose1_date') and \
        datetime.strptime(record['scr_dose1_date'], '%Y-%m-%d').strftime('%Y')
    vacc_month = record.get('scr_dose1_date') and \
        datetime.strptime(record['scr_dose1_date'], '%Y-%m-%d').strftime('%B')
    vaccine_item = create_vaccine_item(vacc_covid, vacc_year, vacc_month, 'dont_know')

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM,
        additional_items = [vaccine_item])


def airs_create_weekly_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the
    weekly follow-up encounter.
    """
    integer_questions = [
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
    ]

    boolean_questions = [
        'wk_nasal',
        'wk_chest_symptoms',
        'wk_sensory_symptoms',
        'wk_eye_ear_throat',
        'wk_gi',
    ]

    date_questions = [
        'wk_date',
        'wk_symp_start_date',
        'wk_symp_stop_date',
    ]

    coding_questions = [
        'wk_which_med',
    ]

    question_categories = {
        'valueInteger': integer_questions,
        'valueBoolean': boolean_questions,
        # 'valueString': string_questions, ## none so far
        'valueDate': date_questions,
        'valueCoding': coding_questions,
    }

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)


def airs_create_symptom_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the AIRS Symptom instrument
    """

    integer_questions = [
        'ss_chest_pain',
        'ss_chill',
        'ss_congestion',
        'ss_cough',
        'ss_diarrhea',
        'ss_ear_congestion',
        'ss_ear_pain',
        'ss_eye_pain',
        'ss_fatigue',
        'ss_fever',
        'ss_gi',
        'ss_headache',
        'ss_hoarse',
        'ss_myalgia',
        'ss_nasal_drip',
        'ss_nausea',
        'ss_runny_nose',
        'ss_sinus_pain',
        'ss_skin_rash',
        'ss_sleeping',
        'ss_smell',
        'ss_sneezing',
        'ss_sob',
        'ss_sore_throat',
        'ss_sputum',
        'ss_stomach_pain',
        'ss_sweats',
        'ss_taste',
        'ss_vomiting',
        'ss_wheeze',
    ]

    boolean_questions = [
        'ss_nasal',
        'ss_chest_symptoms',
        'ss_sensory_symptoms',
        'ss_eye_ear_throat',
        'ss_gi',
    ]

    date_questions = [
        'ss_date',
    ]

    coding_questions: List[str] = [
    ]

    question_categories = {
        'valueInteger': integer_questions,
        'valueBoolean': boolean_questions,
        # 'valueString': string_questions, ## none so far
        'valueDate': date_questions,
        # 'valueCoding': coding_questions, ## none so far
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
    initial_encounter_reference: dict,
    redcap_record_instance: REDCapRecord,
    collection_method: CollectionMethod) -> tuple:

    specimen_entry = None
    specimen_observation_entry = None
    specimen_identifier = None
    specimen_received = (collection_method == CollectionMethod.SWAB_AND_SEND and \
        is_complete('post_collection_data_entry_qc', redcap_record_instance))

    if specimen_received:
        # Use barcode fields in this order.
        prioritized_barcodes = [
            redcap_record_instance["results_barcode"],
            redcap_record_instance["return_utm_barcode"],
            redcap_record_instance["pre_scan_barcode"],
        ]

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
                collection_date = get_collection_date(redcap_record_instance, collection_method),
                sample_received_time = redcap_record_instance['samp_process_date'],
                able_to_test = redcap_record_instance['able_to_test'],
                system_identifier = INTERNAL_SYSTEM)

            specimen_observation_entry = create_specimen_observation_entry(
                specimen_reference = specimen_reference,
                patient_reference = patient_reference,
                encounter_reference = initial_encounter_reference)
        else:
            LOG.debug(f"No identifier found for barcode «{specimen_barcode}»")
            LOG.info("No identifier found for barcode. Creating encounter for record instance without sample")
    else:
        LOG.debug("Creating encounter for record instance without sample")

    # Do not log warning for unrecognized barcodes, which are being tracked outside of ID3C.
    if specimen_received and specimen_identifier and not specimen_entry:
        LOG.warning("Skipping record instance. We think the specimen was received, "
             "but we're unable to create the specimen_entry for record: "
             f"{redcap_record_instance.get('record_id')}, instance: {redcap_record_instance.get('redcap_repeat_instance')}"
        )

    specimen_entry_v2 = None
    specimen_observation_entry_v2 = None
    specimen_identifier_v2 = None
    specimen_received_v2 = (collection_method == CollectionMethod.SWAB_AND_SEND and \
        is_complete('post_collection_data_entry_qc_2', redcap_record_instance))

    if specimen_received_v2:
        # Use barcode fields in this order.
        prioritized_barcodes_v2 = [
            redcap_record_instance["results_barcode_v2"],
            redcap_record_instance["return_utm_barcode_v2"],
            redcap_record_instance["pre_scan_barcode_v2"],
        ]

        # Check if specimen barcode is valid
        specimen_barcode_v2 = None
        for barcode_v2 in prioritized_barcodes_v2:
            specimen_barcode_v2 = barcode.strip()
            if specimen_barcode_v2:
                # Disable logging when calling find_identifier here to suppess alerts
                logging.disable(logging.WARNING)
                specimen_identifier_v2 = find_identifier(db, specimen_barcode_v2)
                logging.disable(logging.NOTSET)

                break

        if specimen_identifier_v2:
            specimen_entry_v2, specimen_reference_v2 = create_specimen(
                prioritized_barcodes = prioritized_barcodes_v2,
                patient_reference = patient_reference,
                collection_date = get_collection_date(redcap_record_instance, collection_method),
                sample_received_time = redcap_record_instance['samp_process_date_v2'],
                able_to_test = redcap_record_instance['able_to_test_v2'],
                system_identifier = INTERNAL_SYSTEM)

            specimen_observation_entry_v2 = create_specimen_observation_entry(
                specimen_reference = specimen_reference_v2,
                patient_reference = patient_reference,
                encounter_reference = initial_encounter_reference)
        else:
            LOG.debug(f"No identifier found for v2 barcode «{specimen_barcode_v2}»")
            LOG.info("No identifier found for barcode. Creating v2 encounter for record instance without sample")
    else:
        LOG.debug("Creating v2 encounter for record instance without sample")

    # Do not log warning for unrecognized barcodes, which are being tracked outside of ID3C.
    if specimen_received_v2 and specimen_identifier_v2 and not specimen_entry_v2:
        LOG.warning("Skipping v2 record instance. We think the specimen was received, "
             "but we're unable to create the specimen_entry for record: "
             f"{redcap_record_instance.get('record_id')}, instance: {redcap_record_instance.get('redcap_repeat_instance')}"
             )

    return (specimen_entry, specimen_observation_entry, specimen_entry_v2, specimen_observation_entry_v2)
