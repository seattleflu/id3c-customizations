"""
Process DETs for the UW Reopening (Husky Coronavirus Testing) REDCap projects.
"""
import logging
from dateutil.relativedelta import relativedelta
from enum import Enum
from id3c.cli.command.etl import redcap_det
from id3c.cli.redcap import is_complete, Record as REDCapRecord
from seattleflu.id3c.cli.command import age_ceiling
from .redcap import *


LOG = logging.getLogger(__name__)


class HuskyProject():
    id: int
    lang: str
    api_token_env_var: str
    command_name: str


    def __init__(self, project_id: int, lang: str, api_token_env_var: str, command_name: str) -> None:
        self.id = project_id
        self.lang = lang
        self.api_token_env_var = api_token_env_var
        self.command_name = command_name

PROJECTS = [
        HuskyProject(23854, "en", "REDCAP_API_TOKEN", "uw-reopening")
    ]

LANGUAGE_CODE = {
project.id: project.lang
    for project in PROJECTS }

class CollectionMethod(Enum):
    SWAB_AND_SEND = 'swab_and_send'
    KIOSK = 'kiosk'

class EventType(Enum):
    ENROLLMENT = 'enrollment'
    ENCOUNTER = 'encounter'


REVISION = 2

REDCAP_URL = 'https://redcap.iths.org/'
INTERNAL_SYSTEM = "https://seattleflu.org"
ENROLLMENT_EVENT_NAME = "enrollment_arm_1"
ENCOUNTER_EVENT_NAME = "encounter_arm_1"
SWAB_AND_SEND_SITE = 'UWReopeningSwabNSend'

REQUIRED_ENROLLMENT_INSTRUMENTS = [
    'eligibility_screening',
    'consent_form',
    'enrollment_questionnaire'
]


def command_for_each_project(function):
    """
    A decorator to register one redcap-det subcommand per REDCap project, each
    calling the same base *function*.

    Used for side-effects only; the original *function* is unmodified.
    """
    for project in PROJECTS:
        help_message = "Process REDCap DETs for Husky Coronavirus Testing (UW reopening)"

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
def redcap_det_uw_reopening(*, db: DatabaseSession, cache: TTLCache, det: dict,
    redcap_record_instances: List[REDCapRecord]) -> Optional[dict]:

    assert redcap_record_instances is not None and len(redcap_record_instances) > 0, \
        "The redcap_record_instances list was not populated."

    record_id = redcap_record_instances[0]['record_id']
    project_id = redcap_record_instances[0].project.id

    enrollments = [record for record in redcap_record_instances if record["redcap_event_name"] == ENROLLMENT_EVENT_NAME]
    assert len(enrollments) == 1, \
        f"Record had {len(enrollments)} enrollments."

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

    # If the participant's age < 18 ensure we have parental consent.
    if (enrollment['core_age_years'] == "" or int(enrollment['core_age_years']) < 18) and \
            (is_complete('parental_consent_form', enrollment) == False or enrollment['signature_parent'] == ''):
        LOG.debug("The participant is < 18 years old and we do not have parental consent. Skipping record.")
        return None

    # Create the participant resource entry and reference.
    # Assumes that the project language is the participant's preferred language.
    netid = normalize_net_id(enrollment.get('netid'))

    if netid:
        patient_entry, patient_reference = create_patient_using_unique_identifier(
            sex = enrollment['core_sex'],
            preferred_language = LANGUAGE_CODE[enrollment.project.id],
            unique_identifier = netid,
            record = enrollment,
            system_identifier = INTERNAL_SYSTEM)
    else:
        patient_entry, patient_reference = create_patient_using_demographics(
            sex = enrollment['core_sex'],
            preferred_language = LANGUAGE_CODE[enrollment.project.id],
            first_name = enrollment['core_participant_first_name'],
            last_name = enrollment['core_participant_last_name'],
            birth_date = enrollment['core_birthdate'],
            zipcode = enrollment['core_zipcode'],
            record = enrollment,
            system_identifier = INTERNAL_SYSTEM)

    if not patient_entry:
        LOG.warning("Skipping record with insufficient information to construct patient")
        return None

    birthdate = parse_date_from_string(enrollment.get('core_birthdate'))

    location_resource_entries = build_location_resources(
        db = db,
        cache = cache,
        housing_type = enrollment.get('core_housing_type'),
        primary_street_address = enrollment['core_home_street'],
        secondary_street_address = enrollment['core_apartment_number'],
        city = enrollment['core_home_city'],
        state = enrollment['core_home_state'],
        zipcode = enrollment['core_zipcode'],
        system_identifier = INTERNAL_SYSTEM)

    persisted_resource_entries = [patient_entry, *location_resource_entries]

    for redcap_record_instance in redcap_record_instances:

        event_type = None
        collection_method = None

        if redcap_record_instance["redcap_event_name"] == ENROLLMENT_EVENT_NAME:
            event_type = EventType.ENROLLMENT
        elif redcap_record_instance["redcap_event_name"] == ENCOUNTER_EVENT_NAME:
            event_type = EventType.ENCOUNTER
            if is_complete('kiosk_registration_4c7f', redcap_record_instance):
                collection_method = CollectionMethod.KIOSK
            elif is_complete('test_order_survey', redcap_record_instance):
                collection_method = CollectionMethod.SWAB_AND_SEND
        else:
            LOG.error(f"The record instance has an unexpected event name: {redcap_record_instance['redcap_event_name']}")
            continue

        # Skip an ENCOUNTER instance if we don't have the data we need to
        # create an encounter.
        if event_type == EventType.ENCOUNTER \
            and not is_complete('daily_attestation', redcap_record_instance) \
                and not collection_method  \
                and not redcap_record_instance['testing_date']: # from the 'Testing Determination - Internal' instrument
                    LOG.debug("Skipping record instance with insufficient information to construct the initial encounter")
                    continue

        # site_reference refers to where the sample was collected
        record_location = None
        if collection_method == CollectionMethod.KIOSK:
            record_location = redcap_record_instance.get('location_type')

        location_site_map = {
            'bothell':  'UWBothell',
            'odegaard': 'UWOdegaardLibrary',
            'slu':      'UWSouthLakeUnion',
            'tacoma':   'UWTacoma',
            'uw_club':  'UWClub'
            }

        site_reference = create_site_reference(
            location = record_location,
            site_map = location_site_map,
            default_site = SWAB_AND_SEND_SITE,
            system_identifier = INTERNAL_SYSTEM)

        # Handle various symptoms.
        contained: List[dict] = []
        diagnosis: List[dict] = []

        # Map the various symptoms variables to their onset date.
        # For daily_symptoms_covid_like we don't know the actual onset date. The questions asks
        # "in the past 24 hours"
        if event_type == EventType.ENCOUNTER:
            symptom_onset_map = {
                'daily_symptoms_covid_like': None,
                'symptoms': redcap_record_instance['symptom_onset'],
                'symptoms_kiosk': redcap_record_instance['symptom_duration_kiosk'],
                'symptoms_swabsend': redcap_record_instance['symptom_duration_swabsend']
            }
        elif event_type == EventType.ENROLLMENT:
            symptom_onset_map = {'symptoms_base': redcap_record_instance['symptom_onset_base']}

        contained, diagnosis = build_contained_and_diagnosis(
            patient_reference = patient_reference,
            record = redcap_record_instance,
            symptom_onset_map = symptom_onset_map,
            system_identifier = INTERNAL_SYSTEM)

        collection_code = None
        if event_type == EventType.ENROLLMENT or collection_method == CollectionMethod.SWAB_AND_SEND:
            collection_code = CollectionCode.HOME_HEALTH
        elif collection_method == CollectionMethod.KIOSK:
            collection_code = CollectionCode.FIELD

        encounter_date = get_encounter_date(redcap_record_instance, event_type)

        initial_encounter_entry, initial_encounter_reference = create_encounter(
            encounter_date = encounter_date,
            patient_reference = patient_reference,
            site_reference = site_reference,
            locations = location_resource_entries,
            diagnosis = diagnosis,
            contained = contained,
            collection_code = collection_code,
            parent_encounter_reference = None,
            encounter_reason_code = None,
            encounter_identifier_suffix = None,
            system_identifier = INTERNAL_SYSTEM,
            record = redcap_record_instance)

        # Skip the entire record if we can't create the enrollment encounter.
        # Otherwise, just skip the record instance.
        if not initial_encounter_entry:
            if event_type == EventType.ENROLLMENT:
                LOG.warning("Skipping record because we could not create the enrollment encounter")
                return None
            else:
                LOG.warning("Skipping record instance with insufficient information to construct the initial encounter")
                continue

        specimen_entry = None
        specimen_observation_entry = None
        specimen_received = (collection_method == CollectionMethod.SWAB_AND_SEND and \
            is_complete('post_collection_data_entry_qc', redcap_record_instance)) or \
            (collection_method == CollectionMethod.KIOSK and \
            is_complete('kiosk_registration_4c7f', redcap_record_instance))

        if specimen_received:
            # Use barcode fields in this order.
            prioritized_barcodes = [
                redcap_record_instance["collect_barcode_kiosk"],
                redcap_record_instance["return_utm_barcode"],
                redcap_record_instance["pre_scan_barcode"]]

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
            LOG.info("Creating encounter for record instance without sample")

        if specimen_received and not specimen_entry:
            LOG.warning("Skipping record instance. We think the specimen was received," \
                + " but we're unable to create the specimen_entry.")
            continue

        computed_questionnaire_entry = None
        enrollment_questionnaire_entry = None
        daily_questionnaire_entry = None
        testing_determination_internal_questionnaire_entry = None
        follow_up_encounter_entry = None
        follow_up_questionnaire_entry = None
        follow_up_computed_questionnaire_entry = None

        computed_questionnaire_entry = create_computed_questionnaire_response(
            redcap_record_instance, patient_reference, initial_encounter_reference,
            birthdate, parse_date_from_string(initial_encounter_entry['resource']['period']['start']))

        if event_type == EventType.ENROLLMENT:
            enrollment_questionnaire_entry = create_enrollment_questionnaire_response(
            enrollment, patient_reference, initial_encounter_reference)
        else:
            testing_determination_internal_questionnaire_entry = \
                create_testing_determination_internal_questionnaire_response(
                redcap_record_instance, patient_reference, initial_encounter_reference)

            daily_questionnaire_entry = \
                create_daily_questionnaire_response(
                redcap_record_instance, patient_reference, initial_encounter_reference)

            if is_complete('week_followup', redcap_record_instance):
                # Don't set locations because the f/u survey doesn't ask for home address.
                follow_up_encounter_entry, follow_up_encounter_reference = create_encounter(
                    encounter_date = redcap_record_instance['fu_timestamp'].split()[0],
                    patient_reference = patient_reference,
                    site_reference = site_reference,
                    collection_code = CollectionCode.HOME_HEALTH,
                    parent_encounter_reference = initial_encounter_reference,
                    encounter_reason_code = follow_up_encounter_reason_code(),
                    encounter_identifier_suffix = "_follow_up",
                    system_identifier = INTERNAL_SYSTEM,
                    record = redcap_record_instance)

                follow_up_questionnaire_entry = create_follow_up_questionnaire_response(
                redcap_record_instance, patient_reference, follow_up_encounter_reference)
                follow_up_computed_questionnaire_entry = create_computed_questionnaire_response(
                redcap_record_instance, patient_reference, follow_up_encounter_reference,
                birthdate, parse_date_from_string(follow_up_encounter_entry['resource']['period']['start']))


        current_instance_entries = [
            initial_encounter_entry,
            computed_questionnaire_entry,
            enrollment_questionnaire_entry,
            testing_determination_internal_questionnaire_entry,
            daily_questionnaire_entry,
            specimen_entry,
            specimen_observation_entry,
            follow_up_encounter_entry,
            follow_up_questionnaire_entry,
            follow_up_computed_questionnaire_entry
        ]

        persisted_resource_entries.extend(list(filter(None, current_instance_entries)))


    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f"{REDCAP_URL}{project_id}/{record_id}",
        entries = list(filter(None, persisted_resource_entries))
    )


def get_encounter_date(record: dict, event_type: EventType) -> Optional[str]:
    # First try the attestation_date
    # from the daily attestation survey then try nasal_swab_timestamp from
    # the kiosk registration and finally the swab-and-send order date.
    encounter_date = None

    if event_type == EventType.ENCOUNTER:
        if record.get('attestation_date'):
            encounter_date = record.get('attestation_date')
        elif record.get('nasal_swab_timestamp'):
            encounter_date = datetime.strptime(record.get('nasal_swab_timestamp'),
                '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        elif record.get('time_test_order'):
            encounter_date = datetime.strptime(record.get('time_test_order'),
                '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
        elif record.get('testing_date'): # from the 'Testing Determination - Internal' instrument
            encounter_date = record.get('testing_date')

    elif event_type == EventType.ENROLLMENT:
        encounter_date = record.get('enrollment_date')

    else:
        return None

    return encounter_date


def get_collection_date(record: dict, collection_method: CollectionMethod) -> Optional[str]:
    """
    Determine sample/specimen collection date from the given REDCap *record*.
    """
    if collection_method == CollectionMethod.KIOSK:
        return record["nasal_swab_q"]
    elif collection_method == CollectionMethod.SWAB_AND_SEND:
        return record["date_on_tube"] or record["kit_reg_date"] or record["back_end_scan_date"]
    else:
        return None


def create_enrollment_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the enrollment
    encounter (i.e. encounter of enrollment into the study)
    """

    # Do not include `core_age_years` because we calculate the age ourselves in the computed questionnaire.
    integer_questions = [
        'weight',
        'height_total',
        'tier'
    ]

    string_questions = [
        'text_or_email',
        'text_or_email_attestation',
        'campus_location',
        'affiliation',
        'student_level',
        'sea_employee_type',
        'core_house_members',
        'core_education',
        'core_income',
        'wfh_base',
        'core_housing_type',
        'core_health_risk',
        'core_tobacco_use',
        'sought_care_base',
        'hospital_where_base',
        'hospital_ed_base',
        'prior_test_positive_base',
        'prior_test_type_base',
        'prior_test_result_base',
        'contact_base',
        'wash_hands_base',
        'clean_surfaces_base',
        'hide_cough_base',
        'mask_base',
        'distance_base',
        'novax_reason',
        'covid_vaccine',
        'covid_novax_reason',
        'countries_visited_base',
        'states_visited_base',
        'alerts_off',
        'pronouns',
        'on_campus_freq',
        'vaccine_method',
        'vaccine_where'
    ]

    date_questions = [
        'today_consent',
        'enrollment_date_time',
        'hospital_arrive_base',
        'hospital_leave_base',
        'prior_test_positive_date_base'
    ]

    boolean_questions = [
        'study_area',
        'attend_uw',
        'english_speaking',
        'athlete',
        'uw_medicine_yesno',
        'inperson_classes',
        'uw_job',
        'uw_greek_member',
        'live_other_uw',
        'uw_apt_yesno',
        'core_pregnant',
        'core_latinx',
        'mobility',
        'vaccine_hx',
        'hall_health',
        'prior_test_base',
        'travel_countries_phs_base',
        'travel_states_phs_base',
        'swab_and_send_calc',
        'kiosk_calc',
        'covid_test_week_base'
    ]

    decimal_questions = [
        'bmi'
    ]

    coding_questions = [
        'core_race'
    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'core_race',
        'core_health_risk',
        'core_tobacco_use',
        'sought_care_base',
        'prior_test_type_base',
        'prior_test_positive_base',
        'contact_base'
    ]

    question_categories = {
        'valueCoding': coding_questions,
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueString': string_questions,
        'valueDate': date_questions,
        'valueDecimal': decimal_questions
    }

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    # Combine all fields answering the same question
    record['countries_visited_base'] = combine_multiple_fields(record, 'country', '_base')
    record['states_visited_base'] = combine_multiple_fields(record, 'state', '_base')

    # Age Ceiling
    try:
        record['core_age_years'] = age_ceiling(int(record['core_age_years']))
    except ValueError:
        record['core_age_years'] = record['core_age_years'] = None

    # Set the study tier
    tier = None
    if record['tier_1'] == '1':
        tier = 1
    elif record['tier_2'] == '1':
        tier = 2
    elif record['tier_3'] == '1':
        tier = 3
    record['tier'] = tier

    vaccine_item = create_vaccine_item(record["vaccine"], record['vaccine_year'], record['vaccine_month'], 'dont_know')

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM,
        additional_items = [vaccine_item])


def create_follow_up_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the follow-up
    encounter.

    Note: `fu_which_activites` is misspelled on
    purpose to match the misspelling of the fields in the REDCap project.
    """
    boolean_questions = [
        'fu_illness',
        'fu_change',
        'result_changes',
        'fu_household_sick'
    ]

    integer_questions = [
        'fu_days_illness',
        'fu_number_sick'
    ]

    string_questions = [
        'fu_fever',
        'fu_headache',
        'fu_cough',
        'fu_chills',
        'fu_sweat',
        'fu_throat',
        'fu_nausea',
        'fu_nose',
        'fu_tired',
        'fu_ache',
        'fu_breathe',
        'fu_diarrhea',
        'fu_rash',
        'fu_ear',
        'fu_eye',
        'fu_smell_taste',
        'fu_feel_normal',
        'fu_care',
        'fu_hospital_ed',
        'fu_work_school',
        'fu_activities',
        'fu_which_activites',
        'fu_test_result',
        'fu_behaviors_no',
        'fu_behaviors_inconclusive',
        'fu_behaviors',
        'fu_1_symptoms',
        'fu_2_symptoms',
        'fu_3_symptoms',
        'fu_4_symptoms',
        'fu_1_test',
        'fu_2_test',
        'fu_3_test',
        'fu_4_test',
        'fu_1_result',
        'fu_2_result',
        'fu_3_result',
        'fu_4_result',
        'fu_healthy_test',
        'fu_healthy_result'
    ]

    date_questions = [
        'fu_timestamp',
        'fu_symptom_duration',
        'fu_date_care',
        'fu_1_date',
        'fu_2_date',
        'fu_3_date',
        'fu_4_date',
        'followup_date'
    ]

    question_categories = {
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueString': string_questions,
        'valueDate': date_questions
    }

    # Combine checkbox answers into one list
    checkbox_fields = [
        'fu_care',
        'fu_which_activites',
        'fu_behaviors_no',
        'fu_behaviors_inconclusive',
        'fu_behaviors',
        'fu_1_symptoms',
        'fu_2_symptoms',
        'fu_3_symptoms',
        'fu_4_symptoms'
    ]

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)


def create_testing_determination_internal_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the
    testing_determination_internal instrument. This instrument is used
    to communicate with the Priority Queue application. Note that for this
    questionnaire we do want to return answers for all questions even if they are
    not answered.
    """
    boolean_questions = [
        'testing_trigger',
        'surge_selected_flag',
    ]

    string_questions = [
        'testing_type',
    ]

    question_categories = {
        'valueBoolean': boolean_questions,
        'valueString': string_questions,
    }

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)


def create_computed_questionnaire_response(record: REDCapRecord, patient_reference: dict,
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
        age = delta.years
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


def create_daily_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the daily
    attestation questionnaire.
    """
    boolean_questions = [
        'daily_symptoms',
        'prev_pos',
        'hall_health_2',
        'prior_test',
        'travel_countries_phs',
        'travel_states_phs',
        'screen_positive',
        'attend_event',
    ]

    string_questions = [
        'daily_exposure',
        'daily_exposure_known_pos',
        'sought_care',
        'hospital_where',
        'hospital_ed',
        'prior_test_positive',
        'prior_test_type',
        'prior_test_result',
        'contact',
        'wash_hands',
        'clean_surfaces',
        'hide_cough',
        'mask',
        'distance',
        'wfh',
        'countries_visited',
        'states_visited'
    ]

    date_questions = [
        'attestation_timestamp',
        'hospital_arrive',
        'hospital_leave',
        'prior_test_positive_date'
    ]

    question_categories = {
        'valueBoolean': boolean_questions,
        'valueString': string_questions,
        'valueDate': date_questions
    }

    # Combine checkbox answers into one list
    checkbox_fields = [
        'sought_care',
        'prior_test_type',
        'contact'
    ]

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    # Combine all fields answering the same question
    record['countries_visited'] = combine_multiple_fields(record, 'country')
    record['states_visited'] = combine_multiple_fields(record, 'state')

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)

class UnknownRedcapZipCode(ValueError):
    """
    Raised by :function: `zipcode_map` if a provided *redcap_code* is not
    among a set of expected values.
    """
    pass
