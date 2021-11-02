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
    command_name: str


    def __init__(self, project_id: int, lang: str, command_name: str) -> None:
        self.id = project_id
        self.lang = lang
        self.command_name = command_name


REDCAP_URL  = 'https://hct.redcap.rit.uw.edu/' # REDCap server hostname
REDCAP_PID  = 45                              # REDCap project ID
REDCAP_LANG = 'en'                            # Survey language
ETL_COMMAND = 'uw-reopening'                  # Run as 'id3c etl redcap-det <ETL_COMMAND>'

PROJECTS = [
        HuskyProject(REDCAP_PID, REDCAP_LANG, ETL_COMMAND)
    ]

LANGUAGE_CODE = {
project.id: project.lang
    for project in PROJECTS }

class CollectionMethod(Enum):
    SWAB_AND_SEND = 'swab_and_send'
    KIOSK = 'kiosk'
    UW_DROPBOX = 'uw_dropbox'

class EventType(Enum):
    ENROLLMENT = 'enrollment'
    ENCOUNTER = 'encounter'

REVISION = 2

INTERNAL_SYSTEM = "https://seattleflu.org"
ENROLLMENT_EVENT_NAME = "enrollment_arm_1"
ENCOUNTER_EVENT_NAME = "encounter_arm_1"
SWAB_AND_SEND_SITE = 'UWReopeningSwabNSend'
UW_DROPBOX_SITE = 'UWReopeningDropbox'
STUDY_START_DATE = datetime(2020, 9, 24) # Study start date of 2020-09-24

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

    if redcap_record_instances is None or len(redcap_record_instances) == 0:
        LOG.warning(f"There are no record instances. Skipping record.")
        return None

    enrollments = [record for record in redcap_record_instances if record.event_name == ENROLLMENT_EVENT_NAME]

    if not len(enrollments) == 1:
        LOG.warning(f"There are {len(enrollments)} enrollment instances for record: {redcap_record_instances[0].get('record_id')}. Skipping record.")
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
        LOG.warning(f"Skipping record {enrollment.get('record_id')} with insufficient information to construct patient")
        return None

    birthdate = parse_date_from_string(enrollment.get('core_birthdate'))
    if not birthdate:
        LOG.warning(f"Record {enrollment.get('record_id')} has an invalid or missing `core_birthdate` value")

    location_resource_entries = build_residential_location_resources(
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

        if redcap_record_instance.event_name == ENROLLMENT_EVENT_NAME:
            event_type = EventType.ENROLLMENT
            #check_enrollment_data_quality(redcap_record_instance)

        elif redcap_record_instance.event_name == ENCOUNTER_EVENT_NAME:
            event_type = EventType.ENCOUNTER
            if is_complete('kiosk_registration_4c7f', redcap_record_instance):
                collection_method = CollectionMethod.KIOSK
            elif is_complete('test_order_survey', redcap_record_instance):
                collection_method = CollectionMethod.SWAB_AND_SEND
            elif is_complete('husky_test_kit_registration', redcap_record_instance):
                collection_method = CollectionMethod.UW_DROPBOX
        else:
            LOG.info(f"Skipping event: {redcap_record_instance.event_name!r} for record "
            f"{redcap_record_instance.get('record_id')} because the event is not one "
            "that we process")
            continue

        # Skip an ENCOUNTER instance if we don't have the data we need to
        # create an encounter.
        if event_type == EventType.ENCOUNTER:
            if not is_complete('daily_attestation', redcap_record_instance) \
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
            default_site = UW_DROPBOX_SITE if collection_method == CollectionMethod.UW_DROPBOX else SWAB_AND_SEND_SITE,
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
        if event_type == EventType.ENROLLMENT or collection_method in [CollectionMethod.SWAB_AND_SEND, CollectionMethod.UW_DROPBOX]:
            collection_code = CollectionCode.HOME_HEALTH
        elif collection_method == CollectionMethod.KIOSK:
            collection_code = CollectionCode.FIELD

        encounter_date = get_encounter_date(redcap_record_instance, event_type)

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
                    f"{redcap_record_instance.get('record_id')}")
                return None
            else:
                LOG.warning("Skipping record instance with insufficient information to construct the initial encounter "
                    f"for record: {redcap_record_instance.get('record_id')}, instance: "
                    f"{redcap_record_instance.get('redcap_repeat_instance')}")
                continue
 
        specimen_entry = None
        specimen_observation_entry = None
        specimen_received = (collection_method == CollectionMethod.SWAB_AND_SEND and \
            is_complete('post_collection_data_entry_qc', redcap_record_instance)) or \
            (collection_method == CollectionMethod.KIOSK and \
            is_complete('kiosk_registration_4c7f', redcap_record_instance)) or \
            (collection_method == CollectionMethod.UW_DROPBOX and \
            is_complete('husky_test_kit_registration', redcap_record_instance) and \
            redcap_record_instance["barcode_swabsend"])

        if specimen_received:
            # Use barcode fields in this order.
            prioritized_barcodes = [
                redcap_record_instance["collect_barcode_kiosk"],
                redcap_record_instance["return_utm_barcode"],
                redcap_record_instance["pre_scan_barcode"],
                redcap_record_instance["barcode_swabsend"]]

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
            LOG.warning("Skipping record instance. We think the specimen was received, "
                 "but we're unable to create the specimen_entry for record: "
                 f"{redcap_record_instance.get('record_id')}, instance: {redcap_record_instance.get('redcap_repeat_instance')}"
                 )
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
                    encounter_id = create_encounter_id(redcap_record_instance, True),
                    encounter_date = extract_date_from_survey_timestamp(redcap_record_instance, 'week_followup') \
                        or datetime.strptime(redcap_record_instance.get('fu_timestamp'),
                        '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'),
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
        source = f"{REDCAP_URL}{enrollment.project.id}/{enrollment.id}",
        entries = list(filter(None, persisted_resource_entries))
    )


def get_encounter_date(record: REDCapRecord, event_type: EventType) -> Optional[str]:
    # First try the attestation_date
    # from the daily attestation survey then try nasal_swab_timestamp from
    # the kiosk registration and finally the swab-and-send order date.
    # For all surveys, try the survey _timestamp field (which is in Pacific time)
    # before custom fields because the custom fields aren't always populated and when
    # they are populated they use the browser's time zone.
    # testing_determination_internal is not enabled as a survey, but we attempt to get its
    # timestamp just in case it ever is enabled as a survey.
    encounter_date = None

    if event_type == EventType.ENCOUNTER:
        encounter_date = extract_date_from_survey_timestamp(record, 'daily_attestation') \
            or record.get('attestation_date') \
            or extract_date_from_survey_timestamp(record, 'kiosk_registration_4c7f') \
            or (record.get('nasal_swab_timestamp') and datetime.strptime(record.get('nasal_swab_timestamp'),
                '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')) \
            or extract_date_from_survey_timestamp(record, 'test_order_survey') \
            or (record.get('time_test_order') and datetime.strptime(record.get('time_test_order'),
                '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')) \
            or extract_date_from_survey_timestamp(record, 'testing_determination_internal') \
            or record.get('testing_date') \
            or extract_date_from_survey_timestamp(record, 'husky_test_kit_registration') \
            or record.get('kit_reg_date')

        # We have seen cases when the `attestation_date` is not getting set
        # by REDCap in the `daily_attesation` instrument. Here, we get the date
        # based on the instance ID of the `daily_attestation` instrument. It's safe
        # to do this because the Musher computes the instance from the date.
        if encounter_date is None and is_complete('daily_attestation', record) and record.repeat_instance:
            encounter_date = get_date_from_repeat_instance(record.repeat_instance)

    elif event_type == EventType.ENROLLMENT:
        encounter_date = extract_date_from_survey_timestamp(record, 'enrollment_questionnaire') \
            or record.get('enrollment_date')

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

    if collection_method == CollectionMethod.KIOSK:
        collection_date = extract_date_from_survey_timestamp(record, "kiosk_registration_4c7f") or record.get("nasal_swab_q")

    elif collection_method in [CollectionMethod.SWAB_AND_SEND, CollectionMethod.UW_DROPBOX]:
        collection_date = record.get("date_on_tube") \
            or extract_date_from_survey_timestamp(record, "husky_test_kit_registration") \
            or record.get("kit_reg_date") \
            or extract_date_from_survey_timestamp(record, "test_fulfillment_form") \
            or record.get("back_end_scan_date")

    return collection_date


def create_enrollment_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the enrollment
    encounter (i.e. encounter of enrollment into the study)
    """

    # Do not include `core_age_years` because we calculate the age ourselves in the computed questionnaire.
    integer_questions = [
        'weight',
        #'height_total',
        #'tier'
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
        'vaccine_where',
        'uw_housing_group',
        'added_surveillance_groups',
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
        #'uw_greek_member',
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
        'covid_test_week_base',
        #'uw_housing_resident',
        #'on_campus_2x_week',
    ]

    '''
    decimal_questions = [
        'bmi'
    ]
    '''

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
        #'valueDecimal': decimal_questions
    }

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    # Combine all fields answering the same question
    record['countries_visited_base'] = combine_multiple_fields(record, 'country', '_base')
    record['states_visited_base'] = combine_multiple_fields(record, 'state', '_base')

    '''
    # Set the study tier
    tier = None
    if record['tier_1'] == '1':
        tier = 1
    elif record['tier_2'] == '1':
        tier = 2
    elif record['tier_3'] == '1':
        tier = 3
    record['tier'] = tier
    '''

    vaccine_item = create_vaccine_item(record["vaccine"], record['vaccine_year'], record['vaccine_month'], 'dont_know')

    '''
    # Set the UW housing group
    housing_group = None
    if record.get('uw_housing_group_a') == '1':
        housing_group = 'a'
    elif record.get('uw_housing_group_b') == '1':
        housing_group = 'b'
    elif record.get('uw_housing_group_c') == '1':
        housing_group = 'c'
    elif record.get('uw_housing_group_d') == '1':
        housing_group = 'd'
    elif record.get('uw_housing_group_e') == '1':
        housing_group = 'e'
    elif record.get('uw_housing_group_f') == '1':
        housing_group = 'f'
    elif record.get('uw_housing_group_g') == '1':
        housing_group = 'g'
    record['uw_housing_group'] = housing_group
    '''

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
    to communicate with the Priority Queue application.
    """
    boolean_questions = [
        'testing_trigger',
        'surge_selected_flag',
    ]

    string_questions = [
        'testing_type',
    ]

    date_questions = [
        'testing_date',
    ]

    question_categories = {
        'valueBoolean': boolean_questions,
        'valueString': string_questions,
        'valueDate': date_questions
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


def get_date_from_repeat_instance(instance_id: int) -> str:
    """
    Returns the date associated with a REDCap repeat instance.
    This is safe to do only when the Daily Attestation exists
    because the Musher sets the repeat instance based on the date.
    Returns the date as a string because that is how it is used.
    """
    return (STUDY_START_DATE + relativedelta(days=(instance_id -1))).strftime('%Y-%m-%d')

'''
def check_enrollment_data_quality(record: REDCapRecord) -> None:
    """
    Warns if the enrollment record violates data quality checks.
    """

    # UW Housing residence groups: a participant should be in at most 1 group.
    housing_groups = [f'uw_housing_group_{i}' for i in 'abcdefg']
    housing_group_count = sum(map(lambda group: int(record[group] or 0), housing_groups))
    if housing_group_count > 1:
        LOG.warning(f"Record {record['record_id']} enrollment data quality issue: "
        f"In {housing_group_count} UW Housing residence groups")

    # Weekly test invitations: a participant should not be in a UW Housing residence
    # group and also have a value set for `added_surveillance_groups`
    if housing_group_count > 0 and record['added_surveillance_groups']:
        LOG.warning(f"Record {record['record_id']} enrollment data quality issue: "
        "In a UW Housing residence group and has a value for `added_surveillance_groups`")
'''