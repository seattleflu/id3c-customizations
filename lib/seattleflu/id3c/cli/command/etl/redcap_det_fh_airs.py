"""
Process DETs for the Fred Hutch AIRS REDCap projects.
"""
from dateutil.relativedelta import relativedelta
from enum import Enum

from .redcap import *
from id3c.cli.command.etl import redcap_det
from id3c.cli.redcap import is_complete, Record as REDCapRecord
import logging


LOG = logging.getLogger(__name__)


class AIRSProject(NamedTuple):
    id: int
    lang: str
    command_name: str

    def __init__(self, project_id: int, lang: str, command_name: str) -> None:
        self.id = project_id
        self.lang = lang
        self.command_name = command_name

PROJECTS = [
    AIRSProject(1372, "en", 'airs')
]

LANGUAGE_CODE = {
    project.id: project.lang
        for project in PROJECTS }

class EventType(Enum):
    ENROLLMENT = 'enrollment'
    ENCOUNTER = 'encounter'

REVISION = 1

REDCAP_URL = 'https://redcap.fredhutch.org/'
INTERNAL_SYSTEM = "https://seattleflu.org"

ENROLLMENT_EVENT_NAME_PREFIX = 'screening_and_enro_arm'
ENCOUNTER_EVENT_NAME_PREFIX = 'week_'

REQUIRED_ENROLLMENT_INSTRUMENTS = [
    'informed_consent',
    'enrollment'
]

# A decorator lets us keep command registration up here at the top, instead of
# moving the loop after the definition of redcap_det_airs().
#
def command_for_each_project(function):
    """
    A decorator to register one redcap-det subcommand per AIRS project, each
    calling the same base *function*.

    Used for side-effects only; the original *function* is unmodified.
    """
    for project in PROJECTS:
        help_message = "Process REDCap DETs for AIRS (Fred Hutch)"

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

# To-DO: this needs work on, specifically recard_record_instance (site, location, symptoms, etc)
def redcap_det_airs(*, db: DatabaseSession, cache: TTLCache, det: dict,
    redcap_record_instances: List[REDCapRecord]) -> Optional[dict]:

    assert redcap_record_instances is not None and len(redcap_record_instances) > 0, \
        'The redcap_record_instances list was not populated.'

    enrollments = [record for record in redcap_record_instances if \
        record.event_name.startswith(ENROLLMENT_EVENT_NAME_PREFIX)]
    assert len(enrollments) == 1, \
        f'Record had {len(enrollments)} enrollments.'

    enrollment = enrollments[0]

    incomplete_enrollment_instruments = {
                instrument
                    for instrument
                    in REQUIRED_ENROLLMENT_INSTRUMENTS
                    if not is_complete(instrument, enrollment)
            }

    if incomplete_enrollment_instruments:
        LOG.debug(f'The following required enrollment instruments «{incomplete_enrollment_instruments}» are not yet marked complete.')
        return None

    # Create the participant resource entry and reference.
    patient_entry, patient_reference = create_patient_using_demographics(
        sex = enrollment.get['enr_sex'],
        first_name = enrollment['scr_first_name'],
        last_name = enrollment['scr_last_name'],
        birth_date = enrollment['enr_dob'],
        zipcode = enrollment['enr_mail_zip'],
        record = enrollment,
        system_identifier = INTERNAL_SYSTEM)

    if not patient_entry:
        LOG.warning('Skipping record with insufficient information to construct patient')
        return None

    location_resource_entries = build_residential_location_resources(
        db = db,
        cache = cache,
        housing_type = enrollment.get('enr_living_group'),
        primary_street_address = enrollment['enr_mail_street_address'],
        secondary_street_address = enrollment['enr_mail_street_address_2'],
        city = enrollment['enr_mail_city'],
        state = enrollment['enr_mail_state'],
        zipcode = enrollment['enr_mail_zip'],
        system_identifier = INTERNAL_SYSTEM)

    persisted_resource_entries = [patient_entry, *location_resource_entries]

    for redcap_record_instance in redcap_record_instances:

        event_type = None

        if redcap_record_instance.event_name.startswith(ENROLLMENT_EVENT_NAME_PREFIX):
            event_type = EventType.ENROLLMENT
        elif redcap_record_instance.event_name.startswith(ENCOUNTER_EVENT_NAME_PREFIX):
            event_type = EventType.ENCOUNTER
        else:
            LOG.error(f'The record instance has an unexpected event name: {redcap_record_instance.event_name}')
            continue

        # Skip an ENCOUNTER instance if we don't have the data we need to
        # create an encounter. Require the participant to have provided
        # survey data or a sample.
        # this project can trigger a test kit if PT fills out either weekly or symptom survey
        if event_type == EventType.ENCOUNTER \
            and not is_complete('weekly', redcap_record_instance) or not is_complete('symptoms', redcap_record_instance) \
            and not is_complete('swab_kit_reg', redcap_record_instance) or not is_complete('swab_kit_reg_2', redcap_record_instance) \
            and not is_complete('post_collection_data_entry_qc', redcap_record_instance) or not is_complete('post_collection_data_entry_qc_2', redcap_record_instance):
            LOG.debug('Skipping record instance with insufficient information to construct the encounter')
            continue

        # From this point on, log at the `warning` level if we have to skip the encounter.
        # That situation would be one we'd need to dig into.
        site_reference = create_site_reference(
            location = location,
            site_map = site_map,
            default_site = SWAB_AND_SEND_SITE,
            system_identifier = INTERNAL_SYSTEM)

        # Handle various symptoms.
        contained: List[dict] = []
        diagnosis: List[dict] = []

        # Map the various symptoms variables to their onset date.
        if event_type == EventType.ENCOUNTER:
            symptom_onset_map = {
                'airs_kit_activation': redcap_record_instance['symptom_duration_2'] or 'airs_kit_activation_2': redcap_record_instance['symptom_duration_2_v2'],
            }
            contained, diagnosis = build_contained_and_diagnosis(
                patient_reference = patient_reference,
                record = redcap_record_instance,
                symptom_onset_map = symptom_onset_map,
                system_identifier = INTERNAL_SYSTEM)

        encounter_date = get_encounter_date(redcap_record_instance, event_type)
        if not encounter_date:
            LOG.warning('Skipping record instance because we could not create an encounter_date')
            continue

        encounter_entry, encounter_reference = create_encounter(
            encounter_id = create_encounter_id(redcap_record_instance),
            encounter_date = encounter_date,
            patient_reference = patient_reference,
            site_reference = site_reference,
            locations = location_resource_entries,
            diagnosis = diagnosis,
            contained = contained,
            system_identifier = INTERNAL_SYSTEM,
            record = redcap_record_instance)

        # Skip the entire record if we can't create the enrollment encounter.
        # Otherwise, just skip the record instance.
        if not encounter_entry:
            if event_type == EventType.ENROLLMENT:
                LOG.warning('Skipping record because we could not create the enrollment encounter')
                return None
            else:
                LOG.warning('Skipping record instance with insufficient information to construct the encounter')
                continue
        specimen_entry = None
        specimen_observation_entry = None
        specimen_received = is_complete('post_collection_data_entry_qc', redcap_record_instance) or is_complete('post_collection_data_entry_qc_2', redcap_record_instance)

        if specimen_received:
            # Use barcode fields in this order.
            prioritized_barcodes = [
                redcap_record_instance['return_utm_barcode'] or redcap_record_instance['return_utm_barcode_v2'], # Post Collection Data Entry Qc
                redcap_record_instance['utm_tube_barcode_2'] or redcap_record_instance['utm_tube_barcode_2_v2'], # Scan Kit Reg
                redcap_record_instance['pre_scan_barcode_v1'] or redcap_record_instance['pre_scan_barcode_v2'] # Back End Mail Scans
                ]

            specimen_entry, specimen_reference = create_specimen(
                prioritized_barcodes = prioritized_barcodes,
                patient_reference = patient_reference,
                collection_date = get_collection_date(redcap_record_instance),
                sample_received_time = redcap_record_instance['samp_process_date'] or redcap_record_instance['samp_process_date_v2'],
                able_to_test = redcap_record_instance['able_to_test'] or redcap_record_instance['able_to_test_v2'],
                system_identifier = INTERNAL_SYSTEM)

            specimen_observation_entry = create_specimen_observation_entry(
                specimen_reference = specimen_reference,
                patient_reference = patient_reference,
                encounter_reference = encounter_reference)
        else:
            LOG.info('Creating encounter for record instance without sample')

        enrollment_questionnaire_entry = None
        encounter_questionnaire_entry = None
        operational_questionnaire_entry = None

        if event_type == EventType.ENROLLMENT:
            enrollment_questionnaire_entry = create_enrollment_questionnaire_response(
            enrollment, patient_reference, encounter_reference)


        current_instance_entries = [
            encounter_entry,
            enrollment_questionnaire_entry,
            encounter_questionnaire_entry,
            operational_questionnaire_entry,
            specimen_entry,
            specimen_observation_entry,
        ]

        persisted_resource_entries.extend(list(filter(None, current_instance_entries)))


    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f'{REDCAP_URL}{enrollment.project.id}/{enrollment.id}',
        entries = list(filter(None, persisted_resource_entries))
    )


def get_encounter_date(record: REDCapRecord, event_type: EventType) -> Optional[str]:
    """
    Determine the encounter_date from the given REDCap *record*.
    """
    encounter_date = None

    if event_type == EventType.ENROLLMENT:
        encounter_date = extract_date_from_survey_timestamp(record, 'enrollment').split()[0]

    elif event_type == EventType.ENCOUNTER:
        if record.get('wk_date'): #Weekly
            encounter_date = record['wk_date']
        elif record.get('ss_date'): #Symptom
            encounter_date = record['ss_date']
        elif record.get('kit_reg_date'): #Swab Kit Reg
            encounter_date = record['kit_reg_date']
        elif record.get('kit_reg_date_v2'): #Swab Kit Reg V2
            encounter_date = record['kit_reg_date_v2']
        elif record.get('samp_process_date'): #Post Collection Data Entry Qc
            encounter_date = record['samp_process_date'].split()[0]
        elif record.get('samp_process_date_v2'): #Post Collection Data Entry Qc V2
            encounter_date = record['samp_process_date_v2'].split()[0]

    return encounter_date


def create_encounter_id(record: REDCapRecord) -> str:
    """
    Create the hashed encounter_id from the REDCap *record*.
    """
    if record.event_name:
        redcap_event_name = record.event_name
    else:
        redcap_event_name = ''

    return generate_hash(f'{record.project.base_url}{record.project.id}/{record.id}/{redcap_event_name}')


def get_collection_date(record: REDCapRecord) -> Optional[str]:
    """
    Determine sample/specimen collection date from the given REDCap *record*.
    """
    return record['date_on_tube'] or record['date_on_tube_v2'] \
        or record['kit_reg_date'] or record['kit_reg_date_v2'] \
        or extract_date_from_survey_timestamp(record, 'airs_kit_activation') or extract_date_from_survey_timestamp(record, 'airs_kit_activation_v2') \
        or record['back_end_scan_date'] or record['back_end_scan_date_v2']


def create_enrollment_questionnaire_response(record: REDCapRecord,
    patient_reference: dict, encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the screening and enrollment
    encounter (i.e. encounter of screening and enrollment into the study)

    Do not include PII identifiers.

    """

    integer_questions = [
        'enr_living_population',
    ]

    # Do not include PII identifiers.
    string_questions = [
        'enr_living_group',
        'enr_other_group',
        'scr_contact_pub',
        'scr_num_doses',
        'scr_dose1_man',
        'scr_dose1_man_other'
        'scr_dose2_man',
        'scr_dose2_man_other',
        'scr_dose3_man',
        'scr_dose3_man_other',
        'scr_vacc_infl',
    ]

    date_questions = [
        'scr_dose1_date',
        'scr_dose2_date',
        'scr_dose3_date',
        'scr_vacc_infl_date',
    ]

    boolean_questions = [
        'enr_living_alone',
        'enr_living_area',
        'enr_outside_mask',
        'enr_indoor_no_mask',
        'enr_outdoor_no_mask',

    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'enr_mask_type',
    ]

    question_categories = {
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueString': string_questions,
        'valueDate': date_questions,
    }

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM


def create_weekly_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the weekly encounter
    for the weekly survey (i.e. weekly encounter for testing)
    """

    string_questions = [
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
        'wk_hoarse',
        'wk_sore_throat',
        'wk_diarrhea',
        'wk_nausea',
        'wk_stomach_pain',
        'wk_vomiting',

    ]

    date_questions = [
        'wk_symp_start_date',
        'wk_symp_stop_date',
        'wk_date',

    ]

    boolean_questions = [
        'wk_nasal',
        'wk_chest_symptoms',
        'wk_sensory_symptoms',
        'wk_eye_ear_throat',
        'wk_gi',
    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'wk_which_med',
    ]

    question_categories = {
        'valueString': string_questions,
        'valueDate': date_questions,
        'valueBoolean': boolean_questions,
    }

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)


    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)

def create_symptoms_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the symptoms
    for the symptoms survey (i.e. symptoms encounter for testing)
    """

    string_questions = [
        'ss__congestion',
        'ss__nasal_drip',
        'ss__runny_nose',
        'ss__sinus_pain',
        'ss__sneezing',
        'ss__chest_pain',
        'ss__cough',
        'ss__sob',
        'ss__sputum',
        'ss__wheeze',
        'ss__smell',
        'ss__taste',
        'ss__chill',
        'ss__fatigue',
        'ss__fever',
        'ss__headache',
        'ss__sleeping',
        'ss__myalgia',
        'ss__skin_rash',
        'ss__sweats',
        'ss__ear_congestion',
        'ss__ear_pain',
        'ss__eye_pain',
        'ss__hoarse',
        'ss__hoarse',
        'ss__sore_throat',
        'ss__diarrhea',
        'ss__nausea',
        'ss__stomach_pain',
        'ss__vomiting',

    ]

    date_questions = [
        'ss__ealiest_date',
        'ss__date',

    ]

    boolean_questions = [
        'ss__nasal',
        'ss__chest_symptoms',
        'ss__sensory_symptoms',
        'ss__eye_ear_throat',
        'ss__gi',
    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
    ]

    question_categories = {
        'valueString': string_questions,
        'valueDate': date_questions,
        'valueBoolean': boolean_questions,
    }

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)


    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)

# TO-DO: create function for the swab_results and swab_results_2
