"""
Process DETs for the Childcare REDCap projects.
Out of concern for privacy (PII), for this project we decided not
to store the participant's age on encounters. The `age` column
on warehouse.encounter is null for encounters created.
"""
from dateutil.relativedelta import relativedelta
from enum import Enum

from .redcap import *
from id3c.cli.command.etl import redcap_det
from id3c.cli.redcap import is_complete, Record as REDCapRecord
import logging


LOG = logging.getLogger(__name__)


class ChildcareProject():
    id: int
    lang: str
    command_name: str


    def __init__(self, project_id: int, lang: str, command_name: str) -> None:
        self.id = project_id
        self.lang = lang
        self.command_name = command_name

PROJECTS = [
        ChildcareProject(23740, 'en', 'childcare'),
        ChildcareProject(29351, 'en', 'childcare-2021')
    ]

LANGUAGE_CODE = {
project.id: project.lang
    for project in PROJECTS }

class EventType(Enum):
    ENROLLMENT = 'enrollment'
    ENCOUNTER = 'encounter'

class StudyArm(Enum):
    PRIMARY = 'primary'
    SECONDARY = 'secondary'

REVISION = 1

REDCAP_URL = 'https://redcap.iths.org/'
INTERNAL_SYSTEM = 'https://seattleflu.org'

SWAB_AND_SEND_SITE = 'ChildcareSwabNSend'
RADFORD_SITE = 'UWChildrensCenterRadfordCourt'
SANDPOINT_SITE = 'ChildcareCenter70thAndSandPoint'
PORTAGE_BAY_SITE = 'UWChildrensCenterPortageBay'
MINOR_SITE = 'MinorAvenueChildrensHouse'
MAINTINYTOTS_SITE = 'TinyTotsDevelopmentCenterMain'
EASTTINYTOTS_SITE = 'TinyTotsDevelopmentCenterEast'
DLBEACON_SITE = 'DeniseLouieBeaconHill'
DLMAG_SITE = 'DeniseLouieMercyMagnusonPl'
MIGHTY_SITE = 'MightyKidz'
BIRCH_SITE = 'BirchTreeAcademy'
MOTHERS_SITE = 'MothersPlace'
UWCHILDRENS_WEST_SITE = 'UWChildrensCenterWestCampus'
UWCHILDRENS_LAUREL_SITE = 'UWChildrensCenterLaurelVillage'

ENROLLMENT_EVENT_NAME_PREFIX = 'enrollment_arm_'
ENCOUNTER_EVENT_NAME_PREFIX = 'week'
UNSCHEDULED_ENCOUNTER_EVENT_NAME = 'unscheduled_arm_1'
COLLECTION_CODE = CollectionCode.HOME_HEALTH


# Don't include baseline_screening because it's completed by a staff member,
# not the participant.
REQUIRED_ENROLLMENT_INSTRUMENTS = [
    'consentassent_form',
    'enrollment_questionnaire'
]


def command_for_each_project(function):
    """
    A decorator to register one redcap-det subcommand per REDCap project, each
    calling the same base *function*.
    Used for side-effects only; the original *function* is unmodified.
    """
    for project in PROJECTS:
        help_message = 'Process REDCap DETs for the Childcare Study'

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
def redcap_det_childcare(*, db: DatabaseSession, cache: TTLCache, det: dict,
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

    # If the participant's age < 18 ensure we have parental consent.
    if (enrollment['core_age_years'] == "" or int(enrollment['core_age_years']) < 18) and \
        enrollment['parent_signature'] == '':
        LOG.debug("The participant is < 18 years old and we do not have parental consent. Skipping record.")
        return None

    # Create the participant resource entry and reference.
    patient_entry, patient_reference = create_patient_using_demographics(
        sex = 'unknown', # Set to unknown so that we don't ingest identifiers
        preferred_language = enrollment.get('language'),
        first_name = enrollment['core_participant_first_name'],
        last_name = enrollment['core_participant_last_name'],
        birth_date = enrollment['core_birthdate'],
        zipcode = enrollment['core_zipcode'],
        record = enrollment,
        system_identifier = INTERNAL_SYSTEM)

    if not patient_entry:
        LOG.warning('Skipping record with insufficient information to construct patient')
        return None

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

    childcare_center = enrollment['childcare_center']

    for redcap_record_instance in redcap_record_instances:

        event_type = None
        study_arm = None

        if redcap_record_instance.event_name.startswith(ENROLLMENT_EVENT_NAME_PREFIX):
            event_type = EventType.ENROLLMENT
        elif redcap_record_instance.event_name.startswith(ENCOUNTER_EVENT_NAME_PREFIX) \
            or redcap_record_instance.event_name == UNSCHEDULED_ENCOUNTER_EVENT_NAME:
            event_type = EventType.ENCOUNTER
        else:
            LOG.error(f'The record instance has an unexpected event name: {redcap_record_instance.event_name}')
            continue

        if '_arm_1' in redcap_record_instance.event_name:
            study_arm = StudyArm.PRIMARY
        elif '_arm_2' in redcap_record_instance.event_name:
            study_arm = StudyArm.SECONDARY
        else:
            LOG.error(f'The record instance has an unexpected study arm in the event name: {redcap_record_instance.event_name}')
            continue

        # Skip an ENCOUNTER instance if we don't have the data we need to
        # create an encounter. Require the participant to have provided
        # survey data or a sample.
        if event_type == EventType.ENCOUNTER \
            and not is_complete('symptom_check', redcap_record_instance) \
            and not is_complete('swab_kit_reg', redcap_record_instance) \
            and not is_complete('post_collection_data_entry_qc', redcap_record_instance):
            LOG.debug('Skipping record instance with insufficient information to construct the encounter')
            continue

        # From this point on, log at the `warning` level if we have to skip the encounter.
        # That situation would be one we'd need to dig into.

        # Create the site reference for the encounter. For primary participants, use
        # a completed `return_pickup` survey to indicate that they are having their
        # sample picked up from home instead of returning it to a dropbox.
        site_map = {
            'childcare_room_70th': SANDPOINT_SITE,
            'childcare_room_radford' : RADFORD_SITE,
            'childcare_room_portage': PORTAGE_BAY_SITE,
            'childcare_room_minor': MINOR_SITE,
            'childcare_room_maintinytots': MAINTINYTOTS_SITE,
            'childcare_room_easttinytots': EASTTINYTOTS_SITE,
            'childcare_room_dlbeacon': DLBEACON_SITE,
            'childcare_room_dlmag': DLMAG_SITE,
            'childcare_room_mighty': MIGHTY_SITE,
            'childcare_room_birch': BIRCH_SITE,
            'childcare_room_mothers': MOTHERS_SITE,
            'childcare_room_wcampus': UWCHILDRENS_WEST_SITE,
            'childcare_room_laurel': UWCHILDRENS_LAUREL_SITE
        }

        location = None # No location will cause `create_site_reference` to use the `default_site` value.

        if study_arm == StudyArm.PRIMARY and event_type == EventType.ENCOUNTER and \
            not is_complete('return_pickup', redcap_record_instance):
            location = childcare_center

        site_reference = create_site_reference(
            location = location,
            site_map = site_map,
            default_site = SWAB_AND_SEND_SITE,
            system_identifier = INTERNAL_SYSTEM)

        # Handle various symptoms.
        contained: List[dict] = []
        diagnosis: List[dict] = []

        # Map the various symptoms variables to their onset date.
        # The PRIMARY arm does not get the symptom survey at enrollment,
        # but the SECONDARY arm does.
        if event_type == EventType.ENCOUNTER or study_arm == StudyArm.SECONDARY:
            symptom_onset_map = {
                'symptoms_check': redcap_record_instance['symptom_duration'],
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
            collection_code = COLLECTION_CODE,
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
        specimen_received = is_complete('post_collection_data_entry_qc', redcap_record_instance)

        if specimen_received:
            # Use barcode fields in this order.
            prioritized_barcodes = [
                redcap_record_instance['return_utm_barcode'], # Post Collection Data Entry Qc
                redcap_record_instance['utm_tube_barcode'], # Scan Kit Reg
                redcap_record_instance['pre_scan_barcode'] # Back End Mail Scans
                ]

            specimen_entry, specimen_reference = create_specimen(
                prioritized_barcodes = prioritized_barcodes,
                patient_reference = patient_reference,
                collection_date = get_collection_date(redcap_record_instance),
                sample_received_time = redcap_record_instance['samp_process_date'],
                able_to_test = redcap_record_instance['able_to_test'],
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
            enrollment, study_arm, patient_reference, encounter_reference)

        # The SECONDARY arm gets "encounter" surveys in the ENROLLMENT event.
        if event_type == EventType.ENCOUNTER or study_arm == StudyArm.SECONDARY:
            encounter_questionnaire_entry = create_encounter_questionnaire_response(
            redcap_record_instance, patient_reference, encounter_reference)

            operational_questionnaire_entry = create_operational_questionnaire_response(
            redcap_record_instance, patient_reference, encounter_reference)

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
        encounter_date = record.get('enrollment_date')

    elif event_type == EventType.ENCOUNTER:
        if record.get('today_2'): #Symptom Check
            encounter_date = record['today_2']
        elif record.get('kit_reg_date'): #Swab Kit Reg
            encounter_date = record['kit_reg_date']
        elif record.get('samp_process_date'): #Post Collection Data Entry Qc
            encounter_date = record['samp_process_date'].split()[0]

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
    return record['date_on_tube'] or record['kit_reg_date'] \
        or extract_date_from_survey_timestamp(record, 'symptom_check') \
        or record['back_end_scan_date']


def create_enrollment_questionnaire_response(record: REDCapRecord, study_arm: StudyArm,
    patient_reference: dict, encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the enrollment
    encounter (i.e. encounter of enrollment into the study)

    Do not include PII identifiers.

    Include info about the current year's flu vaccine as an additional item.
    """

    integer_questions = [
        'weight',
        'inches_total',
    ]

    # Do not include PII identifiers.
    string_questions = [
        'study_arm',
        'text_or_email',
        'core_housing_type',
        'core_house_members',
        'core_health_risk',
        'core_tobacco_use',
        'ace',
        'novax_reason',
        'vaccine_method',
        'vaccine_where',
        'attend_event_3',
        'hide_cough',
        'mask',
        'distance',
        'wash_hands',
        'clean_surfaces',
    ]

    boolean_questions = [
        'vaccine_hx',
    ]

    decimal_questions = [
        'bmi',
    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'core_health_risk',
        'core_tobacco_use',
        'ace',
    ]

    question_categories = {
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueString': string_questions,
        'valueDecimal': decimal_questions
    }

    record['study_arm'] = study_arm.value

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    vaccine_item = create_vaccine_item(record['vaccine'], record['vaccine_year'], record['vaccine_month'], 'dont_know')

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM,
        additional_items = [vaccine_item])


def create_encounter_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the encounter
    encounter (i.e. encounter for testing)
    """

    string_questions = [
        'sought_care',
        'hospital_ed',
        'contact',
        'attend_event',
    ]

    date_questions = [
        'hospital_arrive',
        'hospital_leave',
    ]

    boolean_questions = [
        'symptoms_y_n',
        'sick_pick_up',
    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'sought_care',
        'contact',
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


def create_operational_questionnaire_response(record: REDCapRecord, patient_reference: dict,
    encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry to capture info
    to be used for operational reporting
    """
    string_questions = [
        'date_on_tube_match_kit_reg_text',
        'qc_comments_type',
        'packaging_errors',
        'utm_tube_errors',
        'ship_carrier',
    ]

    date_questions = [
        'symptom_check_timestamp',
        'kit_reg_date_time',
        'samp_process_date',
        'return_pu_date_time',
        'back_end_scan',
    ]

    boolean_questions = [
        'name_verification',
        'name_verification_sec',
        'able_to_test',
        'return_date_time_ce_m',
        'return_date_time_ce_e',
    ]

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'qc_comments_type',
        'packaging_errors',
        'utm_tube_errors',
    ]

    question_categories = {
        'valueString': string_questions,
        'valueDate': date_questions,
        'valueBoolean': boolean_questions,
    }

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    date_on_tube_match_kit_reg_map = {
        '1': 'yes',
        '0': 'no',
        '2': 'no date',
        }

    if record.get('date_on_tube_match_kit_reg'):
        record['date_on_tube_match_kit_reg_text'] = \
            date_on_tube_match_kit_reg_map[record['date_on_tube_match_kit_reg']]

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)
