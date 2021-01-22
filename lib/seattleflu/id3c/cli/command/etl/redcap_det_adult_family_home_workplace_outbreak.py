"""
Process DETs for the Adult Family Home outbreak and Workplace outbreak project.
"""
from enum import Enum

from . import first_record_instance
from .redcap import *
from id3c.cli.command.etl import redcap_det
from id3c.cli.redcap import is_complete, Record as REDCapRecord
import logging
from seattleflu.id3c.cli.command import age_ceiling


LOG = logging.getLogger(__name__)

REVISION = 0

REDCAP_URL = 'https://redcap.iths.org/'
INTERNAL_SYSTEM = 'https://seattleflu.org'
COLLECTION_CODE = CollectionCode.FIELD # Samples are always collected at the site

REQUIRED_INSTRUMENTS = [
    'registration_information',
    'sample_collection'
]

# We are not ingesting the actual AFH or workplace site name
# due to PII concerns and because sites will be added dynamically
# as homes and workplaces get included over time. We do need a site
# to associate the encounter with, so we're using these generic ones.
GENERIC_AFH_SITE = 'GenericAdultFamilyHomeOutbreakSite'
GENERIC_WORKPLACE_SITE = 'GenericWorkplaceOutbreakSite'

class SiteType(Enum):
    ADULT_FAMILY_HOME = 'adult-family-home'
    WORKPLACE = 'workplace'


class ParticipantRole(Enum):
    STAFF = 'staff'
    RESIDENT = 'resident'
    OTHER = 'other'


SITE_MAP = {
    SiteType.ADULT_FAMILY_HOME: GENERIC_AFH_SITE,
    SiteType.WORKPLACE: GENERIC_WORKPLACE_SITE
    }


class OutbreakProject():
    id: int
    lang: str
    command_name: str


    def __init__(self, project_id: int, lang: str, command_name: str) -> None:
        self.id = project_id
        self.lang = lang
        self.command_name = command_name


PROJECTS = [
        # This REDCap project is named `Clinical COVID Testing (Congregate Settings)`
        OutbreakProject(27619, 'en', 'afh-workplace-outbreak')
    ]

LANGUAGE_CODE = {
project.id: project.lang
    for project in PROJECTS }


def command_for_each_project(function):
    """
    A decorator to register one redcap-det subcommand per REDCap project, each
    calling the same base *function*.
    Used for side-effects only; the original *function* is unmodified.
    """
    for project in PROJECTS:
        help_message = 'Process REDCap DETs for the Adult Family Home outbreak '
        'and Workplace outbreak project'

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
# This project is the REDCap classic type. We should get only one instance per record.
@first_record_instance
def redcap_det_adult_family_home_workplace_outbreak(*, db: DatabaseSession, cache: TTLCache, det: dict,
    redcap_record: REDCapRecord) -> Optional[dict]:

    incomplete_enrollment_instruments = {
                instrument
                    for instrument
                    in REQUIRED_INSTRUMENTS
                    if not is_complete(instrument, redcap_record)
            }

    if incomplete_enrollment_instruments:
        LOG.debug(f'The following required enrollment instruments «{incomplete_enrollment_instruments}» are not yet marked complete.')
        return None

    if redcap_record['site_type'] == 'afh':
        siteType = SiteType.ADULT_FAMILY_HOME
    elif redcap_record['site_type'] == 'workplace':
        siteType = SiteType.WORKPLACE
    else:
        LOG.warning(f'Skipping record {redcap_record.get("record_id")} with unrecognized site type {redcap_record.get("site_type")!r}')
        return None

    if redcap_record['patient_role'] == 'staff':
        participant_role = ParticipantRole.STAFF
    elif redcap_record['patient_role'] == 'resident':
        participant_role = ParticipantRole.RESIDENT
    elif redcap_record['patient_role'] == 'etc':
        participant_role = ParticipantRole.OTHER
    else:
        LOG.warning(f'Skipping record {redcap_record.get("record_id")} with unrecognized participant role {redcap_record.get("patient_role")!r}')
        return None

    patient_entry, patient_reference = create_patient_using_demographics(
        sex = redcap_record['core_sex'],
        preferred_language = redcap_record['language'],
        first_name = redcap_record['core_participant_first_name'],
        last_name = redcap_record['core_participant_last_name'],
        birth_date = redcap_record['core_birthdate'],
        zipcode = redcap_record['core_home_zipcode'],
        record = redcap_record,
        system_identifier = INTERNAL_SYSTEM)

    if not patient_entry:
        LOG.warning(f'Skipping record {redcap_record.get("record_id")} with insufficient information to construct patient')
        return None

    location_resource_entries = None

    # Home address is not captured for AFH residents
    if participant_role != ParticipantRole.RESIDENT:
        location_resource_entries = build_residential_location_resources(
            db = db,
            cache = cache,
            housing_type = None, # The registration questionnaire does not ask housing type
            primary_street_address = redcap_record['core_home_street'],
            secondary_street_address = redcap_record['core_apartment_number'],
            city = redcap_record['core_home_city'],
            state = redcap_record['core_home_state'],
            zipcode = redcap_record['core_home_zipcode'],
            system_identifier = INTERNAL_SYSTEM)

    site_reference = create_site_reference(
            location = siteType,
            site_map = SITE_MAP,
            default_site = None,
            system_identifier = INTERNAL_SYSTEM)

    encounter_date = get_encounter_date(redcap_record)
    if not encounter_date:
        LOG.warning(f'Skipping record {redcap_record.get("record_id")} because we could not create an encounter_date')
        return None

    encounter_entry, encounter_reference = create_encounter(
        encounter_id = create_encounter_id(redcap_record),
        encounter_date = encounter_date,
        patient_reference = patient_reference,
        site_reference = site_reference,
        locations = location_resource_entries,
        diagnosis = None, # We do not ask the participant to select specific symptoms
        contained = None, # We do not ask the participant to select specific symptoms
        collection_code = COLLECTION_CODE,
        system_identifier = INTERNAL_SYSTEM,
        record = redcap_record)

    if not encounter_entry:
        LOG.warning(f'Skipping record {redcap_record.get("record_id")} because we could not create the encounter')
        return None

    # Use barcode fields in this order
    prioritized_barcodes = [
        redcap_record['core_collection_barcode'], # Sample Collection
        redcap_record['return_collection_barcode'], # Post Collection Data Entry Qc
        ]

    specimen_entry, specimen_reference = create_specimen(
        prioritized_barcodes = prioritized_barcodes,
        patient_reference = patient_reference,
        collection_date = get_collection_date(redcap_record),
        sample_received_time = redcap_record['samp_process_date'],
        able_to_test = redcap_record['able_to_test'],
        system_identifier = INTERNAL_SYSTEM)

    specimen_observation_entry = create_specimen_observation_entry(
        specimen_reference = specimen_reference,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference)

    # We skip the record if the Sample Collection instrument is not complete.
    # We should have a barcode to create a specimen_entry. If that's not the case, do warn and skip.
    if not specimen_entry:
        LOG.warning(f'Skipping record {redcap_record.get("record_id")} because we could not create the specimen')
        return None

    registration_questionnaire_entry = create_registration_questionnaire_response(
        redcap_record, patient_reference, encounter_reference)

    entries = [patient_entry, encounter_entry, specimen_entry, specimen_observation_entry, registration_questionnaire_entry]

    if location_resource_entries:
        entries.extend(location_resource_entries)


    return create_bundle_resource(
        bundle_id = str(uuid4()),
        timestamp = datetime.now().astimezone().isoformat(),
        source = f'{REDCAP_URL}{redcap_record.project.id}/{redcap_record.id}',
        entries = entries
        )


def get_encounter_date(record: REDCapRecord) -> Optional[str]:
    """
    Determine from the given REDCap *record* the date when the participant
    was enrolled into the project.
    """
    return extract_date_from_survey_timestamp(record, 'registration_information') or \
        record['registration_date']


def create_encounter_id(record: REDCapRecord) -> str:
    """
    Create the hashed encounter_id from the REDCap *record*.
    """
    return generate_hash(f'{record.project.base_url}{record.project.id}/{record.id}')


def get_collection_date(record: REDCapRecord) -> Optional[str]:
    """
    Determine the sample/specimen collection date from the given REDCap *record*.
    """
    return record['date_on_tube'] or \
        extract_date_from_survey_timestamp(record, 'sample_collection') or \
        record['sample_collection_date']


def create_registration_questionnaire_response(record: REDCapRecord,
    patient_reference: dict, encounter_reference: dict) -> Optional[dict]:
    """
    Returns a FHIR Questionnaire Response resource entry for the registration questionnaire.

    Do not include PII identifiers, such as the AFH name or workplace name.
    """

    # Do not include PII identifiers.
    # Not including `language` because it's a free text field.
    string_questions = [
        'site_type',
        'patient_role',
    ]

    date_questions = [
        'event_date',
    ]

    boolean_questions = [
        'core_latinx',
        'symptomatic',
    ]

    integer_questions = [
        'age',
    ]

    coding_questions = [
        'core_race',
    ]

    question_categories = {
        'valueString': string_questions,
        'valueDate': date_questions,
        'valueBoolean': boolean_questions,
        'valueInteger': integer_questions,
        'valueCoding': coding_questions,
    }

    # Do some pre-processing
    # Combine checkbox answers into one list
    checkbox_fields = [
        'core_race',
    ]

    for field in checkbox_fields:
        record[field] = combine_checkbox_answers(record, field)

    # Age Ceiling
    try:
        record['age'] = age_ceiling(int(record['age']))
    except ValueError:
        record['age'] = None

    return create_questionnaire_response(
        record = record,
        question_categories = question_categories,
        patient_reference = patient_reference,
        encounter_reference = encounter_reference,
        system_identifier = INTERNAL_SYSTEM)
