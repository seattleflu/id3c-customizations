"""
Parse and upload clinical data.

Clinical data will contain PII (personally identifiable information) and
unnecessary information that does not need to be stored. This process will only
pull out specific columns of interest that will then be stored in the receiving
schema of ID3C.
"""
import click
import hashlib
import logging
import os
import re
import pandas as pd
import id3c.db as db
import time
import requests
import hmac
import base64
import json
import glob
from datetime import datetime, timezone
from functools import partial
from math import ceil
from typing import Optional, List, Dict
from id3c.db.session import DatabaseSession
from id3c.cli import cli
from id3c.cli.io.pandas import dump_ndjson, load_file_as_dataframe, read_excel
from id3c.cli.command.geocode import get_geocoded_address
from id3c.cli.command.location import location_lookup
from id3c.cli.command.de_identify import generate_hash
from id3c.cli.command import pickled_cache
from dateutil.relativedelta import relativedelta
from .etl.redcap_map import map_sex
from .etl.fhir import generate_patient_hash
from . import (
    add_provenance,
    age_ceiling,
    barcode_quality_control,
    trim_whitespace,
    group_true_values_into_list,
)

LOG = logging.getLogger(__name__)
PHSKC_IDENTIFIERS = {
    'main_cid': 'phskcCid',
    'all_cids': 'phskcCid',
    'phskc_barcode': 'phskcCid',
}

@cli.group("clinical", help = __doc__)
def clinical():
    pass

# UW Clinical subcommand
@clinical.command("parse-uw")
@click.argument("uw_filename", metavar = "<UW Clinical Data filename>")
@click.option("-o", "--output", metavar="<output filename>",
    help="The filename for the output of missing barcodes")


def parse_uw(uw_filename, output):
    """
    Process clinical data from UW.

    Given a <UW Clinical Data filename> of an Excel document, selects specific
    columns of interest and reformats the queried data into a stream of JSON
    documents suitable for the "upload" sibling command.

    <output filename> is the desired filepath of the output CSV of problematic
    barcodes encountered while parsing. If not provided, the problematic
    barcodes print to the log.

    All clinical records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    if uw_filename.endswith('.csv'):
        read = pd.read_csv
    else:
        read = pd.read_excel

    read_uw = partial(
        read,
        dtype = {'tract_identifier': 'string'},
        parse_dates = ['Collection.Date', 'LabDtTm'],
        na_values = ['NA', '', 'Unknown', 'NULL'],
    )

    clinical_records = (
        read_uw(uw_filename)
            .pipe(trim_whitespace)
            .pipe(add_provenance, uw_filename)
            .pipe(coalesce_columns, "encountered", "Collection.Date", "LabDtTm")
            .pipe(create_unique_identifier))

    # Standardize names of columns that will be added to the database
    column_map = {
        'Age': 'age',
        'Collection_ID': 'barcode',
        'EthnicGroup': 'HispanicLatino',
        'Fac': 'site',
        'encountered': 'encountered',
        'PersonID': 'individual',
        'Race': 'Race',
        'Sex': 'AssignedSex',
        'tract_identifier': 'census_tract',
        'fluvaccine': 'FluShot',
        'identifier': 'identifier',
        '_provenance': '_provenance',
    }

    clinical_records = clinical_records.rename(columns=column_map)

    # Normalize barcode to strings and lowercase
    clinical_records['barcode'] = clinical_records['barcode'].str.lower()
    clinical_records['individual'] = clinical_records['individual'].str.lower()

    barcode_quality_control(clinical_records, output)

    # Age must be converted to Int64 dtype because pandas does not support NaNs
    # with normal type 'int'
    clinical_records["age"] = clinical_records["age"].astype(pd.Int64Dtype())

    # Subset df to drop missing barcodes
    drop_missing_rows(clinical_records, 'barcode')

    # Drop columns we're not tracking
    clinical_records = clinical_records[column_map.values()]

    remove_pii(clinical_records)


    dump_ndjson(clinical_records)


def coalesce_columns(df: pd.DataFrame, new_column: str, column_a: str, column_b: str) -> pd.DataFrame:
    """
    Coalesces values from *column_a* and *column_b* of *df* into *new_column*.
    """
    return df.assign(**{new_column: df[column_a].combine_first(df[column_b])})


def create_unique_identifier(df: pd.DataFrame):
    """Generate a unique identifier for each encounter and drop duplicates"""

    # This could theoretically use the EID (encounter id) column provided to
    # us, but sticking to this constructed identifier has two benefits I see:
    #
    # 1. We will continue to match existing data if we get updated records or
    #    re-process old datasets.  This is somewhat unlikely, but possible.
    #
    # 2. More importantly, a clinical encounter may span multiple days (unlike
    #    those in ID3C) and so multiple samples may be collected on different
    #    days from one encounter.  We want to keep treating those as multiple
    #    encounters on our end.
    #
    #   -trs, 2 Dec 2019

    df['identifier'] = (df['labMRN'] + df['LabAccNum'] + \
                        df['encountered'].astype('string')
                        ).str.lower()
    return df.drop_duplicates(subset="identifier")

def remove_pii(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove personally identifiable information from a given *df*.
    Return the new DataFrame.
    """
    df['age'] = df['age'].apply(age_ceiling)
    df["individual"] = df["individual"].apply(generate_hash)
    df["identifier"] = df["identifier"].apply(generate_hash)


def drop_missing_rows(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Filters the given *df* by removing rows with ``null`` values
    for the given *column*.
    """
    df = df.loc[df[column].notnull()]

@clinical.command("parse-sch")
@click.argument("sch_filename", metavar = "<SCH Clinical Data filename>")
@click.option("-o", "--output", metavar="<output filename>",
    help="The filename for the output of missing barcodes")
@click.option("--manifest-format",
    metavar="<manifest format>",
    default="year5",
    type=click.Choice(['year1','year2','year3','year4','year5']),
    help="The format of input manifest file; default is \"year5\"")

def parse_sch(sch_filename, manifest_format, output):
    """
    Process clinical data from SCH.

    All clinical records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    clinical_records = load_file_as_dataframe(sch_filename) \
                        .replace({"": None, "NA": None})

    # drop records with no patient identifier
    clinical_records.dropna(subset=['pat_id2'],inplace=True)

    clinical_records['age'] = clinical_records['age'].astype('float')

    clinical_records = trim_whitespace(clinical_records)
    clinical_records = add_provenance(clinical_records, sch_filename)
    add_insurance(clinical_records)
    add_icd10(clinical_records)

    # Standardize column names
    column_map = {
        "pat_id2": "individual",
        "study_id": "barcode",
        "drawndate": "encountered",
        "sex": "AssignedSex",
        "ethnicity": "HispanicLatino",
        "race": "Race",
        "admit_during_this_encounter": "AdmitDuringThisEncounter",
        "admit_to_icu": "AdmitToICU"
    }

    # Accounting for differences in format for year 3
    if manifest_format in ['year1', 'year2']:
        column_map["vaccine_given"] = "FluShot"
    elif manifest_format in ['year3', 'year4', 'year5']:
        column_map.update({
            "flu_vx_12mo": "FluShot",
            "flu_date": "FluShotDate",
            "covid_screen": "CovidScreen",
            "covid_vx_d1": "CovidShot1",
            "cov_d1_date": "CovidShot1Date",
            "covid_vx_d2": "CovidShot2",
            "cov_d2_date": "CovidShot2Date",
        })
        if manifest_format == 'year3':
            column_map["covid_vx_manu"] = "CovidShotManufacturer"
        elif manifest_format in ['year4', 'year5']:
            column_map.update({
                "covid_vx_manu1": "CovidShot1Manu",
                "covid_vx_manu2": "CovidShot2Manu",
                "covid_vx_manu3": "CovidShot3Manu",
                "covid_vx_d3": "CovidShot3",
                "cov_d3_date": "CovidShot3Date",
                "covid_vx_manu4": "CovidShot4Manu",
                "covid_vx_d4": "CovidShot4",
                "cov_d4_date": "CovidShot4Date",
            })
            # Year 5 files and final Year 4 file include these columns for a fifth dose
            if 'covid_vx_manu5' in clinical_records.columns:
                column_map.update({
                    "covid_vx_manu5": "CovidShot5Manu",
                    "covid_vx_d5": "CovidShot5",
                    "cov_d5_date": "CovidShot5Date",
                })


    else:
        # Don't fall through silently
        LOG.warning(f"Invalid manifest_format {manifest_format}")

    clinical_records = clinical_records.rename(columns=column_map)

    barcode_quality_control(clinical_records, output)

    # Subset df to drop missing encountered date
    drop_missing_rows(clinical_records, 'encountered')

    # Drop unnecessary columns
    columns_to_keep = list(column_map.values()) + [
        "age",
        "MedicalInsurance",
        "census_tract",
        "_provenance",

        # Test result columns
        'adeno',
        'chlamydia',
        'corona229e',
        'corona_hku1',
        'corona_nl63',
        'corona_oc43',
        'flu_a_h3',
        'flu_a_h1_2009',
        'flu_b',
        'flu_a',
        'flu_a_h1',
        'hmpv',
        'mycoplasma',
        'paraflu_1_4',
        'pertussis',
        'rhino_ent',
        'rsv',

        # ICD-10 codes
        'ICD10',
    ]

    clinical_records = clinical_records[clinical_records.columns.intersection(columns_to_keep)]

    # Convert dtypes
    # Incoming `encountered` value is typically just date but is cast to datetime with timezone in postgres. Timezone is
    # being specified here to ensure values are set to midnight local time instead of UTC.
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"]).dt.tz_localize('America/Los_Angeles')

    # Reformat vaccination dates. Values are immediately stripped of time component, so don't need timezone specified.
    if manifest_format in ['year3', 'year4', 'year5']:
        clinical_records["FluShotDate"] = pd.to_datetime(clinical_records["FluShotDate"]).dt.strftime('%Y-%m-%d')
        clinical_records["CovidShot1Date"] = pd.to_datetime(clinical_records["CovidShot1Date"]).dt.strftime('%Y-%m-%d')
        clinical_records["CovidShot2Date"] = pd.to_datetime(clinical_records["CovidShot2Date"]).dt.strftime('%Y-%m-%d')
    if manifest_format in ['year4', 'year5']:
        clinical_records["CovidShot3Date"] = pd.to_datetime(clinical_records["CovidShot3Date"]).dt.strftime('%Y-%m-%d')
        if 'CovidShot4Date' in clinical_records.columns:
            clinical_records["CovidShot4Date"] = pd.to_datetime(clinical_records["CovidShot4Date"]).dt.strftime('%Y-%m-%d')
        if 'CovidShot5Date' in clinical_records.columns:
            clinical_records["CovidShot5Date"] = pd.to_datetime(clinical_records["CovidShot5Date"]).dt.strftime('%Y-%m-%d')

    # Insert static value columns
    clinical_records["site"] = "SCH"

    create_encounter_identifier(clinical_records)
    remove_pii(clinical_records)

    dump_ndjson(clinical_records)

def add_icd10(df: pd.DataFrame) -> None:
    """
    Adds a new column for ICD-10 codes to a given *df*.
    """
    def icd10(series: pd.Series) -> pd.Series:
        """ Returns an array of unique ICD-10 codes from a given *series*. """
        icd10_columns = [col for col in df.columns if col.startswith('diag_cd')]
        icd10_codes = [ series[i] for i in icd10_columns if not pd.isna(series[i])]
        return list(set(icd10_codes))

    df['ICD10'] = df.apply(icd10, axis='columns')


def add_insurance(df: pd.DataFrame) -> None:
    """
    Adds a new column for insurance type to a given *df*.
    """
    def insurance(series: pd.Series) -> pd.Series:
        """ Returns an array of unique insurance types from a given *series*. """
        insurance_columns = ['insurance_1', 'insurance_2']
        insurances = [ series[i] for i in insurance_columns if not pd.isna(series[i])]
        return list(set(insurances))

    df['MedicalInsurance'] = df.apply(insurance, axis='columns')


def create_encounter_identifier(df: pd.DataFrame) -> None:
    """
    Creates an encounter identifier column on a given *df*.
    """
    df["identifier"] = (
        df["individual"] + df["encountered"].astype('string')
        ).str.lower()


@clinical.command("parse-kp")
@click.argument("kp_filename", metavar = "<KP Clinical Data filename>")
@click.argument("kp_specimen_manifest_filename",
    metavar = "<KP Specimen Manifest filename(s)>",
    nargs   = -1)
@click.option("--manifest-format",
    metavar="<manifest format>",
    default="year2",
    type=click.Choice(['year1','year2']),
    help="The format of input manifest file; default is \"year2\"")
@click.option("-o", "--output", metavar="<output filename>",
    help="The filename for the output of missing barcodes")

def parse_kp(kp_filename, kp_specimen_manifest_filename, manifest_format, output):
    """
    Process clinical data from KP.

    All clinical records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    clinical_records = pd.read_csv(kp_filename)
    clinical_records.columns = clinical_records.columns.str.lower()

    clinical_records = trim_whitespace(clinical_records)
    clinical_records = add_provenance(clinical_records, kp_filename)
    clinical_records = add_kp_manifest_data(clinical_records, kp_specimen_manifest_filename, manifest_format)

    clinical_records = convert_numeric_columns_to_binary(clinical_records)
    clinical_records = rename_symptoms_columns(clinical_records)
    clinical_records = collapse_columns(clinical_records, 'symptom')
    clinical_records = collapse_columns(clinical_records, 'race')

    clinical_records['FluShot'] = clinical_records['fluvaxdt'].notna()

    column_map = {
        "enrollid": "individual",
        "enrolldate": "encountered",
        "barcode": "barcode",
        "age": "age",
        "sex": "AssignedSex",
        "race": "Race",
        "hispanic": "HispanicLatino",
        "symptom": "Symptoms",
        "FluShot": "FluShot",
        "censustract": "census_tract",
        "_provenance": "_provenance",
    }

    if manifest_format=="year1":
        del column_map["censustract"]

    clinical_records = clinical_records.rename(columns=column_map)

    barcode_quality_control(clinical_records, output)

    # Drop unnecessary columns
    clinical_records = clinical_records[column_map.values()]

    # Convert dtypes
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"]).dt.tz_localize('America/Los_Angeles')

    # Insert static value columns
    clinical_records["site"] = "KP"

    create_encounter_identifier(clinical_records)
    remove_pii(clinical_records)

    # Placeholder columns for future data.
    # See https://seattle-flu-study.slack.com/archives/CCAA9RBFS/p1568156642033700?thread_ts=1568145908.029300&cid=CCAA9RBFS
    clinical_records["MedicalInsurace"] = None

    dump_ndjson(clinical_records)


def add_kp_manifest_data(df: pd.DataFrame, manifest_filenames: tuple, manifest_format: str) -> pd.DataFrame:
    """
    Join the specimen manifest data from the given *manifest_filenames* with the
    given clinical records DataFrame *df*
    """
    manifest_data = pd.DataFrame()

    if manifest_format=="year1":
        sheet_name = 'KP'
        rename_map = {
            'Barcode ID (Sample ID)': 'barcode',
            'kp_id': 'enrollid',
        }
    else:
        sheet_name = 'aliquoting'
        rename_map = {
            'sample_id': 'barcode',
            'kp_id': 'enrollid',
        }

    for filename in manifest_filenames:
        manifest = read_excel(filename, sheet_name = sheet_name)
        manifest_data = manifest_data.append(manifest)

    manifest_data.dropna(subset = ['kp_id'], inplace = True)

    regex = re.compile(r"^KP-([0-9]{6,})-[0-9]$", re.IGNORECASE)
    manifest_data.kp_id = manifest_data.kp_id.apply(lambda x: regex.sub('WA\\1', x))

    manifest_data = manifest_data.rename(columns=rename_map)
    manifest_data = trim_whitespace(manifest_data)

    return df.merge(manifest_data[['barcode', 'enrollid']], how='left')


@clinical.command("parse-phskc")
@click.argument("phskc_manifest_filename", metavar = "<PHSKC Clinical Manifest Data filename>",
            type = click.Path(exists=True, dir_okay=False))
@click.argument("file_pattern", metavar = "<PHSKC Clinical Data filename pattern>")
@click.argument("geocoding_cache_file", envvar = "GEOCODING_CACHE", metavar = "<Geocoding cache filename>",
            type = click.Path(dir_okay=False, writable=True))

def parse_phskc(phskc_manifest_filename: str, file_pattern: str, geocoding_cache_file: str = None) -> None:
    """
    Process clinical data from PHSKC.

    Given a path to PHSKC clinical files that need to be parsed, a
    <PHSKC Clinical Manifest Data filename> and a <Geocoding Cache filename>
    selects specific columns of interest and parses them into a clinical
    manifest data file. If data had been previously parsed, compare the
    last modified timestamps of the file with the last parsed timestamps to decide
    if we need to parse this file again.

    Clinical records are parsed and transformed into suitable data for downstream
    CID matching. PII is not removed in this function. Parsed data will later be joined
    with barcode data present in the LIMS.

    All clinical records (both newly and previously parsed data) are output to stdout
    as newline-delimited JSON records. You will likely want to redirect stdout to a file.
    """
    parsed_clinical_records = pd.read_json(phskc_manifest_filename, orient='records', dtype={'inferred_symptomatic': 'string', 'census_tract': 'int64', 'age': 'int64'}, lines=True)
    if not parsed_clinical_records.empty:
        parsed_clinical_records.columns = parsed_clinical_records.columns.str.lower()

    for file in glob.glob(file_pattern):
        relative_filename = file.split('/')[-1]
        last_modified_time = os.path.getmtime(file)
        LOG.debug(f'Working on `{relative_filename}`. Last modified time was {last_modified_time}')

        # grab manifest records of previously parsed records from this file
        if not parsed_clinical_records.empty:
            manifest_records = parsed_clinical_records.loc[
                parsed_clinical_records._provenance.str['filename'] == relative_filename, :
            ]
        else:
            manifest_records = pd.DataFrame()

        if manifest_records.empty or (last_modified_time > manifest_records['last_parsed']).all():
            LOG.info(f'Parsing `{relative_filename}`, no previous parse or file was last modified more recently than previous parse')
            clinical_records = pd.read_excel(file, dtype={'inferred_symptomatic': 'str'})
            clinical_records.columns = clinical_records.columns.str.lower()
            clinical_records = trim_whitespace(clinical_records)
        else:
            LOG.debug(f'Skipped parsing of `{relative_filename}`, file has not been modified since last parse')
            continue

        if clinical_records.empty and not manifest_records.empty:
            LOG.warning(
                f"A previously parsed PHSKC file is now empty: `{relative_filename}`. These records must be removed from the manifest manually.")
            continue
        elif clinical_records.empty:
            LOG.debug(f'Skipped parsing of `{relative_filename}`, file was empty')
            continue

        clinical_records = add_provenance(clinical_records, relative_filename)
        clinical_records = format_phskc_data(clinical_records, geocoding_cache_file)

        # if we don't have any manifest data at all, make these records the new manifest data.
        # if we don't have any manifest data for this file, add parsed data to the dataframe
        # if we do have manifest data for this file, drop the old manifest data before appending new ones
        if manifest_records.empty and parsed_clinical_records.empty:
            parsed_clinical_records = clinical_records
        elif manifest_records.empty:
            parsed_clinical_records = pd.concat([parsed_clinical_records, clinical_records]).reset_index(drop=True)
        else:
            parsed_clinical_records = parsed_clinical_records.drop(index=manifest_records.index)
            parsed_clinical_records = pd.concat([parsed_clinical_records, clinical_records]).reset_index(drop=True)

        LOG.info(f"Dropped {len(manifest_records)} and saved {len(clinical_records)} new manifest records")

    LOG.info(f"Dumping {len(parsed_clinical_records)} parsed PHSKC records to stdout")
    dump_ndjson(parsed_clinical_records)


@clinical.command("deduplicate-phskc")
@click.argument("phskc_manifest_filename", metavar = "<PHSKC Clinical Manifest Data filename>",
            type = click.Path(exists=True, dir_okay=False))
def deduplicate_phskc(phskc_manifest_filename: str) -> None:
    """
    Deduplicate parsed clinical data from PHSKC

    Given a <PHSKC Clinical Manifest Data filename> of manifest data with
    potentially duplicated records, output a deduplicated version of this
    manifest file

    PII is not removed by this function.
    """
    parsed_clinical_records = pd.read_json(phskc_manifest_filename, orient='records', dtype={'inferred_symptomatic': 'string', 'census_tract': 'int64', 'age': 'int64'}, lines=True)
    if parsed_clinical_records.empty:
        return
    else:
        parsed_clinical_records.columns = parsed_clinical_records.columns.str.lower()

    LOG.info(f"Read {len(parsed_clinical_records)} parsed PHSKC records from manifest file")

    # remove final full duplicates
    full_duplicates = parsed_clinical_records.duplicated(subset=parsed_clinical_records.columns.difference(["_provenance", "last_parsed"]), keep='last')
    fully_duplicated_records = parsed_clinical_records.loc[full_duplicates, :]
    parsed_clinical_records = parsed_clinical_records.loc[~full_duplicates, :]
    LOG.debug(f"Dropped {len(fully_duplicated_records)} fully duplicated records. {len(parsed_clinical_records)} remain.")

    # remove all identifier duplicates
    id_duplicates = parsed_clinical_records.duplicated(subset=PHSKC_IDENTIFIERS.keys(), keep=False)
    id_duplicated_records = parsed_clinical_records.loc[id_duplicates, :]
    parsed_clinical_records = parsed_clinical_records.loc[~id_duplicates, :]
    LOG.debug(f"Dropped {len(id_duplicated_records)} records with duplicated identifiers. {len(parsed_clinical_records)} remain.")

    # remove all single identifier duplicates
    for identifier in PHSKC_IDENTIFIERS.keys():
        single_duplicates = parsed_clinical_records.duplicated(subset=identifier, keep=False)
        single_duplicated_records = parsed_clinical_records.loc[single_duplicates, :]
        parsed_clinical_records = parsed_clinical_records.loc[~single_duplicates, :]
        LOG.debug(f"Dropped {len(single_duplicated_records)} records with a duplicated {identifier} column. {len(parsed_clinical_records)} remain.")

    LOG.info(f"A total of {len(parsed_clinical_records)} parsed PHSKC records exist after deduplication")
    dump_ndjson(parsed_clinical_records)


def format_phskc_data(clinical_records: pd.DataFrame, geocoding_cache_file: str) -> pd.DataFrame:
    """
    Formats a DataFrame with PHSKC clinical data in a manner
    suitable to compare with existing PHSKC manifest data.
    """
    clinical_records['site'] = 'PHSKC'
    clinical_records['patient_class'] = 'field'
    clinical_records['encounter_status'] = 'finished'

    # generate encounter and individual identifiers for each record
    clinical_records['individual'] = clinical_records.apply(
        lambda row: generate_patient_hash(
            row['pat_name'].split(',')[::-1],
            map_sex(row['sex']),
            str(row['birth_date']),
            str(row["pat_address_zip"])
        ), axis=1
    )

    clinical_records['identifier'] = clinical_records.apply(
        lambda row: generate_hash(
            f"{row['individual']}{row['collect_ts']}".lower()
        ), axis=1
    )

    # localize encounter timestamps to pacific time
    clinical_records['encountered'] = clinical_records['collect_ts'].dt.tz_localize('America/Los_Angeles')

    # calculate age based on sample collection date and birth day. Localize birth date datetime value to ensure accurate
    # delta with local collection datetime.
    clinical_records['birth_date'] = pd.to_datetime(clinical_records['birth_date']).dt.tz_localize('America/Los_Angeles')
    clinical_records['age'] = clinical_records.apply(
        lambda row: age_ceiling(
                relativedelta(
                    row['encountered'],
                    row['birth_date']
                ).years
            ), axis=1
    )

    # fill address NA values with empty strings to prevent geocoding failure, geocode addresses,
    # then hash them and get the census tract to remove PII from downstream processes
    clinical_records.fillna(
        {
            'pat_address_line1': '',
            'pat_address_line2': '',
            'pat_address_city': '',
            'pat_address_state': '',
            'pat_address_zip': '',
        }, inplace=True
    )

    with pickled_cache(geocoding_cache_file) as cache:
        clinical_records['lat'], clinical_records['lng'], clinical_records['canonical_address'] = zip(
            *clinical_records.apply(
                lambda row: get_geocoded_address(
                    {
                        'street': row['pat_address_line1'],
                        'secondary': row['pat_address_line2'],
                        'city': row['pat_address_city'],
                        'state': row['pat_address_state'],
                        'zipcode': row['pat_address_zip']
                    },
                    cache
                ),
                axis=1
            )
        )

    db = DatabaseSession()
    clinical_records = clinical_records.apply(
       lambda row: encode_addresses(db, row), axis=1
    )

    column_map = {
        'ethnic_group': 'ethnicity',
        'barcode': 'phskc_barcode',
        'collect_ts': 'collection_date'
    }

    columns_to_keep = list(column_map.keys()) + [
        '_provenance',
        'individual',
        'identifier',
        'site',
        'sex',
        'age',
        'race',
        'encountered',
        'address_hash',
        'census_tract',
        'main_cid',
        'all_cids',
        #'reason_for_visit',
        'survey_testing_because_exposed',
        'if_symptoms_how_long',
        'survey_have_symptoms_now',
        'inferred_symptomatic',
        'vaccine_status',
        'patient_class',
        'encounter_status',
        'result_value'
    ]

    clinical_records = clinical_records[columns_to_keep]
    clinical_records = clinical_records.rename(columns=column_map)

    # some phskc records have this non-breaking space value, which is easier to deal
    # with later if we convert all occurences to NA
    clinical_records = clinical_records.replace(to_replace="\u00a0", value=pd.NA)

    # phskc data is sent with some rows duplicated, so before we add manifest data
    # we should drop these copied rows, keeping the first one
    clinical_records.drop_duplicates(subset=clinical_records.columns.difference(['_provenance']), inplace=True)
    clinical_records['last_parsed'] = int(time.time())

    return clinical_records


def encode_addresses(db: DatabaseSession, row: pd.Series) -> pd.Series:
    """
    Given a series with latitude and longitute data, plus a canonical
    address, encodes that data into census tract information and hashes
    the address.
    """
    location = location_lookup(db, (row.get('lat'), row.get('lng')), 'tract')

    if location:
        row['census_tract'] = location.identifier
    else:
        row['census_tract'] = None

    if row['canonical_address']:
        row['address_hash'] = generate_hash(row['canonical_address'])
    else:
        row['address_hash'] = None

    return row


def convert_numeric_columns_to_binary(df: pd.DataFrame) -> pd.DataFrame:
    """
    In a given DataFrame *df* of clinical records, convert a hard-coded list of
    columns from numeric coding to binary.

    See Kaiser Permanente data dictionary for details
    """
    numeric_columns = [
        'runnynose',
        'hispanic',
        'racewhite',
        'raceblack',
        'raceasian',
        'raceamerind',
        'racenativehi',
    ]
    for col in numeric_columns:
        df.loc[df[col] > 1, col] = None

    return df


def rename_symptoms_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ Renames the hard-coded symptoms columns in a given DataFrame *df* """
    symptoms_columns = [
        'fever',
        'sorethroat',
        'runnynose',
        'cough'
    ]

    symptoms_map = {}
    for symptom in symptoms_columns:
        symptoms_map[symptom] = 'symptom' + symptom

    return df.rename(columns=symptoms_map)


def collapse_columns(df: pd.DataFrame, stub: str, pid='enrollid') -> pd.DataFrame:
    """
    Given a pandas DataFrame *df* of clinical records, collapses the 0/1
    encoding of multiple race options into a single array in a resulting
    column called "Race". Removes the original "Race*" option columns. Returns
    the new DataFrame.

    >>> df = pd.DataFrame(columns = ['enrollid', 'racewhite', 'raceblack', 'raceasian', 'census_tract'])
    >>> df.loc[0] = ['WA000000', 1, 1, 1, '00000000000']
    >>> collapse_columns(df, 'race').columns.values.tolist()
    ['enrollid', 'census_tract', 'race']

    """
    stub_data = df.filter(regex=f'^({pid}|{stub}.*)$', axis='columns')
    stub_columns = list(stub_data)
    stub_columns.remove(pid)

    df = df.drop(columns=stub_columns)

    stub_data_long = pd.wide_to_long(stub_data, stub, i=pid, j=f"new_{stub}",
                        suffix='\\w+').reset_index()

    stub_data_new = group_true_values_into_list(stub_data_long, stub, [pid])

    return df.merge(stub_data_new, how='left')


@clinical.command("upload")
@click.argument("clinical_file",
    metavar = "<clinical.ndjson>",
    type = click.File("r"))

def upload(clinical_file):
    """
    Upload clinical records into the database receiving area.

    <clinical.ndjson> must be a newline-delimited JSON file produced by this
    command's sibling commands.

    Once records are uploaded, the clinical ETL routine will reconcile the
    clinical records with known sites, individuals, encounters and samples.
    """
    db = DatabaseSession()

    try:
        LOG.info(f"Copying clinical records from {clinical_file.name}")

        row_count = db.copy_from_ndjson(("receiving", "clinical", "document"), clinical_file)

        LOG.info(f"Received {row_count:,} clinical records")
        LOG.info("Committing all changes")
        db.commit()

    except:
        LOG.info("Rolling back all changes; the database will not be modified")
        db.rollback()
        raise


def prepare_lims_request(
        verb: str,
        path: str,
        body: List[dict] = None,
        lims_server: Optional[str] = None,
        lims_key: Optional[str] = None,
        lims_secret: Optional[str] = None
    ) -> requests.PreparedRequest:
    """
    Prepares a LIMS API request so that it is ready to be sent. Creates the
    request object and signs it using HMAC. Requires the environment values
    `LIMS_API_KEY_ID` and `LIMS_API_KEY_SECRET` if these are not passed directly.
    """
    if lims_server is None and 'LIMS_API_URL' in os.environ:
        lims_server = os.environ['LIMS_API_URL']
    else:
        raise ValueError('`LIMS_API_URL` was not found in the environment, and not provided to LIMS auth builder.')

    if lims_key is None and 'LIMS_API_KEY_ID' in os.environ:
        lims_key = os.environ['LIMS_API_KEY_ID']
    else:
        raise ValueError('`LIMS_API_KEY_ID` was not found in the environment, and not provided to LIMS auth builder.')

    if lims_secret is None and 'LIMS_API_KEY_SECRET' in os.environ:
        lims_secret = os.environ['LIMS_API_KEY_SECRET']
    else:
        raise ValueError('`LIMS_API_KEY_SECRET` was not found in the environment, and not provided to LIMS auth builder.')

    # The LIMS server expects the nonce to be a UNIX timestamp. This value prevents replay attacks to the LIMS.
    unix_timestamp_seconds_utc = int(datetime.now(timezone.utc).replace(tzinfo=timezone.utc).timestamp() * 1000)
    nonce = str(unix_timestamp_seconds_utc)

    # The secret provided by the LIMS application is base64-encoded, with the final "==" stripped off.
    hmac_signer = hmac.new(base64.b64decode(lims_secret + '=='), digestmod=hashlib.sha512)

    request = requests.Request(
        verb,
        f'{lims_server}{path}',
        data=json.dumps(body),
        headers={
            'hmac-key-id': lims_key,
            'content-type': 'application/json'
        }
    )
    prepared_request = request.prepare()

    # Append the nonce, HTTP verb, API path, and a hash digest of the body (if it exists) to the data to be HMAC-signed.
    hmac_signer.update(nonce.encode() + verb.encode() + path.encode())
    if body is not None:
        body_hash = hashlib.md5(str(prepared_request.body).encode()).hexdigest().encode()
        hmac_signer.update(body_hash)

    # Generate the signature and add it to the authorization header.
    signature = hmac_signer.hexdigest()
    prepared_request.headers['Authorization'] = f'HMAC {nonce}:{signature}'

    return prepared_request


def match_lims_identifiers(clinical_records: pd.DataFrame, lims_identifiers: Dict[str, str]) -> Dict[str, str]:
    """
    Fetch internal SFS sample identifiers from the LIMS and match them
    on the desired identifier value.
    """
    lims_search_results = []
    session = requests.Session()
    for clinical_term, lims_term in lims_identifiers.items():
        LOG.debug(f"Trying to match clinical term `{clinical_term}` with lims term `{lims_term}`")

        clinical_ids = clinical_records[clinical_term].tolist()
        lims_query_terms = [{lims_term: identifier} for identifier in clinical_ids if identifier]
        if len(lims_query_terms) > 0:
            LOG.debug(f"Fetching matches for {len(lims_query_terms)} {lims_term} identifiers from the LIMS")

            # perform requests with exponential backoff and batching, since
            # these can be a bit intense for the LIMS server if there are lots
            # of records to search on.
            lims_request = prepare_lims_request(
                'POST',
                '/api/v1/sfs-specimens/find-specimen-identifiers',
                lims_query_terms
            )
            response = session.send(lims_request)
            LOG.info(f"{response.status_code} {response.reason} response for content=specimen-identifiers from {response.url}")
            response.raise_for_status()

            # grab the identifiers section from each valid LIMS specimen result
            if response.content is not None:
                search_results = [
                    result.get('ids', None) for result in json.loads(response.content) if result is not None and 'error' not in result
                ]

        else:
            LOG.debug(f"Skipping LIMS query with {len(lims_query_terms)} identifiers to search for")
            search_results = []

        LOG.debug(f"Received {len(search_results)} `{clinical_term}` identifiers matching lims identifier `{lims_term}`")
        lims_search_results.extend(search_results)

    # if a term that we are searching for exists within our search results,
    # add it to our matched identifiers dictionary and associate it with the
    # sample id of the queried record.r
    matched_identifiers = {}
    for identifiers in lims_search_results:
        for term in set(lims_identifiers.values()):
            if identifiers and term in identifiers:
                matched_identifiers[identifiers[term]] = identifiers['matrixId']

    return matched_identifiers
