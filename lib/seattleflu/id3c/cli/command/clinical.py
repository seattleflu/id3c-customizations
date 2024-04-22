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
import numpy as np
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
KP2023_IDENTIFIERS = {
    'collection_id': 'kaiserPermanenteSpecimenId'
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
    clinical_records = drop_missing_rows(clinical_records, 'barcode')

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

def remove_pii(df: pd.DataFrame) -> None:
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
    return df

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
    clinical_records = drop_missing_rows(clinical_records, 'encountered')

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
    clinical_records = map_icd10_codes(clinical_records, 'kp')

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
        "icd10": "ICD10"
    }

    if manifest_format=="year1":
        del column_map["censustract"]

    clinical_records = clinical_records.rename(columns=column_map)

    barcode_quality_control(clinical_records, output)

    # Drop unnecessary columns
    clinical_records = clinical_records[column_map.values()]

    # Convert dtypes
    #clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"]).dt.tz_localize('America/Los_Angeles')
    # unlike other clinical parse functions, do not convert from UTC to local timezone
    # this is because of a reingestion of kp 2018-2021 encounter metadata in 2024, in order to include ICD-10 codes
    # timestamp conversion from UTC to local timezone only was added after kp 2018-2021 encounters were processed into id3c
    # encounter identifiers are based on encounter date, so need to keep encounter date consistent with old
    # records in order to avoid re-uploading the same encounter to id3c with a different encounter identifier than before
    
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"])

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
    parsed_clinical_records = pd.read_json(phskc_manifest_filename, orient='records', dtype={'inferred_symptomatic': 'string', 'census_tract': 'string', 'age': 'int64'}, lines=True)
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
@click.argument("phskc_manifest_skips_filename", metavar = "<PHSKC Clinical Manifest Data filename>",
            type = click.Path(exists=True, dir_okay=False))
def deduplicate_phskc(phskc_manifest_filename: str, phskc_manifest_skips_filename: str) -> None:
    """
    Deduplicate parsed clinical data from PHSKC

    Given a <PHSKC Clinical Manifest Data filename> of manifest data with
    potentially duplicated records, output a deduplicated version of this
    manifest file

    PII is not removed by this function.
    """
    parsed_clinical_records = pd.read_json(phskc_manifest_filename, orient='records', dtype={'inferred_symptomatic': 'string', 'census_tract': 'string', 'age': 'int64'}, lines=True)
    if parsed_clinical_records.empty:
        return
    else:
        parsed_clinical_records.columns = parsed_clinical_records.columns.str.lower()

    LOG.debug(f"Read {len(parsed_clinical_records)} parsed PHSKC records from manifest file")

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

    all_duplicates = [fully_duplicated_records, id_duplicated_records]
    # remove all single identifier duplicates. ensure we don't treat NAs as dups
    for identifier in PHSKC_IDENTIFIERS.keys():
        single_duplicates = parsed_clinical_records.duplicated(subset=identifier, keep=False)
        single_duplicates[parsed_clinical_records[identifier].isnull()] = False
        all_duplicates.append(parsed_clinical_records.loc[single_duplicates, :].copy())
        parsed_clinical_records = parsed_clinical_records.loc[~single_duplicates, :]
        LOG.debug(f"Dropped {len(all_duplicates[-1])} records with a duplicated {identifier} column. {len(parsed_clinical_records)} remain.")

    dropped_records = pd.concat(all_duplicates)
    dropped_records.to_json(phskc_manifest_skips_filename, orient='records', lines=True)
    LOG.info(f"Skipped a total of {len(dropped_records)} duplicated records")

    LOG.info(f"A total of {len(parsed_clinical_records)} parsed PHSKC records exist after deduplication")
    dump_ndjson(parsed_clinical_records)


@clinical.command("match-phskc")
@click.argument("phskc_manifest_new_filename", metavar = "<PHSKC Clinical Manifest Newly Parsed Data filename>",
                type = click.Path(exists= True, dir_okay=False))
@click.argument("phskc_manifest_unmatched_filename", metavar = "<PHSKC Clinical Manifest Un-Matched Data filename>",
                type = click.Path(exists=True, dir_okay=False))
@click.argument("phskc_manifest_matched_filename", metavar = "<PHSKC Clinical Manifest Matched Data filename>",
                type = click.Path(exists=True, dir_okay=False))

def match_phskc(phskc_manifest_new_filename: str, phskc_manifest_unmatched_filename: str, phskc_manifest_matched_filename: str) -> None:
    """
    Match clinical data from PHSKC with identifiers from the LIMS.

    Given a <PHSKC Clinical Manifest Un-Matched Data filename> of manifest
    data not yet matched to a LIMS identifiers, a <PHSKC Clinical Manifest
    Matched Data filename> of records already matched to LIMS data, and a
    <PHSKC Clinical Manifest Newly Parsed Data Filename> of data that was newly
    parsed and should be matched to LIMS data, attempt to match any unmatched
    barcodes or newly parsed data with LIMS data and add them to the match file.
    Remove any matches from the unmatched file.

    PII is removed by this function. The unmatched data input file may be modified
    by this command.

    All matched clinical records (both previously and newly matched records) are output
    to stdout as newline-delimited JSON records.  You will likely want to redirect stdout
    to a file.
    """
    new_clinical_records = pd.read_json(phskc_manifest_new_filename, orient='records', dtype={'inferred_symptomatic': 'string', 'census_tract': 'string', 'age': 'int64'}, lines=True)
    unmatched_clinical_records = pd.read_json(phskc_manifest_unmatched_filename, orient='records', dtype={'inferred_symptomatic': 'string', 'census_tract': 'string', 'age': 'int64'}, lines=True)
    matched_clinical_records = pd.read_json(phskc_manifest_matched_filename, orient='records', dtype={'inferred_symptomatic': 'string', 'census_tract': 'string', 'age': 'int64'}, lines=True)
    LOG.info(f"A total of {len(matched_clinical_records)} records are matched to LIMS data with {len(unmatched_clinical_records)} still unmatched.")

    # if a file appears in our diff, it means we just re-parsed it. therefore all records in our unmatched
    # dataset from that file will be stale. remove them here and replace them with freshly parsed records.
    LOG.info(f"Received {len(new_clinical_records)} newly parsed records")
    if not new_clinical_records.empty:
        for file in new_clinical_records._provenance.str['filename'].unique():
            LOG.debug(f"Rematching all records from newly parsed file {file}")

            # find any currently unmatched clinical records from this file in our
            # dataset and drop them.
            if not unmatched_clinical_records.empty:
                stale_records = unmatched_clinical_records.loc[
                    unmatched_clinical_records._provenance.str['filename'] == file, :
                ]
                unmatched_clinical_records = unmatched_clinical_records.drop(stale_records.index)
                LOG.debug(f"Dropped {len(stale_records)} stale unmatched records")

            # get all the freshly parsed records from this file and add them to the records we must match
            fresh_records = new_clinical_records.loc[
                new_clinical_records._provenance.str['filename'] == file, :
            ]
            unmatched_clinical_records = pd.concat([unmatched_clinical_records, fresh_records]).reset_index(drop=True)
            LOG.debug(f"Received {len(fresh_records)} fresh unmatched records")
    else:
        LOG.debug(f"Didn't receive any newly parsed records. Trying to match {len(unmatched_clinical_records)} previously unmatched data")

    LOG.info(f"Attempting to match {len(unmatched_clinical_records)} unmatched identifiers to LIMS data")
    identifier_pairs = match_lims_identifiers(unmatched_clinical_records, PHSKC_IDENTIFIERS)

    # try to find a barcode match for each possible identifier if we haven't already matched this row
    unmatched_clinical_records['barcode'] = pd.NA
    for identifier in PHSKC_IDENTIFIERS.keys():
        unmatched_clinical_records['barcode'] = unmatched_clinical_records.apply(
            lambda row: identifier_pairs.get(row[identifier], pd.NA) if pd.isna(row['barcode']) else row['barcode'],
            axis=1
        )

    newly_matched_clinical_records = unmatched_clinical_records.loc[~unmatched_clinical_records['barcode'].isna()]
    unmatched_clinical_records = unmatched_clinical_records[unmatched_clinical_records['barcode'].isna()]

    # drop any PII and parse metadata
    newly_matched_clinical_records = newly_matched_clinical_records.drop(
        columns=list(PHSKC_IDENTIFIERS.keys()) + ['last_parsed']
    )

    # newly matched barcodes shouldn't be in our previously matched data.
    # any barcodes that show up in a previous parse might contain refreshed data,
    # so we should keep the newly matched data and drop the old match. If there is
    # no difference, our diff won't pull this into ID3C.
    if newly_matched_clinical_records.empty:
        LOG.debug(f"No new records were matched to LIMS data")
    elif matched_clinical_records.empty:
        LOG.debug(f"{len(newly_matched_clinical_records)} were matched. No previously matched data to consolidate")
    else:
        LOG.debug(f"{len(newly_matched_clinical_records)} were matched. Refreshing all previously matched data")

        refreshed_old_records = matched_clinical_records.barcode.isin(newly_matched_clinical_records.barcode)
        refreshed_new_records = newly_matched_clinical_records.barcode.isin(matched_clinical_records.barcode)
        matched_clinical_records = matched_clinical_records.loc[~refreshed_old_records, :]
        LOG.debug(f"{len(matched_clinical_records)} previously matched records remain after dropping potentially refreshed records")

        newly_refreshed_clinical_records = newly_matched_clinical_records.loc[refreshed_new_records, :]
        newly_paired_clinical_records = newly_matched_clinical_records.loc[~refreshed_new_records, :]
        LOG.debug(f"{len(newly_paired_clinical_records)} had not been previously matched. {len(newly_refreshed_clinical_records)} had been previously matched and were refreshed")

        newly_matched_clinical_records = pd.concat([newly_paired_clinical_records, newly_refreshed_clinical_records]).reset_index(drop=True)

    matched_clinical_records = pd.concat([matched_clinical_records, newly_matched_clinical_records]).reset_index(drop=True)
    LOG.info(f"A total of {len(matched_clinical_records)} records are matched to LIMS data with {len(unmatched_clinical_records)} still unmatched.")

    unmatched_clinical_records.to_json(phskc_manifest_unmatched_filename, orient='records', lines=True)
    if not matched_clinical_records.empty:
        dump_ndjson(matched_clinical_records)


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


@clinical.command("parse-kp2023")
@click.argument("kp2023_filename", metavar = "<Path to kp2023 clinical data file>",
            type = click.Path(exists=True, dir_okay=False))

def parse_kp2023(kp2023_filename: str) -> None:
    """
    Process clinical data from kp2023.

    Given a path to a kp2023 clinical file and a geocoding cache filename, selects specific
    columns of interest and parses them into a clinical manifest data file. If data
    has been previously parsed, compare the last modified timestamps of the clinical file
    with the last parsed timestamps of the manifest to decide if we need to parse clinical file again.
    (Note that if a manifest already exists, it must have the same root name as the clinical data file
    in order to be recognized as previously parsed data.)

    Clinical records are parsed and transformed into suitable data for downstream
    CID matching. PII is not removed in this function because parsed data will later be joined
    with barcode data present in the LIMS.

    All clinical records (both newly and previously parsed data) are output to stdout
    as newline-delimited JSON records. You will likely want to redirect stdout to a file.
    """

    clinical_records = pd.read_csv(kp2023_filename)
    clinical_records.columns = clinical_records.columns.str.lower()
    clinical_records = trim_whitespace(clinical_records)
    clinical_records = add_provenance(clinical_records, os.path.basename(kp2023_filename)) 
    # since kp may provide updates as new files, and to avoid processing outdated records in id3c, add spreadsheet timestamp column
    last_modified_time = os.path.getmtime(kp2023_filename)
    clinical_records['spreadsheet_timestamp'] = last_modified_time
    clinical_records['spreadsheet_timestamp'] = pd.to_datetime(clinical_records['spreadsheet_timestamp'], unit='s')

    # check that all expected columns are present (even if empty)
    # do this check before standardizing names, but allow fixes for known typos through
    expected_columns = [
        'marshfield_lab_id',
        'hispaniclatino',
        'assigned_sex',
        'censustract',
        'type_of_visit',
        'age',
        'encountered',
        'race_ai_an',
        'race_asian',
        'race_black_aa',
        'race_nh_opi',
        'race_white',
        'race_unknown',
        'symptom_cough',
        'symptom_fever',
        'symptom_chills',
        'symptom_throat', # typo that appeared in early KP2023 metadata will be let through below
        'symptom_sob', # typo that appeared in early KP2023 metadata will be let through below
        'symptom_nose',
        'symptom_smell_taste',
        #'symptom_unk', # no longer in data dictionary as of January 2024
        #'symptom_no_answer', # no longer in data dictionary as of January 2024
        'date_flu_1',
        'date_flu_2',
        'flu_type_1',
        'flu_type_2',
        'date_covid_1',
        'date_covid_2',
        'date_covid_3',
        'date_covid_4',
        'date_covid_5',
        'date_covid_6',
        'date_symptom_onset',
    ]

    # check for missing expected columns
    missing_cols = list(set(expected_columns).difference(clinical_records.columns))
    # Allow typos from early KP2023 metadata sheets through
    if 'symptom_sob' in missing_cols and 'sympton_sob' in clinical_records.columns:
        missing_cols.remove('symptom_sob')
    if 'symptom_throat' in missing_cols and 'symptom__throat' in clinical_records.columns:
        missing_cols.remove('symptom_throat')
    if len(missing_cols) > 0:
        raise MissingColumn(f'One or more expected columns are missing from the input spreadsheet: {*missing_cols,}')

    # rename columns
    column_map = {
        'marshfield_lab_id':    'collection_id', # will be mapped to lims barcode with id3c clinical match-kp2023
        'hispaniclatino':       'ethnicity',
        'assigned_sex':         'sex',
        'symptom__throat':      'symptom_throat', # fix typo if present
        'censustract':          'census_tract',
        'type_of_visit':        'patient_class',
        'sympton_sob':          'symptom_sob' # fix typo if present
    }

    clinical_records = clinical_records.rename(columns=column_map)

    # The collection ids on the tubes from KP have aliquot numbers appended to them (ex: KPWB100001C-1)
    # but the collection ids in the metadata spreadsheet do not have these aliquot numbers at the end (ex: KPWB100001C)
    # therefore, we will check that there is no aliquot number at the end of the collection id in the metadata spreadsheet,
    # and strip the aliquot number if it is there, with a warning since we don't expect it
    # in the LIMS, this id is called KaiserPermanenteSpecimenId and will also have its aliquot number stripped
    collection_ids_with_aliquot = clinical_records['collection_id'].apply(lambda x: True if re.search(r'-\d+$', x) else False)
    # if there are any collection ids with aliquot (not expected), give a warning and strip the aliquot number
    if any(collection_ids_with_aliquot):
        LOG.warning('Warning: One or more Marshfield lab IDs contain aliquot id, which is unexpected. Stripping aliquot id ' +
            f'from these samples: {*clinical_records.loc[collection_ids_with_aliquot]["collection_id"].values,}')
        clinical_records.loc[collection_ids_with_aliquot, 'collection_id'] = clinical_records.loc[
            collection_ids_with_aliquot, 'collection_id'
        ].apply(lambda cid: re.sub(r'-\d+$','', cid))

    # convert symptom columns from numeric to binary (0/1)
    clinical_records = convert_column_set_to_binary(clinical_records, 'symptom_')
        
    # check that expected binary columns only contain 0/1/None values
    # race
    if not column_set_is_binary(clinical_records, 'race_'):
        raise UnexpectedNumeric(f'One or more columns with prefix "race_" have values other than 0/1/None.\
                                  These columns are expected to be binary.')
    # sex column is binary, but the map function that we use below
    # will automatically convert non-0/1 values to None,
    # so don't need to check that here
    # likewise with patient_class
    
    # map high risk codes to ICD-10 codes, and collapse into one column 'icd10'
    clinical_records = map_icd10_codes(clinical_records, 'kp2023')

    # collapse race and symptom columns
    clinical_records = collapse_columns(clinical_records, 'symptom_', 'collection_id')
    clinical_records = collapse_columns(clinical_records, 'race_', 'collection_id')

    # rename collapsed race and symptom columns
    clinical_records = clinical_records.rename(columns={'symptom_': 'symptom', 'race_': 'race'})

    # map flu vaccines
    map_flu_vaccine_kp2023(clinical_records, 'flu_type_1')
    map_flu_vaccine_kp2023(clinical_records, 'flu_type_2')

    # map ethnicity
    map_ethnicity_kp2023(clinical_records, 'ethnicity')

    # map sex
    clinical_records['sex'] = clinical_records['sex'].map({0: 'female', 1: 'male'})

    # insert site
    # ideally site would be 'KP', but currently KP2023's ETL is set up to be processed with FHIR
    # during id3c etl clinical, while the KP ETL is not processed with FHIR.
    # therefore, the site needs to be different so that it can be used by id3c etl clinical to decide whether to use FHIR or not
    # could maybe use provenance instead of site to make that distinction, but keeping this for now
    clinical_records['site'] = 'KP2023'

    # map patient class, expect all to be 1 (outpatient)
    # records with a value other than 1 will be carried forward with a warning and value will be changed to NaN
    unexpected_patient_class = clinical_records[clinical_records['patient_class'] != 1]
    if not unexpected_patient_class.empty:
        for record in unexpected_patient_class:
            LOG.warning(f"Record {record['individual']} has value {record['patient_class']} in type_of_visit column, expected 1. Proceeding with record anyway.")
    clinical_records['patient_class'] = clinical_records['patient_class'].map({1: 'outpatient'})

    # apply age ceiling, hash individual id
    clinical_records['age'] = clinical_records['age'].apply(age_ceiling)

    # create hashed encounter id
    # although KP provides an 'individual' column, we ignore it because there is exactly 1 individual value per collection id (Marshfield lab ID)
    # so we ignore the individual column and instead treat collection id as an individual id
    # therefore, we use the collection id and the encounter date to create a hashed encounter id
    clinical_records['identifier'] = clinical_records.apply(
        lambda row: generate_hash(
            f"{row['collection_id']}{row['encountered']}".lower()
        ), axis=1
    )

    # drop records with missing values: barcode/collectionid (not sure which variable this maps to yet), encounter date
    # also require sex, which is necessary for creating patient resource
    clinical_records = drop_missing_rows(clinical_records, 'encountered')
    clinical_records = drop_missing_rows(clinical_records, 'collection_id')
    clinical_records = drop_missing_rows(clinical_records, 'sex')

    # Incoming `encountered` value is typically just date but is cast to datetime with timezone in postgres. Timezone is
    # being specified here to ensure values are set to midnight local time instead of UTC.
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"]).dt.tz_localize('America/Los_Angeles')

    # convert other dates to datetime format; no need to localize because id3c will not insert time component for these dates
    # get list of date columns besides encountered
    date_cols = [col for col in clinical_records.columns if col.startswith('date')]
    for col in date_cols:
        clinical_records[col] = pd.to_datetime(clinical_records[col]).dt.strftime('%Y-%m-%d')

    # convert census_tract to string
    # do this here rather than upon import with dtype, because the latter would require assuming capitalization of column name from KP
    clinical_records['census_tract'] = clinical_records['census_tract'].astype('Int64').astype('str')

    # ensure there are no unintended columns being kept
    columns_to_keep = [
        '_provenance',
        'identifier',
        'site',
        'sex',
        'age',
        'race',
        'ethnicity',
        'encountered',
        'census_tract',
        'collection_id',
        'symptom',
        'icd10',
        'date_flu_1',
        'date_flu_2',
        'flu_type_1',
        'flu_type_2',
        'date_covid_1',
        'date_covid_2',
        'date_covid_3',
        'date_covid_4',
        'date_covid_5',
        'date_covid_6',
        'date_symptom_onset',
        'patient_class',
        'spreadsheet_timestamp'
    ]

    # throw a warning if there are any columns not in columns_to_keep
    # except for known extra columns, like 'individual' which might not be included anymore but has been included in the past
    extra_cols = list(set(clinical_records.columns).difference(columns_to_keep))
    if 'individual' in extra_cols:
        extra_cols.remove('individual')
    if len(extra_cols) > 0:
        LOG.warning(f'Warning: One or more unexpected columns present after parsing: {*extra_cols,}.\n\
        Removing these columns before dumping records to stdout.')

    clinical_records = clinical_records[columns_to_keep]

    # dump ndjson to stdout
    LOG.info(f"Dumping {len(clinical_records)} parsed KP2023 records to stdout")
    dump_ndjson(clinical_records)


def convert_column_set_to_binary(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Given a DataFrame *df* of clinical records and a string *prefix* with
    a prefix denoting which columns to convert, returns a DataFrame where
    columns whose names begin with *prefix* contain only values 0, 1, or None.
    Any value other than 1 in the input column is converted to None.
    See KP2023 data dictionary for details.
    """
    cols = [c for c in df.columns if c.startswith(prefix)]
    for c in cols:
        df.loc[df[c] > 1, c] = None

    return df


def column_set_is_binary(df: pd.DataFrame, prefix:str) -> bool:
    """
    Given a DataFrame *df* of clinical records and a string *prefix* with
    a prefix denoting columns of interest, returns True if all columns
    beginning with the provided prefix contain only 0/1/None values,
    otherwise returns False.
    """
    cols = [c for c in df.columns if c.startswith(prefix)]
    return all([np.isin(df[c].dropna().unique(), [0, 1]).all() for c in cols])


def map_icd10_codes(df: pd.DataFrame, sample_stream: str) -> pd.DataFrame:
    """
    Given a DataFrame *df* of clinical records, returns a DataFrame
    with an icd10 column containing a list of all positive icd10 codes for each record
    """
    if sample_stream == 'kp':
        icd10_mapper = {
            "cvd":                                                                                                  "I25.10",       
            "chf":                                                                                                  "I50.9",
            "bronch":                                                                                               "J42", # could map to J41 or J42
            "copd":                                                                                                 "J44.9",
            "asthma":                                                                                               "J45",
            "diabetes":                                                                                             "E11.9",
            "renal":                                                                                                "E18.9",
            "chemo":                                                                                                "Z51.1",
            "solidorgan":                                                                                           "Z94",
            "hsct":                                                                                                 "Z94.84",
            "liver":                                                                                                "B18", # this could map to B18 or 70.9
            "cancer":                                                                                               "C", # not sure what to do for original kp sample stream which is not more specific about cancer type
            "lungmalig":                                                                                            "C34"
        }

    elif sample_stream == 'kp2023':
        icd10_mapper = {
            "chronic ischemic heart disease":                                                                       "I25",
            "heart failure":                                                                                        "I50",
            "simple and mucopurulent chronic bronchitis":                                                           "J41",
            "unspecified chronic bronchitis":                                                                       "J42",
            "other chronic obstructive pulmonary disease":                                                          "J44",
            "asthma":                                                                                               "J45",
            "bronchiectasis":                                                                                       "J47",
            "acute respiratory distress syndrome":                                                                  "J80",
            "type 2 diabetes mellitus":                                                                             "E11",
            "encounter for antineoplastic chemotherapy and immunotherapy":                                          "Z51.1",
            "transplanted organ and tissue status":                                                                 "Z94",
            "chronic viral hepatitis":                                                                              "B18",
            "alcoholic liver disease":                                                                              "K70",
            "malignant neoplasm of lip":                                                                            "C00",
            "malignant neoplasm of base of tongue":                                                                 "C01",
            "malignant neoplasm of other and unspecified parts of tongue":                                          "C02",
            "malignant neoplasm of gum":                                                                            "C03",
            "malignant neoplasm of floor of mouth":                                                                 "C04",
            "malignant neoplasm of palate":                                                                         "C05",
            "malignant neoplasm of other and unspecified parts of mouth":                                           "C06",
            "malignant neoplasm of parotid gland":                                                                  "C07",
            "malignant neoplasm of other and unspecified major salivary glands":                                    "C08",
            "malignant neoplasm of tonsil":                                                                         "C09",
            "malignant neoplasm of oropharynx":                                                                     "C10",
            "malignant neoplasm of nasopharynx":                                                                    "C11",
            "malignant neoplasm of pyriform sinus":                                                                 "C12",
            "malignant neoplasm of hypopharynx":                                                                    "C13",
            "malignant neoplasm of other and ill-defined sites in the lip, oral cavity and pharynx":                "C14",
            "malignant neoplasm of esophagus":                                                                      "C15",
            "malignant neoplasm of stomach":                                                                        "C16",
            "malignant neoplasm of small intestine":                                                                "C17",
            "malignant neoplasm of colon":                                                                          "C18",
            "malignant neoplasm of rectosigmoid junction":                                                          "C19",
            "malignant neoplasm of rectum":                                                                         "C20",
            "malignant neoplasm of anus and anal canal":                                                            "C21",
            "malignant neoplasm of liver and intrahepatic bile ducts":                                              "C22",
            "malignant neoplasm of gallbladder":                                                                    "C23",
            "malignant neoplasm of other and unspecified parts of biliary tract":                                   "C24",
            "malignant neoplasm of pancreas":                                                                       "C25",
            "malignant neoplasm of other and ill-defined digestive organs":                                         "C26",
            "malignant neoplasm of nasal cavity and middle ear":                                                    "C30",
            "malignant neoplasm of accessory sinuses":                                                              "C31",
            "malignant neoplasm of larynx":                                                                         "C32",
            "malignant neoplasm of trachea":                                                                        "C33",
            "malignant neoplasm of bronchus and lung":                                                              "C34",
            "malignant neoplasm of thymus":                                                                         "C37",
            "malignant neoplasm of heart, mediastinum and pleura":                                                  "C38",
            "malignant neoplasm of other and ill-defined sites in the respiratory system and intrathoracic organs": "C39",
            "malignant neoplasm of bone and articular cartilage of limbs":                                          "C40",
            "malignant neoplasm of bone and articular cartilage of other and unspecified sites":                    "C41",
            "malignant melanoma of skin":                                                                           "C43",
            "other and unspecified malignant neoplasm of skin":                                                     "C44",
            "mesothelioma":                                                                                         "C45",
            "kaposi's sarcoma":                                                                                     "C46",
            "malignant neoplasm of peripheral nerves and autonomic nervous system":                                 "C47",
            "malignant neoplasm of retroperitoneum and peritoneum":                                                 "C48",
            "malignant neoplasm of other connective and soft tissue":                                               "C49",
            "merkel cell carcinoma":                                                                                "C4A",
            "malignant neoplasms of breast":                                                                        "C50",
            "malignant neoplasm of vulva":                                                                          "C51",
            "malignant neoplasm of vagina":                                                                         "C52",
            "malignant neoplasm of cervix uteri":                                                                   "C53",
            "malignant neoplasm of corpus uteri":                                                                   "C54",
            "malignant neoplasm of uterus, part unspecified":                                                       "C55",
            "malignant neoplasm of ovary":                                                                          "C56",
            "malignant neoplasm of other and unspecified female genital organs":                                    "C57",
            "malignant neoplasm of placenta":                                                                       "C58",
            "malignant neoplasm of penis":                                                                          "C60",
            "malignant neoplasm of prostate":                                                                       "C61",
            "malignant neoplasm of testis":                                                                         "C62",
            "malignant neoplasm of other and unspecified male genital organs":                                      "C63",
            "malignant neoplasm of kidney, except renal pelvis":                                                    "C64",
            "malignant neoplasm of renal pelvis":                                                                   "C65",
            "malignant neoplasm of ureter":                                                                         "C66",
            "malignant neoplasm of bladder":                                                                        "C67",
            "malignant neoplasm of other and unspecified urinary organs":                                           "C68",
            "malignant neoplasm of eye and adnexa":                                                                 "C69",
            "malignant neoplasm of meninges":                                                                       "C70",
            "malignant neoplasm of brain":                                                                          "C71",
            "malignant neoplasm of spinal cord, cranial nerves and other parts of central nervous system":          "C72",
            "malignant neoplasm of thyroid gland":                                                                  "C73",
            "malignant neoplasm of adrenal gland":                                                                  "C74",
            "malignant neoplasm of other endocrine glands and related structures":                                  "C75",
            "malignant neoplasm of other and ill-defined sites":                                                    "C76",
            "secondary and unspecified malignant neoplasm of lymph nodes":                                          "C77",
            "secondary malignant neoplasm of respiratory and digestive organs":                                     "C78",
            "secondary malignant neoplasm of other and unspecified sites":                                          "C79",
            "malignant neuroendocrine tumors":                                                                      "C7A",
            "secondary neuroendocrine tumors":                                                                      "C7B",
            "malignant neoplasm without specification of site":                                                     "C80",
            "hodgkin lymphoma":                                                                                     "C81",
            "follicular lymphoma":                                                                                  "C82",
            "non-follicular lymphoma":                                                                              "C83",
            "mature t/nk-cell lymphomas":                                                                           "C84",
            "other specified and unspecified types of non-hodgkin lymphoma":                                        "C85",
            "other specified types of t/nk-cell lymphoma":                                                          "C86",
            "malignant immunoproliferative diseases and certain other b-cell lymphomas":                            "C88",
            "multiple myeloma and malignant plasma cell neoplasms":                                                 "C90",
            "lymphoid leukemia":                                                                                    "C91",
            "myeloid leukemia":                                                                                     "C92",
            "monocytic leukemia":                                                                                   "C93",
            "other leukemias of specified cell type":                                                               "C94",
            "leukemia of unspecified cell type":                                                                    "C95",
            "other and unspecified malignant neoplasms of lymphoid, hematopoietic and related tissue":              "C96"
        }

    else:
        raise ValueError(f'Unrecognized sample stream input to function map_icd10_codes: ' + sample_stream)

    # rename columns
    df = df.rename(columns=icd10_mapper)

    # get list of icd10 columns, which are the columns included the icd10_mapper
    icd10_cols = pd.Index(list(icd10_mapper.values()))

    # collapse binary columns into list of true icd10 categories
    df['icd10'] = df[icd10_cols].astype('bool').apply(lambda row: icd10_cols[row], axis=1)
    
    # remove binary icd10 columns
    df = df.drop(list(icd10_mapper.values()), axis='columns')

    return df


def map_flu_vaccine_kp2023(df: pd.DataFrame, column: str) -> None:
    """
    Given a DataFrame *df* and an existing numeric *column* in the df,
    replaces the numeric codes in the column with strings that describe
    the flu vaccine received, according to the 2023 KP Data Dictionary
    """
    kp2023_flu_mapper = {
        0:  "Afluria Quadrivalent",
        1:  "Fluad Quadrivalent",
        2:  "Fluarix Quadrivalent",
        3:  "Flublok Quadrivalent",
        4:  "Flucelvax Quadrivalent",
        5:  "Flulaval Quadrivalent",
        6:  "Flumist Quadrivalent",
        7:  "Fluzone High-Dose Quadrivalent",
        8:  "Fluzone Quadrivalent",
        9:  "Unknown"
    }

    # for any records whose flu_type value is not either in mapper or null, return the collection id
    invalid_flu_values = df.loc[df[column].apply(lambda x: False if (x in kp2023_flu_mapper or pd.isna(x)) else True)]['collection_id'].values
    if len(invalid_flu_values) > 0:
        raise ValueError(f'One or more invalid KP2023 flu vaccine values. Valid values are 0-9 or null. These Marshfield lab IDs have invalid flu vaccine values: {*invalid_flu_values,}')
    
    # map numeric values to strings based on dict
    # note that this is exhaustive mapping, so any value not 0-9 will be changed to NaN
    df[column] = df[column].map(kp2023_flu_mapper)


def map_ethnicity_kp2023(df: pd.DataFrame, column: str) -> None:
    """
    Given a DataFrame *df* and an existing numeric *column* in the df,
    replaces the numeric codes in the column with strings that describe
    ethnicity, according to the 2023 KP Data Dictionary
    """
    kp2023_ethnicity_mapper = {
        0:  "Not Hispanic or Latino",
        1:  "Hispanic or Latino",
        8:  "Don't know",
        9: "Prefer not to answer"
    }

    # for any records whose ethnicity value is not either in mapper or null, return the collection id
    invalid_ethnicity_values = df.loc[df[column].apply(lambda x: False if (x in kp2023_ethnicity_mapper or pd.isna(x)) else True)]['collection_id'].values
    if len(invalid_ethnicity_values) > 0:
        raise ValueError(f'One or more invalid KP2023 ethnicity ("HispanicLatino") values. Valid values are 0,1,8,9 or null. These Marshfield lab IDs have invalid ethnicity values: {*invalid_ethnicity_values,}')
    
    df[column] = df[column].map(kp2023_ethnicity_mapper)  


@clinical.command("match-kp2023")
@click.argument("kp2023_manifest_filename", metavar = "<KP2023 Clinical Manifest filename>",
                type = click.Path(exists= True, dir_okay=False))
@click.argument("kp2023_manifest_matched_filename", metavar = "<KP2023 Clinical Manifest Matched Data filename>",
                type = click.Path(dir_okay=False))
@click.argument("kp2023_manifest_unmatched_output_filename", metavar = "<KP2023 Clinical Manifest Unmatched Data output filename>",
                type = click.Path(dir_okay=False))


def match_kp2023(kp2023_manifest_filename: str, kp2023_manifest_matched_filename: str, kp2023_manifest_unmatched_output_filename: str) -> None:
    """
    Match clinical data from KP2023 with identifiers from the LIMS.

    Given a <KP2023 Clinical Manifest filename> which has records to be matched,
    and, a <KP2023 Clinical Manifest Matched Data filename> which has records that have
    already been matched to LIMS identifiers, attempts to match the records in
    <KP2023 Clinical Manifest filename> to LIMS data and adds any newly matched
    records to the matched file. Removes any matches from <KP2023 Clinical Manifest name>
    before writing it to <KP2023 Clinical Manifest Unmatched Data output filename>.

    <KP2023 Clinical Manifest Matched Data filename> does not have to be an existing file,
    but a filename must be provided. If the file does not exist, the newly matched records
    will be output to stdout without consolidating with previously matched records.

    <KP2023 Clinical Manifest Unmatched Data output filename> does not have to exist, and
    if a file with this path exists, it will be overwritten.

    PII is removed by this function.

    <KP2023 Clinical Manifest filename> can include records that have been previously matched,
    or it can consist of only unmatched records. All matched clinical records (both previously
    and newly matched records) are output to stdout as newline-delimited JSON records.
    You will likely want to redirect stdout to a file.
    """
    clinical_records = pd.read_json(kp2023_manifest_filename, orient='records', dtype={'census_tract': 'string', 'age': 'int64'}, lines=True)
    # if the <KP2023 Clinical Manifest Matched Data filename> file exists, read the file into a df; otherwise, create an empty df
    if os.path.exists(kp2023_manifest_matched_filename):
        matched_clinical_records = pd.read_json(kp2023_manifest_matched_filename, orient='records', dtype={'census_tract': 'string', 'age': 'int64'}, lines=True)
    else:
        LOG.debug("No previously matched data was provided.")
        matched_clinical_records = pd.DataFrame()

    LOG.info(f"Attempting to match {len(clinical_records)} identifiers to LIMS data")
    # identifier_pairs is a dict where keys are clinical identifiers and values are corresponding matrixIds from LIMS
    identifier_pairs = match_lims_identifiers(clinical_records, KP2023_IDENTIFIERS)

    # add 'barcode' column, which contains lims matrixIds
    clinical_records['barcode'] = pd.NA
    for identifier in KP2023_IDENTIFIERS.keys():
        clinical_records['barcode'] = clinical_records.apply(
            lambda row: identifier_pairs.get(row[identifier], pd.NA) if pd.isna(row['barcode']) else row['barcode'],
            axis=1
        )

    newly_matched_clinical_records = clinical_records.loc[~clinical_records['barcode'].isna()]
    unmatched_clinical_records = clinical_records[clinical_records['barcode'].isna()]

    # keep clinical identifier (collection_id) to use as individual id for FHIR bundle
    # in this case, the clinical identifier is probably a 'safe' external ID, but we treat it as PII out of an abundance of caution
    # therefore, hash the clinical identifier in newly_matched_clinical_records
    for clinical_identifier in KP2023_IDENTIFIERS.keys():
        newly_matched_clinical_records[clinical_identifier] = newly_matched_clinical_records[clinical_identifier].apply(generate_hash)

    # newly matched barcodes shouldn't be in our previously matched data.
    # any barcodes that show up in a previous parse might contain refreshed data,
    # so we should keep the newly matched data and drop the old match. If there is
    # no difference, our diff won't pull this into ID3C.
    #
    # it is necessary to combine the newly matched records with the old matched records
    # because if the input <KP2023 Clinical Manifest filename> is the unmatched manifest,
    # then it will not contain any old matched records, so we want to store all old matched records
    # and add to that manifest each time this command is run.
    if newly_matched_clinical_records.empty:
        LOG.debug("No new records were matched to LIMS data")
    elif matched_clinical_records.empty:
        LOG.debug(f"{len(newly_matched_clinical_records)} were matched. No previously matched data to consolidate")
    else:
        LOG.debug(f"{len(newly_matched_clinical_records)} were matched. Refreshing all previously matched data")

        # old versions of matched records whose barcode is in both the old matched and new matched records:
        refreshed_old_records = matched_clinical_records.barcode.isin(newly_matched_clinical_records.barcode)
        # new versions of those matched records:
        refreshed_new_records = newly_matched_clinical_records.barcode.isin(matched_clinical_records.barcode)
        # take the old versions out
        matched_clinical_records = matched_clinical_records.loc[~refreshed_old_records, :]
        LOG.debug(f"{len(matched_clinical_records)} previously matched records remain after dropping potentially refreshed records")

        # and put the new versions in
        newly_refreshed_clinical_records = newly_matched_clinical_records.loc[refreshed_new_records, :]
        newly_paired_clinical_records = newly_matched_clinical_records.loc[~refreshed_new_records, :]
        LOG.debug(f"{len(newly_paired_clinical_records)} had not been previously matched. {len(newly_refreshed_clinical_records)} had been previously matched and were refreshed")

        newly_matched_clinical_records = pd.concat([newly_paired_clinical_records, newly_refreshed_clinical_records]).reset_index(drop=True)

    matched_clinical_records = pd.concat([matched_clinical_records, newly_matched_clinical_records]).reset_index(drop=True)
    LOG.info(f"A total of {len(matched_clinical_records)} records are matched to LIMS data with {len(unmatched_clinical_records)} still unmatched.")

    if not unmatched_clinical_records.empty:
        unmatched_clinical_records.to_json(kp2023_manifest_unmatched_output_filename, orient='records', lines=True)
    if not matched_clinical_records.empty:
        dump_ndjson(matched_clinical_records)


@clinical.command("deduplicate-kp2023")
@click.argument("kp2023_master_manifest_filename", metavar = "<KP2023 Clinical Master Manifest filename>",
                type = click.Path(exists= True, dir_okay=False))


def deduplicate_kp2023(kp2023_master_manifest_filename: str) -> None:
    """
    Remove duplicate records, keeping only those from the most recently updated spreadsheet.

    Given a *KP2023 Clinical Master Manifest filename*, which contains parsed KP2023 records and whose provenance
    includes a timestamp field indicating the last updated time of the provenance spreadsheet, outputs a 
    deduplicated version. When duplicates are encountered, the record with the most recent provenance timestamp
    is output.

    Writes deduplicated records in ndjson format to stdout.
    """

    # read in ndjson as pandas df
    clinical_records = pd.read_json(kp2023_master_manifest_filename, orient='records', dtype={'census_tract': 'string', 'age': 'int64'}, lines=True)

    if clinical_records.empty:
        LOG.info("No clinical records provided, nothing to deduplicate.")
        return

    # sort by timestamp
    clinical_records = clinical_records.sort_values(by='spreadsheet_timestamp', ascending=True)

    # use encounter identifier to identify duplicates
    # keep only the duplicate with the latest timestamp
    duplicates = clinical_records.duplicated(subset='identifier', keep='last')
    duplicated_records = clinical_records.loc[duplicates, :]
    clinical_records = clinical_records.loc[~duplicates, :]

    # report on removed duplicates
    # get unique list of provenance filenames from removed duplicated records
    # to parse _provenance column: convert _provenance to str, then convert single to double quotes and parse as json
    duplicated_records['provenance_filename'] = duplicated_records['_provenance'].apply(lambda x: (json.loads(str(x).replace("\'", "\"")))['filename'])
    duplicated_provenance_filenames = duplicated_records['provenance_filename'].unique()
    if len(duplicated_records) > 0:
        LOG.warning(f"Warning: Removed {len(duplicated_records)} duplicated KP2023 records. \n" +
                    f"Duplicated records came from the following provenance(s): {*duplicated_provenance_filenames,}")

    # drop spreadsheet_timestamp column
    clinical_records = clinical_records.drop(columns=['spreadsheet_timestamp'])

    LOG.info(f"Dumping {len(clinical_records)} deduplicated KP2023 records to stdout")
    dump_ndjson(clinical_records)


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
    # sample id of the queried record.
    matched_identifiers = {}
    for identifiers in lims_search_results:
        for term in set(lims_identifiers.values()):
            if identifiers and term in identifiers:
                matched_identifiers[identifiers[term]] = identifiers['matrixId']

    LOG.debug(f"Found an identifier match for {len(matched_identifiers)} identifiers")
    return matched_identifiers

class MissingColumn(KeyError):
    """
    Raised by :function: `parse-kp2023` if any expected columns
    are not found in the input spreadsheet after standardizing column names
    """
    pass

class UnexpectedNumeric(KeyError):
    """
    Raised by function parse-kp2023 if any columns that are expected to be binary
    have values other than 0/1/None
    """
    pass
