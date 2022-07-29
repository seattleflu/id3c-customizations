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
from functools import partial
from math import ceil
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
    default="year4",
    type=click.Choice(['year1','year2','year3','year4']),
    help="The format of input manifest file; default is \"year4\"")

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
    elif manifest_format in ['year3', 'year4']:
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
        elif manifest_format == 'year4':
            column_map.update({
                "covid_vx_manu1": "CovidShot1Manu",
                "covid_vx_manu2": "CovidShot2Manu",
                "covid_vx_manu3": "CovidShot3Manu",
                "covid_vx_d3": "CovidShot3",
                "cov_d3_date": "CovidShot3Date",
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

    clinical_records = clinical_records[columns_to_keep]

    # Convert dtypes
    # Incoming `encountered` value is typically just date but is cast to datetime with timezone in postgres. Timezone is
    # being specified here to ensure values are set to midnight local time instead of UTC.
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"]).dt.tz_localize('America/Los_Angeles')

    # Reformat vaccination dates. Values are immediately stripped of time component, so don't need timezone specified.
    if manifest_format in ['year3', 'year4']:
        clinical_records["FluShotDate"] = pd.to_datetime(clinical_records["FluShotDate"]).dt.strftime('%Y-%m-%d')
        clinical_records["CovidShot1Date"] = pd.to_datetime(clinical_records["CovidShot1Date"]).dt.strftime('%Y-%m-%d')
        clinical_records["CovidShot2Date"] = pd.to_datetime(clinical_records["CovidShot2Date"]).dt.strftime('%Y-%m-%d')
    if manifest_format == 'year4':
        clinical_records["CovidShot3Date"] = pd.to_datetime(clinical_records["CovidShot3Date"]).dt.strftime('%Y-%m-%d')

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
@click.argument("phskc_filename", metavar = "<PHSKC Clinical Data filename>")
@click.argument("phskc_specimen_manifest_filename", metavar = "<PHSKC Specimen Manifest filename(s)>")
@click.argument("geocoding_cache_file", envvar = "GEOCODING_CACHE", metavar = "<Geocoding cache filename>",
            type = click.Path(dir_okay=False, writable=True))

def parse_phskc(phskc_filename: str, phskc_specimen_manifest_filename: str, geocoding_cache_file: str = None) -> None:
    """
    Process clinical data from PHSKC.

    Given a <PHSKC Clinical Data filename> of an Excel document, a
    <PHSKC Specimen Manifest Filename> of a newline-delimited JSON document,
    and a <Geocoding Cache filename> selects specific columns of interest and
    reformats the queried data into a stream of JSON documents suitable for the
    "upload" sibling command.

    Clinical records are parsed and transformed into suitable data for our downstream
    FHIR and Clinical ETLs. Any PII is removed in this function. Parsed data is joined
    with barcode data present in the manifest file. Only data that matches existing
    barcode data will be included.

    All clinical records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    # specify type of inferred_symptomatic to prevent pandas casting automatically to boolean
    clinical_records = pd.read_excel(phskc_filename, dtype={'inferred_symptomatic': 'str'})
    clinical_records.columns = clinical_records.columns.str.lower()

    clinical_records = trim_whitespace(clinical_records)
    clinical_records = add_provenance(clinical_records, phskc_filename)

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

    # phskc data is sent with some rows duplicated, so before we add manifest data
    # we should drop these copied rows, keeping the first one
    clinical_records.drop_duplicates(subset=clinical_records.columns.difference(['_provenance']), inplace=True)
    clinical_records = add_phskc_manifest_data(clinical_records, phskc_specimen_manifest_filename)

    # drop all columns used for joining; we only need to ingest the joined barcode
    clinical_records.drop(['merge_col', 'main_cid', 'phskc_barcode', 'all_cids'], axis=1, inplace=True)

    if not clinical_records.empty:
        dump_ndjson(clinical_records)


def add_phskc_manifest_data(df: pd.DataFrame, manifest_filename: str) -> pd.DataFrame:
    """
    Join the specimen manifest data from the given *manifest_filename* with the
    given clinical records DataFrame *df*.
    """
    rename_map = {
        'cid': 'merge_col',
        'sample': 'barcode'
    }

    manifest_data = pd.read_json(manifest_filename, lines=True)
    manifest_data.dropna(subset=['cid'], inplace = True)

    manifest_data = manifest_data.rename(columns=rename_map)
    manifest_data = trim_whitespace(manifest_data)

    # find and drop AQ sheet rows containing a duplicated CID
    duplicated_cids = manifest_data.duplicated(subset=['merge_col'], keep=False)
    if duplicated_cids.any():
        LOG.warning(f'Dropping {duplicated_cids.sum()} rows with duplicated CID(s) from PHSKC manifest data')
        manifest_data = manifest_data[~duplicated_cids]

    # Since we get two CIDs with each PHSKC record and they aren't guaranteed
    # to be the same, we should try both if they are not the same (note: they
    # are almost always the same). If main_cid is in the AQ sheet, we will give
    # priority to that sample match. If it isn't, we can use the sample associated
    # with all_cids (if available). If both of these are not in the AQ sheet, we
    # should check if the barcode is, since it is also possible the lab used that
    # if the CID was not used. If the barcode is not in the AQ sheet, there is no
    # change to the output and no sample will match this record.
    # If the barcode is in the manifest, we will use that to link the record
    # to a sample and swap the barcode with the main CID for that row.
    df['merge_col'] = df['main_cid'].copy()

    if not df['main_cid'].equals(df['all_cids']):
        main_cid = pd.DataFrame(df['main_cid'])
        all_cid = pd.DataFrame(df['all_cids'])

        # merge cid dataframes to find common rows, use those common rows to select any rows
        # from main_cid and all_cids that are not in common
        common_cids = main_cid.merge(all_cid, left_on='main_cid', right_on='all_cids')
        differing_cids = df[
            (~main_cid.main_cid.isin(common_cids.main_cid)) & (~main_cid.main_cid.isin(common_cids.all_cids))
        ]

        # use all_cids only if it is in the manifest data and the main_cid value does not map to a barcode
        differing_cids = differing_cids.drop(
            differing_cids.loc[differing_cids['main_cid'].isin(manifest_data['merge_col'])].index
        )
        differing_cids['merge_col'] = differing_cids['all_cids'].copy()

        df.update(differing_cids)

    # Use barcode as a backup against any records where main_cid or all_cids don't match any AQ sheet records
    non_mapping_cids = df[(~df.main_cid.isin(manifest_data.merge_col)) & (~df.all_cids.isin(manifest_data.merge_col))]
    mappable_barcodes = non_mapping_cids[non_mapping_cids.phskc_barcode.isin(manifest_data.merge_col)].copy()
    mappable_barcodes['merge_col'] = mappable_barcodes['phskc_barcode']
    df.update(mappable_barcodes)

    # only need records that map to a barcode, so can inner merge
    return df.merge(manifest_data[['barcode', 'merge_col']], how='inner', on='merge_col')


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
