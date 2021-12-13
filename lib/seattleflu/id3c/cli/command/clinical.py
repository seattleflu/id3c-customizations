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


def generate_hash(identifier: str):
    """
    Generate hash for *identifier* that is linked to identifiable records.
    Must provide a "PARTICIPANT_DEIDENTIFIER_SECRET" as an OS environment
    variable.
    """
    secret = os.environ["PARTICIPANT_DEIDENTIFIER_SECRET"]

    assert len(secret) > 0, "Empty *secret* provided!"
    assert len(identifier) > 0, "Empty *identifier* provided!"

    new_hash = hashlib.sha256()
    new_hash.update(identifier.encode("utf-8"))
    new_hash.update(secret.encode("utf-8"))
    return new_hash.hexdigest()

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
    default="year3",
    type=click.Choice(['year1','year2','year3']),
    help="The format of input manifest file; default is \"year3\"")

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
    }

    # Accounting for differences in format for year 3
    if manifest_format in ['year1', 'year2']:
        column_map["vaccine_given"] = "FluShot"
    elif manifest_format == 'year3':
        column_map.update({
            "flu_vx_12mo": "FluShot",
            "flu_date": "FluShotDate",
            "covid_screen": "CovidScreen",
            "covid_vx_d1": "CovidShot1",
            "cov_d1_date": "CovidShot1Date",
            "covid_vx_d2": "CovidShot2",
            "cov_d2_date": "CovidShot2Date",
            "covid_vx_manu": "CovidShotManufacturer",
        })

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
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"])

    # Reformat vaccination dates
    if manifest_format == 'year3':
        clinical_records["FluShotDate"] = pd.to_datetime(clinical_records["FluShotDate"]).dt.strftime('%Y-%m-%d')
        clinical_records["CovidShot1Date"] = pd.to_datetime(clinical_records["CovidShot1Date"]).dt.strftime('%Y-%m-%d')
        clinical_records["CovidShot2Date"] = pd.to_datetime(clinical_records["CovidShot2Date"]).dt.strftime('%Y-%m-%d')

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
