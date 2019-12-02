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
from id3c.cli.command import dump_ndjson
from . import (
    add_provenance,
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
        dtypes = {'tract_identifier': 'str'},
        dates = ['Collection.Date'],
        na_values = ['NA', '', 'Unknown', 'NULL'],
    )

    clinical_records = (
        read_uw(uw_filename)
            .pipe(trim_whitespace)
            .pipe(add_provenance, uw_filename)
            .pipe(create_unique_identifier))

    # Standardize names of columns that will be added to the database
    column_map = {
        'Age': 'age',
        'Collection_ID': 'barcode',
        'EthnicGroup': 'HispanicLatino',
        'Fac': 'site',
        'FinClass': 'MedicalInsurance',
        'LabDtTm': 'encountered',
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

    barcode_quality_control(clinical_records, output)

    # Convert dtypes
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"])
    # Age must be converted to Int64 dtype because pandas does not support NaNs
    # with normal type 'int'
    clinical_records["age"] = clinical_records["age"].astype(pd.Int64Dtype())

    # Subset df to drop missing barcodes
    clinical_records = drop_missing_rows(clinical_records, 'barcode')

    # Drop columns we're not tracking
    clinical_records = clinical_records[column_map.values()]

    clinical_records = remove_pii(clinical_records)


    dump_ndjson(clinical_records)


def create_unique_identifier(df: pd.DataFrame):
    """Generate a unique identifier for each encounter and drop duplicates"""
    df['identifier'] = (df['labMRN'] + df['LabAccNum'] + \
                        df['Collection.Date'].astype(str)
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

    return df

def age_ceiling(age: float, max_age=90) -> float:
    """
    Given an *age*, returns the same *age* unless it exceeds the *max_age*, in
    which case the *max_age* is returned.
    """
    return min(age, max_age)

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
    Returns a filtered version of the given *df* where rows with ``null`` values
    for the given *column* have been removed.
    """
    return df.loc[df[column].notnull()]

@clinical.command("parse-sch")
@click.argument("sch_filename", metavar = "<SCH Clinical Data filename>")
@click.option("-o", "--output", metavar="<output filename>",
    help="The filename for the output of missing barcodes")

def parse_sch(sch_filename, output):
    """
    Process clinical data from SCH.

    All clinical records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    dtypes = {'census_tract': 'str'}
    clinical_records = pd.read_csv(sch_filename, dtype=dtypes)
    clinical_records = trim_whitespace(clinical_records)
    clinical_records = add_provenance(clinical_records, sch_filename)
    clinical_records = add_insurance(clinical_records)

    # Standardize column names
    column_map = {
        "pat_id2": "individual",
        "study_id": "barcode",
        "drawndate": "encountered",
        "age": "age",
        "sex": "AssignedSex",
        "ethnicity": "HispanicLatino",
        "race": "Race",
        "vaccine_given": "FluShot",
        "MedicalInsurance": "MedicalInsurance",
        "census_tract": "census_tract",
        "_provenance": "_provenance",
    }
    clinical_records = clinical_records.rename(columns=column_map)

    barcode_quality_control(clinical_records, output)

    # Subset df to drop missing encountered date
    clinical_records = drop_missing_rows(clinical_records, 'encountered')

    # Drop unnecessary columns
    columns_to_keep = list(column_map.values()) + [  # Test result columns
        'adeno', 'chlamydia', 'corona_229e', 'corona_hku1', 'corona_nl63', 'corona_oc43',
        'flu_a_h3', 'flu_a_h1_2009', 'flu_b', 'flu_a', 'flu_a_h1', 'hmpv', 'mycoplasma',
        'paraflu_1_4', 'pertussis', 'rhino_ent', 'rsv'
    ]
    clinical_records = clinical_records[columns_to_keep]

    # Convert dtypes
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"])

    # Insert static value columns
    clinical_records["site"] = "SCH"

    clinical_records = create_encounter_identifier(clinical_records)
    clinical_records = remove_pii(clinical_records)

    dump_ndjson(clinical_records)


def add_insurance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column for insurance type to a given *df*. Returns the new
    DataFrame.
    """
    def insurance(series: pd.Series) -> pd.Series:
        """ Returns an array of unique insurance types from a given *series*. """
        insurance_columns = ['insurance_1', 'insurance_2']
        insurances = [ series[i] for i in insurance_columns if not pd.isna(series[i])]
        return list(set(insurances))

    df['MedicalInsurance'] = df.apply(insurance, axis='columns')
    return df

def create_encounter_identifier(df: pd.DataFrame) -> pd.DataFrame:
    """ Creates an encounter identifier column on a given *df*. Return the
    modified DataFrame.
    """
    df["identifier"] = (
        df["individual"] + df["encountered"].astype(str)
        ).str.lower()

    return df


@clinical.command("parse-kp")
@click.argument("kp_filename", metavar = "<KP Clinical Data filename>")
@click.argument("kp_specimen_manifest_filename", metavar = "<KP Specimen Manifest filename>")
@click.option("-o", "--output", metavar="<output filename>",
    help="The filename for the output of missing barcodes")

def parse_kp(kp_filename, kp_specimen_manifest_filename, output):
    """
    Process clinical data from KP.

    All clinical records parsed are output to stdout as newline-delimited JSON
    records.  You will likely want to redirect stdout to a file.
    """
    clinical_records = pd.read_csv(kp_filename)
    clinical_records.columns = clinical_records.columns.str.lower()

    clinical_records = trim_whitespace(clinical_records)
    clinical_records = add_provenance(clinical_records, kp_filename)
    clinical_records = add_kp_manifest_data(clinical_records, kp_specimen_manifest_filename)

    clinical_records = convert_numeric_columns_to_binary(clinical_records)
    clinical_records = rename_symptoms_columns(clinical_records)
    clinical_records = collapse_columns(clinical_records, 'symptom')
    clinical_records = collapse_columns(clinical_records, 'race')

    clinical_records['FluShot'] = clinical_records['fluvaxdt'].notna()

    column_map = {  # Missing census_tract
        "enrollid": "individual",
        "enrolldate": "encountered",
        "barcode": "barcode",
        "age": "age",
        "sex": "AssignedSex",
        "race": "Race",
        "hispanic": "HispanicLatino",
        "symptom": "Symptoms",
        "FluShot": "FluShot",
        "_provenance": "_provenance",
    }
    clinical_records = clinical_records.rename(columns=column_map)

    barcode_quality_control(clinical_records, output)

    # Drop unnecessary columns
    clinical_records = clinical_records[column_map.values()]

    # Convert dtypes
    clinical_records["encountered"] = pd.to_datetime(clinical_records["encountered"])

    # Insert static value columns
    clinical_records["site"] = "KP"

    clinical_records = create_encounter_identifier(clinical_records)
    clinical_records = remove_pii(clinical_records)

    # Placeholder columns for future data.
    # See https://seattle-flu-study.slack.com/archives/CCAA9RBFS/p1568156642033700?thread_ts=1568145908.029300&cid=CCAA9RBFS
    clinical_records["MedicalInsurace"] = None

    dump_ndjson(clinical_records)


def add_kp_manifest_data(df: pd.DataFrame, manifest_filename: str) -> pd.DataFrame:
    """
    Join the specimen manifest data from the given *manifest_filename* with the
    given clinical records DataFrame *df*
    """
    barcode = 'Barcode ID (Sample ID)'
    dtypes = {barcode: str}

    manifest_data = pd.read_excel(manifest_filename, sheet_name='KP', dtype=dtypes)

    regex = re.compile(r"^KP-([0-9]{6,})-[0-9]$", re.IGNORECASE)
    manifest_data.kp_id = manifest_data.kp_id.apply(lambda x: regex.sub('WA\\1', x))

    rename_map = {
        barcode: 'barcode',
        'kp_id': 'enrollid',
    }

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
    """
    stub_data = df.filter(regex=f'{pid}|{stub}*', axis='columns')
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
