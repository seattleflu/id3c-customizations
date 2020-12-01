"""
Custom ID3C CLI commands.

This module is listed in the entry points configuration of setup.py, which
causes the core id3c.cli module to load this file at CLI runtime.

By in turn loading our own individual commands here, we allow each command
module to register itself via Click's decorators.
"""
import logging
import pandas as pd
from typing import List


# Load all ETL subcommands.
__all__ = [
    "etl",
    "clinical",
    "longitudinal",
    "reportable_conditions",
    "offer_uw_testing",
]


LOG = logging.getLogger(__name__)


def add_provenance(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """ Adds a ``_provenance`` column to a given DataFrame *df* for reporting """
    df['_provenance'] = list(map(lambda index: {
        'filename': filename,
        'row': index + 2}, df.index))
    return df


def age_ceiling(age: float, max_age=85) -> float:
    """
    Given an *age*, returns the same *age* unless it exceeds the *max_age*, in
    which case the *max_age* is returned.
    """
    return min(age, max_age)


def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """ Trims leading and trailing whitespace from strings in *df* """
    # Guard against AttributeErrors from entirely empty non-string dtype columns
    str_columns: List[str] = list(df.select_dtypes(include='string'))

    df[str_columns] = df[str_columns].apply(lambda column: column.str.strip())

    return df


def barcode_quality_control(clinical_records: pd.DataFrame, output: str) -> None:
    """ Perform quality control on barcodes """
    missing_barcodes = missing_barcode(clinical_records)
    duplicated_barcodes = duplicated_barcode(clinical_records)

    print_problem_barcodes(pd.concat([missing_barcodes, duplicated_barcodes],
                                 ignore_index=True), output)

    assert len(duplicated_barcodes) == 0, "You have duplicated barcodes!"


def missing_barcode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a pandas DataFrame *df*, returns a DataFrame with missing barcodes and
    a description of the problem.
    """
    missing_barcodes = df.loc[df['barcode'].isnull()].copy()
    missing_barcodes['problem'] = 'Missing barcode'

    return missing_barcodes


def duplicated_barcode(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a pandas DataFrame *df*, returns a DataFrame with duplicated barcodes
    and a description of the problem.
    """
    duplicates = pd.DataFrame(df.barcode.value_counts())
    duplicates = duplicates[duplicates['barcode'] > 1]
    duplicates = pd.Series(duplicates.index)

    duplicated_barcodes = df[df['barcode'].isin(duplicates)].copy()
    duplicated_barcodes['problem'] = 'Barcode is not unique'

    return duplicated_barcodes


def print_problem_barcodes(problem_barcodes: pd.DataFrame, output: str):
    """
    Given a pandas DataFrame of *problem_barcodes*, writes the data to
    the log unless a filename *output* is given.
    """
    if output:
        problem_barcodes.to_csv(output, index=False)
    else:
        problem_barcodes.apply(lambda x: LOG.warning(
            f"{x['problem']} in row {x['_provenance']['row']} of file "
            f"{x['_provenance']['filename']}, barcode {x['barcode']}"
        ), axis='columns')


def group_true_values_into_list(long_subset: pd.DataFrame, stub: str,
                                pid: List[str]) -> pd.DataFrame:
    """
    Given a long DataFrame *long_subset*, collapses rows with the same *pid*
    such that every *pid* is represented once in the resulting DataFrame. True
    values for each category in the given *stub* are collapsed into a
    human-readable list.
    """
    long_subset.dropna(inplace=True)
    long_subset[stub] = long_subset[stub].astype('bool')
    true_subset = long_subset[long_subset[stub]]

    return true_subset.groupby(pid + [stub]) \
                      .agg(lambda x: x.tolist()) \
                      .reset_index() \
                      .drop(stub, axis='columns') \
                      .rename(columns={f'new_{stub}': stub})


from . import *
