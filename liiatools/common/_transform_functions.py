import hashlib
from typing import Dict

import pandas as pd
from dagster import get_dagster_logger

from liiatools.common.converters import to_nth_of_month, to_short_postcode
from liiatools.common.reference import authorities

from .data import ColumnConfig, Metadata

log = get_dagster_logger(__name__)


def _get_first(metadata: Dict, *keys, default=None):
    for key in keys:
        if key in metadata:
            return metadata[key]
    return default


def add_la_suffix(
    row: pd.Series, column_config: ColumnConfig, metadata: Metadata
) -> str:
    return f"{row[column_config.id]}_{metadata['la_code']}"


def add_la_code(row: pd.Series, column_config: ColumnConfig, metadata: Metadata) -> str:
    return metadata["la_code"]


def add_la_name(row: pd.Series, column_config: ColumnConfig, metadata: Metadata) -> str:
    return authorities.get_by_code(metadata["la_code"])


def add_year(row: pd.Series, column_config: ColumnConfig, metadata: Metadata) -> str:
    return metadata["year"]


def add_month(row: pd.Series, column_config: ColumnConfig, metadata: Metadata) -> str:
    log.info(f"Metadata {metadata}")
    month_map = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    return month_map[metadata["month"]]


def to_integer(
    row: pd.Series, column_config: ColumnConfig, metadata: Metadata
) -> str | int:
    try:
        return int(float(row[column_config.id]))
    except ValueError:
        return row[column_config.id]


def add_school_year(
    row: pd.Series, column_config: ColumnConfig, metadata: Metadata
) -> str:
    date_value = row["PersonBirthDate"]
    if date_value.month >= 9:
        school_year = date_value.year
    elif date_value.month <= 8:
        school_year = date_value.year - 1
    else:
        school_year = None
    return school_year


enrich_functions = {
    "add_la_suffix": add_la_suffix,
    "la_code": add_la_code,
    "la_name": add_la_name,
    "year": add_year,
    "month": add_month,
    "integer": to_integer,
    "school_year": add_school_year,
}


def degrade_to_first_of_month(
    row: pd.Series, column_config: ColumnConfig, metadata: Metadata
) -> str:
    return to_nth_of_month(row[column_config.id], 1)


def degrade_to_short_postcode(
    row: pd.Series, column_config: ColumnConfig, metadata: Metadata
) -> str:
    return to_short_postcode(row[column_config.id])


def hash_column_sha256(
    row: pd.Series, column_config: ColumnConfig, metadata: Metadata
) -> str:
    value = row[column_config.id]
    if not value:
        return value

    digest = hashlib.sha256()
    digest.update(str(value).encode("utf-8"))

    salt = _get_first(metadata, f"sha256_salt_{column_config.id}", "sha256_salt")
    if salt:
        digest.update(salt.encode("utf-8"))

    return digest.hexdigest()


degrade_functions = {
    "first_of_month": degrade_to_first_of_month,
    "short_postcode": degrade_to_short_postcode,
    "hash_sha256": hash_column_sha256,
}
