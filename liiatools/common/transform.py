from typing import Callable, Dict

import pandas as pd

from liiatools.common.data import (
    ColumnConfig,
    DataContainer,
    Metadata,
    PipelineConfig,
    TableConfig,
)

from ._transform_functions import degrade_functions, enrich_functions


def _transform(
    data: pd.DataFrame,
    table_config: TableConfig,
    metadata: Metadata,
    property: str,
    functions: Dict[str, Callable],
):
    """Performs a transform on a table"""
    for column_config in table_config.columns:
        transform_name = getattr(column_config, property)
        if transform_name:
            assert (
                transform_name in functions
            ), f"Unknown transform for property '{property}': {transform_name}"
            data[column_config.id] = data.apply(
                lambda row: functions[transform_name](row, column_config, metadata),
                axis=1,
            )


def data_transforms(
    data: DataContainer,
    config: PipelineConfig,
    metadata: Metadata,
    property: str,
    functions: Dict[str, Callable],
) -> DataContainer:
    """Pipelines can have a set of data transforms that are applied to the data after it has been cleaned.

    The standard ones we have are enrich and degrade. Enrich adds new columns to the data, degrade modifies existing
    columns to remove or minimise identifying information.
    """

    # Create a copy of the data so we don't mutate the original
    data = {k: v.copy() for k, v in data.items()}

    # Loop over known tables
    for table_config in config.table_list:
        if table_config.id in data:
            _transform(
                data[table_config.id], table_config, metadata, property, functions
            )
    return data


def enrich_data(
    data: DataContainer, config: PipelineConfig, metadata: Metadata = None
) -> DataContainer:
    """Standard set of enrichment transforms adding or modifying columns in the dataset."""
    if metadata is None:
        metadata = {}

    return data_transforms(data, config, metadata, "enrich", enrich_functions)


def degrade_data(
    data: DataContainer, config: PipelineConfig, metadata: Metadata = None
) -> DataContainer:
    """Standard set of degradation transforms removing or modifying columns in the dataset."""
    if metadata is None:
        metadata = {}

    return data_transforms(data, config, metadata, "degrade", degrade_functions)


def prepare_export(
    data: DataContainer, config: PipelineConfig, profile: str = None
) -> DataContainer:
    """Prepare data for export by removing columns that are not required for the given profile"""
    data_container = {}

    table_list = config.tables_for_profile(profile) if profile else config.table_list

    # Loop over known tables
    for table_config in table_list:
        table_name = table_config.id
        table_columns = (
            table_config.columns_for_profile(profile)
            if profile
            else table_config.columns
        )
        table_columns = [column.id for column in table_columns]

        # Only export if the table is in the data
        if table_name in data:
            table = data[table_name].copy()

            # Create any missing columns
            for column_name in table_columns:
                if column_name not in table.columns:
                    table[column_name] = None

            # Return the subset
            data_container[table_name] = table[table_columns].copy()

    return data_container
