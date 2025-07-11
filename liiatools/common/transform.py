import logging
from datetime import datetime
from typing import Callable, Dict

import pandas as pd

from liiatools.common.checks import check_la_signature
from liiatools.common.data import (
    DataContainer,
    ErrorContainer,
    Metadata,
    PipelineConfig,
    ProcessResult,
    TableConfig,
)

from ._transform_functions import degrade_functions, enrich_functions

logger = logging.getLogger(__name__)


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
            if isinstance(transform_name, list):
                for transform_name in transform_name:
                    assert (
                        transform_name in functions
                    ), f"Unknown transform for property '{property}': {transform_name}"
                    data[column_config.id] = data.apply(
                        lambda row: functions[transform_name](
                            row, column_config, metadata
                        ),
                        axis=1,
                    )
            else:
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
) -> ProcessResult:
    """Pipelines can have a set of data transforms that are applied to the data after it has been cleaned.

    The standard ones we have are enrich and degrade. Enrich adds new columns to the data, degrade modifies existing
    columns to remove or minimise identifying information.
    """

    # Create a copy of the data so we don't mutate the original
    data = data.copy()

    errors = ErrorContainer()

    # Loop over known tables
    try:
        for table_config in config.table_list:
            if table_config.id in data:
                _transform(
                    data[table_config.id], table_config, metadata, property, functions
                )
                remove_row_mask = (
                    ~data[table_config.id].isin(["remove_row"]).any(axis=1)
                )
                remove_row_indices = (
                    data[table_config.id].index[~remove_row_mask].tolist()
                )
                data[table_config.id] = data[table_config.id][remove_row_mask]

                for row in remove_row_indices:
                    errors.append(
                        dict(
                            type="InvalidMandatoryField",
                            message=f"Row {row} removed due to invalid mandatory field",
                            table_name=table_config.id,
                            r_ix=row,
                        )
                    )
    except Exception as e:
        # As this step is crucial for ensure privacy, if we have any errors we should fail and return no data for this dataset.
        logger.exception(f"Error in {property} transform")
        data = DataContainer()
        errors.append(dict(type="TransformError", message=str(e)))

    return ProcessResult(data=data, errors=errors)


def enrich_data(
    data: DataContainer, config: PipelineConfig, metadata: Metadata = None
) -> ProcessResult:
    """Standard set of enrichment transforms adding or modifying columns in the dataset."""
    if metadata is None:
        metadata = {}

    return data_transforms(data, config, metadata, "enrich", enrich_functions)


def degrade_data(
    data: DataContainer, config: PipelineConfig, metadata: Metadata = None
) -> ProcessResult:
    """Standard set of degradation transforms removing or modifying columns in the dataset."""
    if metadata is None:
        metadata = {}

    return data_transforms(data, config, metadata, "degrade", degrade_functions)


def prepare_export(
    data: DataContainer, config: PipelineConfig, profile: str = None
) -> DataContainer:
    """
    Prepare data for export by removing columns that are not required for the given profile
    or for all configured tables if no profile is given.

    The DataContainer will only hold tables and columns that are configured in the config,
    and only tables that also exist in the data. If a configured column is missing from a table,
    it will be created. The columns will also be in the order specified in the config.

    :param data: The data to prepare for export
    :param config: The pipeline config
    :param profile: The profile to export for (optional)
    :return: The prepared data
    """
    data_container = DataContainer()

    table_list = config.tables_for_profile(profile) if profile else config.table_list

    # Loop over known tables
    for table_config in table_list:
        table_name = table_config.id
        table_config = (
            table_config.columns_for_profile(profile)
            if profile
            else table_config.columns
        )
        table_columns = [column.id for column in table_config]
        column_types = {col.id: col.type for col in table_config}

        # Only export if the table is in the data
        if table_name in data:
            table = data[table_name].copy()

            for column_name, type in column_types.items():
                # Create any missing columns
                if column_name not in table.columns:
                    table[column_name] = None
                # If column is numeric, ensure blanks are converted to NaN
                if type == "float":
                    table[column_name] = pd.to_numeric(
                        table[column_name], errors="coerce"
                    )
                elif type == "integer":
                    table[column_name] = pd.to_numeric(
                        table[column_name], errors="coerce"
                    ).astype("Int64")

            # Return the subset
            data_container[table_name] = table[table_columns].copy()

    return data_container


def apply_retention(
    data: DataContainer,
    config: PipelineConfig,
    profile: str,
    year_column: str,
    la_column: str,
) -> DataContainer:
    """
    Apply retention rules to the data including number of years for each profile/use case and LAs that have signed
    up to each profile/use case

    :param data: The data to apply retention to
    :param config: The pipeline config
    :param profile: The profile to apply retention for
    :param year_column: The column containing the year for data retention
    :param la_column: The column containing the LA code for data retention
    :return: The data with retention applied
    """
    data_container = DataContainer()
    retention_period = config.retention_period[profile]
    signed_las = check_la_signature(config.la_signed, profile)
    current_year = datetime.now().year

    for table_name in data:
        table = data[table_name].copy()
        data_container[table_name] = table[
            table[year_column] > current_year - retention_period
        ]

        table = data_container[table_name].copy()
        data_container[table_name] = table[table[la_column].isin(signed_las)]

    return data_container
