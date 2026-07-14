import logging
from typing import Optional

from sfdata_stream_parser.filters import generic

from liiatools.cans_pipeline.stream_filters import transform_input
from liiatools.common import stream_filters as stream_functions
from liiatools.common.data import (
    DataContainer,
    FileLocator,
    PipelineConfig,
    ProcessResult,
)
from liiatools.common.spec.__data_schema import DataSchema
from liiatools.common.stream_pipeline import to_dataframe

logger = logging.getLogger(__name__)


def task_cleanfile(
    src_file: FileLocator,
    schema: DataSchema,
    output_config: PipelineConfig,
    logger: Optional[logging.Logger] = None,
) -> ProcessResult:
    """
    Clean input CANS xlsx files according to schema and output clean data and errors
    :param src_file: The pointer to a file in a virtual filesystem
    :param schema: The data schema in a DataSchema class
    :param logger: Optional logger to log messages
    :return: A class containing a DataContainer and ErrorContainer
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Collect table information for stream
    table_info = stream_functions.table_spec_from_filename(
        schema=schema, filename=src_file.name, output_config=output_config
    )

    # Open & Parse file
    stream = transform_input(src_file, table_info)
    stream = stream_functions.pandas_dataframe_to_stream(stream, filename=src_file.name, sheetname=table_info["sheetname"])
    logger.info("File %s opened and parsed, beginning processing", src_file.name)

    # Configure stream
    stream = stream_functions.add_table_name_from_headers(stream, schema=schema)
    stream = stream_functions.inherit_property(stream, ["table_name", "table_spec"])
    stream = stream_functions.convert_column_header_to_match(stream, schema=schema)
    stream = stream_functions.match_config_to_cell(stream, schema=schema)

    logger.info("Stream for file %s configured", src_file.name)

    # Clean stream
    stream = stream_functions.log_blanks(stream)
    stream = stream_functions.conform_cell_types(stream)

    logger.info("Stream for file %s cleaned", src_file.name)

    # Create dataset
    stream = stream_functions.collect_cell_values_for_row(stream)
    dataset_holder, stream = stream_functions.collect_tables(stream)
    error_holder, stream = stream_functions.collect_errors(stream)

    logger.info("Dataset created from file %s", src_file.name)

    # Consume stream so we know it's been processed
    generic.consume(stream)

    dataset = dataset_holder.value
    errors = error_holder.value

    logger.info(
        "Completed processing file %s with the following tables: %s",
        src_file.name,
        list(dataset.keys()),
    )

    dataset = DataContainer(
        {k: to_dataframe(v, schema.table[k]) for k, v in dataset.items()}
    )

    return ProcessResult(data=dataset, errors=errors)
