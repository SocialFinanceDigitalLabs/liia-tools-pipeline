import logging
from typing import Optional
from os.path import basename

from sfdata_stream_parser.filters import generic

from liiatools.common import stream_filters as stream_functions
from liiatools.common.data import DataContainer, FileLocator, ProcessResult
from liiatools.common.spec.__data_schema import DataSchema
from liiatools.common.stream_pipeline import to_dataframe


def task_cleanfile(src_file: FileLocator, schema: DataSchema, logger: Optional[logging.Logger]=None) -> ProcessResult:
    """
    Clean input Annex A xlsx files according to schema and output clean data and errors
    :param src_file: The pointer to a file in a virtual filesystem
    :param schema: The data schema in a DataSchema class
    :param logger: Optional logger to log messages
    :return: A class containing a DataContainer and ErrorContainer
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Open & Parse file
    stream = stream_functions.tablib_parse(src_file)

    logger.info("File %s opened and parsed, beginning processing", basename(src_file.name))

    # Configure stream
    stream = stream_functions.add_table_name(stream, schema=schema)
    stream = stream_functions.inherit_property(stream, ["table_name", "table_spec"])
    stream = stream_functions.convert_column_header_to_match(stream, schema=schema)
    stream = stream_functions.match_config_to_cell(stream, schema=schema)

    logger.info("Stream for file %s configured", basename(src_file.name))

    # Clean stream
    stream = stream_functions.log_blanks(stream)
    stream = stream_functions.conform_cell_types(stream)

    logger.info("Stream for file %s cleaned", basename(src_file.name))

    # Create dataset
    stream = stream_functions.collect_cell_values_for_row(stream)
    dataset_holder, stream = stream_functions.collect_tables(stream)
    error_holder, stream = stream_functions.collect_errors(stream)

    logger.info("Dataset created from file %s", basename(src_file.name))

    # Consume stream so we know it's been processed
    generic.consume(stream)

    dataset = dataset_holder.value
    errors = error_holder.value

    logger.info(
        "Completed processing file %s with the following tables: %s",
        basename(src_file.name),
        list(dataset.keys()),
    )

    dataset = DataContainer(
        {k: to_dataframe(v, schema.table[k]) for k, v in dataset.items()}
    )

    return ProcessResult(data=dataset, errors=errors)
