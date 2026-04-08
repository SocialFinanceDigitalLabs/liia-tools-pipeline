import logging
from typing import Optional

from sfdata_stream_parser.filters import generic

from liiatools.common import stream_filters as stream_functions
from liiatools.common.data import DataContainer, FileLocator, ProcessResult
from liiatools.common.spec.__data_schema import DataSchema
from liiatools.common.stream_pipeline import to_dataframe


def task_cleanfile(
        src_file: FileLocator, schema: DataSchema, logger: Optional[logging.Logger] = None
    ) -> ProcessResult:
    """
    Clean input School Census csv files according to schema and output clean data and errors
    :param src_file: The pointer to a file in a virtual filesystem
    :param schema: The data schema in a DataSchema class
    :param logger: Optional logger to log messages
    :return: A class containing a DataContainer and ErrorContainer
    """
    # Open & Parse file
    stream = stream_functions.tablib_parse(src_file)

    # Configure stream
    stream = stream_functions.add_table_name_from_headers(stream, schema=schema)
    stream = stream_functions.inherit_property(stream, ["table_name", "table_spec"])
    stream = stream_functions.match_config_to_cell(stream, schema=schema)

    # Clean stream
    stream = stream_functions.log_blanks(stream)
    stream = stream_functions.conform_cell_types(stream)

    # Create dataset
    stream = stream_functions.collect_cell_values_for_row(stream)
    dataset_holder, stream = stream_functions.collect_tables(stream)
    error_holder, stream = stream_functions.collect_errors(stream)

    # Consume stream so we know it's been processed
    generic.consume(stream)

    dataset = dataset_holder.value
    errors = error_holder.value

    dataset = DataContainer(
        {k: to_dataframe(v, schema.table[k]) for k, v in dataset.items()}
    )

    return ProcessResult(data=dataset, errors=errors)
