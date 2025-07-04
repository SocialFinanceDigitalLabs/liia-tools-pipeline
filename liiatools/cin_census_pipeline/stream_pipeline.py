from pathlib import Path
from typing import Optional
import logging

from sfdata_stream_parser.filters import generic
from xmlschema import XMLSchema
from liiatools.common.data import PipelineConfig

from liiatools.cin_census_pipeline import stream_record
from liiatools.common import stream_filters as stream_functions
from liiatools.common.data import DataContainer, FileLocator, ProcessResult
from liiatools.common.stream_parse import dom_parse
from liiatools.common.stream_pipeline import to_dataframe_xml

from . import stream_filters as filters


def task_cleanfile(src_file: FileLocator, schema: (XMLSchema, Path), output_config: PipelineConfig, logger: Optional[logging.Logger]=None) -> ProcessResult:
    """
    Clean input cin census xml files according to schema and output clean data and errors
    :param src_file: The pointer to a file in a virtual filesystem
    :param schema: The data schema, and Path to the data schema
    :param logger: Optional logger to log messages
    :param output_config: Configuration for the output, imported as a PipelineConfig class
    :return: A class containing a DataContainer and ErrorContainer
    """
    schema, schema_path = schema
    with src_file.open("rb") as f:
        # Open & Parse file
        stream = dom_parse(f, filename=src_file.name)

        # Configure stream
        stream = stream_functions.strip_text(stream)
        stream = stream_functions.add_context(stream)
        stream = stream_functions.add_schema(stream, schema=schema)
        stream = filters.add_column_spec(stream, schema_path=schema_path)

        # Clean stream
        stream = stream_functions.log_blanks(stream)
        stream = stream_functions.conform_cell_types(stream)
        stream = stream_functions.validate_elements(stream)

        # Create dataset
        error_holder, stream = stream_functions.collect_errors(stream)
        stream = stream_record.message_collector(stream)
        dataset_holder, stream = stream_record.export_table(stream, output_config)

        # Consume stream so we know it's been processed
        generic.consume(stream)

        dataset = dataset_holder.value
        errors = error_holder.value

        dataset = DataContainer(
            {k: to_dataframe_xml(v, schema_path) for k, v in dataset.items()}
        )

    return ProcessResult(data=dataset, errors=errors)
