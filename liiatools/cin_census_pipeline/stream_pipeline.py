import logging
from pathlib import Path
from typing import Optional

from dagster import get_dagster_logger
from sfdata_stream_parser.filters import generic
from xmlschema import XMLSchema

from liiatools.cin_census_pipeline import stream_record
from liiatools.common import stream_filters as stream_functions
from liiatools.common.data import (
    DataContainer,
    FileLocator,
    PipelineConfig,
    ProcessResult,
)
from liiatools.common.stream_parse import dom_parse
from liiatools.common.stream_pipeline import to_dataframe_xml

from . import stream_filters as filters

log = get_dagster_logger(__name__)


def task_cleanfile(
    src_file: FileLocator,
    schema: (XMLSchema, Path),
    output_config: PipelineConfig,
    logger: Optional[logging.Logger] = None,
) -> ProcessResult:
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
        log.info("Cin file opened and parsed, beginning processing")

        # Configure stream
        stream = stream_functions.strip_text(stream)
        log.info("Stream text stripped of whitespace")
        stream = stream_functions.add_context(stream)
        log.info("Stream context added")
        stream = stream_functions.add_schema(stream, schema=schema)
        log.info("Stream schema added")
        stream = filters.add_column_spec(stream, schema_path=schema_path)
        log.info("Stream column specifications added")

        # Clean stream
        stream = stream_functions.log_blanks(stream)
        log.info("Stream blanks logged")
        stream = stream_functions.conform_cell_types(stream)
        log.info("Stream cell types conformed")
        stream = stream_functions.validate_elements(stream)
        log.info("Stream elements validated")

        # Create dataset
        error_holder, stream = stream_functions.collect_errors(stream)
        log.info("Stream errors collected")
        stream = stream_record.message_collector(stream)
        log.info("Stream messages collected")
        dataset_holder, stream = stream_record.export_table(stream, output_config)
        log.info("Stream dataset exported")

        # Consume stream so we know it's been processed
        generic.consume(stream)
        log.info("Stream consumed")

        dataset = dataset_holder.value
        if not dataset:
            log.info("No dataset created from file")
        errors = error_holder.value
        if not errors:
            log.info("No errors collected from file")

        dataset = DataContainer(
            {k: to_dataframe_xml(v, schema_path) for k, v in dataset.items()}
        )
        log.info("Dataset converted to DataContainer")

    return ProcessResult(data=dataset, errors=errors)
