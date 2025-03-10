import logging
from functools import lru_cache
from pathlib import Path

from pydantic_yaml import parse_yaml_file_as
from ruamel.yaml import YAML

yaml = YAML()
yaml.preserve_quotes = True

from liiatools.common.data import PipelineConfig
from liiatools.common.spec.__data_schema import DataSchema

__ALL__ = ["load_schema", "DataSchema", "Category", "Column"]

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent


@lru_cache
def load_pipeline_config():
    """
    Load the pipeline config file
    :return: Parsed pipeline config file
    """
    with open(SCHEMA_DIR / "pipeline.json", "rt") as f:
        return parse_yaml_file_as(PipelineConfig, f)


@lru_cache
def load_schema() -> DataSchema:
    """
    Load the data schema file
    :return: The data schema in a DataSchema class
    """
    schema_path = Path(SCHEMA_DIR, "Annex_A_schema.yml")

    # If we have no schema files, raise an error
    if not schema_path:
        raise ValueError(f"No schema files found")

    with open(schema_path, "r", encoding="utf-8") as file:
        full_schema = yaml.load(file)

    # Now we can parse the full schema into a DataSchema object from the dict
    return DataSchema(**full_schema)
