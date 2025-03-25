import importlib.resources
import logging
from functools import lru_cache
from pathlib import Path

from pydantic_yaml import parse_yaml_file_as
from ruamel.yaml import YAML

yaml = YAML()
yaml.preserve_quotes = True

from liiatools.common.data import PipelineConfig
from liiatools.common.spec import load_region_env
from liiatools.common.spec.__data_schema import DataSchema

__ALL__ = ["load_schema", "DataSchema", "Category", "Column"]

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent

region_config = load_region_env()


@lru_cache
def load_pipeline_config():
    """
    Load the pipeline config file
    :return: Parsed pipeline config file
    """
    try:
        with importlib.resources.open_text(
            f"{region_config}_pipeline_config", "pnw_census_pipeline.json"
        ) as f:
            return parse_yaml_file_as(PipelineConfig, f)
    except ModuleNotFoundError:
        logger.info(f"Configuration region '{region_config}' not found.")
    except FileNotFoundError:
        logger.info(
            f"Configuration file 'pnw_census_pipeline.json' not found in '{region_config}'."
        )


@lru_cache
def load_schema() -> DataSchema:
    """
    Load the data schema file
    :return: The data schema in a DataSchema class
    """
    schema_path = Path(SCHEMA_DIR, "pnw_census_schema.yml")

    # If we have no schema files, raise an error
    if not schema_path:
        raise ValueError(f"No schema files found")

    with open(schema_path, "r", encoding="utf-8") as file:
        full_schema = yaml.load(file)

    # Now we can parse the full schema into a DataSchema object from the dict
    return DataSchema(**full_schema)
