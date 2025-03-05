import importlib.resources
import logging
from decouple import config as env_config
from functools import lru_cache
from pathlib import Path

import xmlschema
import yaml
from pydantic_yaml import parse_yaml_file_as

from liiatools.common.data import PipelineConfig

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent

region_config = env_config("REGION_CONFIG", cast=str)


@lru_cache
def load_pipeline_config():
    try:
        with importlib.resources.open_text(
                f"{region_config}_pipeline_config", "cin_census_pipeline.json"
        ) as f:
            return parse_yaml_file_as(PipelineConfig, f)
    except ModuleNotFoundError:
        logger.info(f"Configuration region '{region_config}' not found.")
    except FileNotFoundError:
        logger.info(f"Configuration file 'cin_census_pipeline.json' not found in '{region_config}'.")


@lru_cache
def load_schema(year: int) -> (xmlschema.XMLSchema, Path):
    return (
        xmlschema.XMLSchema(SCHEMA_DIR / f"CIN_schema_{year:04d}.xsd"),
        Path(SCHEMA_DIR, f"CIN_schema_{year:04d}.xsd"),
    )


@lru_cache
def load_reports():
    with open(SCHEMA_DIR / "reports.yml", "rt") as FILE:
        return yaml.load(FILE, Loader=yaml.FullLoader)
