import logging
from functools import lru_cache
from pathlib import Path

import xmlschema
import yaml
from pydantic_yaml import parse_yaml_file_as

from liiatools.common.data import PipelineConfig

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent


@lru_cache
def load_pipeline_config():
    with open(SCHEMA_DIR / "pipeline.json", "rt") as FILE:
        return parse_yaml_file_as(PipelineConfig, FILE)


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
