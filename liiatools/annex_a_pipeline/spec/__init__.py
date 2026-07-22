import importlib.resources
import logging
from functools import lru_cache
from pathlib import Path

from pydantic_yaml import parse_yaml_file_as
from ruamel.yaml import YAML

from liiatools.common.data import PipelineConfig
from liiatools.common.file_header_readme_generator import (
    MarkdownSection,
    natural_sort_key,
    render_single_schema_readme,
    write_markdown_file,
)
from liiatools.common.spec import load_region_env
from liiatools.common.spec.__data_schema import DataSchema

yaml = YAML()
yaml.preserve_quotes = True

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
            f"{region_config}_pipeline_config", "annex_a_pipeline.json"
        ) as f:
            return parse_yaml_file_as(PipelineConfig, f)
    except ModuleNotFoundError:
        logger.info(f"Configuration region '{region_config}' not found.")
    except FileNotFoundError:
        logger.info(
            f"Configuration file 'annex_a_pipeline.json' not found in '{region_config}'."
        )


@lru_cache
def load_schema() -> DataSchema:
    """
    Load the data schema file
    :return: The data schema in a DataSchema class
    """
    schema_path = Path(SCHEMA_DIR, "Annex_A_schema.yml")

    # If we have no schema files, raise an error
    if not schema_path:
        raise ValueError("No schema files found")

    with open(schema_path, "r", encoding="utf-8") as file:
        full_schema = yaml.load(file)

    # Now we can parse the full schema into a DataSchema object from the dict
    return DataSchema(**full_schema)


def render_schema_header_readme() -> str:
    schema = load_schema()
    table_names = sorted(schema.table.keys(), key=natural_sort_key)
    sections = [
        MarkdownSection(summary=table_name, items=list(schema.table[table_name].keys()))
        for table_name in table_names
    ]

    return render_single_schema_readme(
        title="Annex A Expected Headers",
        intro_lines=["This dataset has a single schema. Expand each table to see required headers."],
        sections=sections,
    )


def generate_schema_header_readme(
    output_file: Path | None = None,
) -> Path:
    if output_file is None:
        output_file = Path("docs", "file_headers_by_dataset", "annex_a_headers.md")

    readme_content = render_schema_header_readme()
    return write_markdown_file(output_file, readme_content)