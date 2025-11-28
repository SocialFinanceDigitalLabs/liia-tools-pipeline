import importlib.resources
import logging
import re
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic_yaml import parse_yaml_file_as

from liiatools.common.data import PipelineConfig
from liiatools.common.spec import load_region_env
from liiatools.common.spec.__data_schema import DataSchema

__ALL__ = ["load_schema", "DataSchema", "Category", "Column"]

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent

region_config = load_region_env()


@lru_cache
def load_pipeline_config():
    try:
        with importlib.resources.open_text(
            f"{region_config}_pipeline_config", "school_census_pipeline.json"
        ) as f:
            return parse_yaml_file_as(PipelineConfig, f)
    except ModuleNotFoundError:
        logger.info(f"Configuration region '{region_config}' not found.")
    except FileNotFoundError:
        logger.info(
            f"Configuration file 'school_census_pipeline.json' not found in '{region_config}'."
        )


@lru_cache
def load_schema(year: int, term: str) -> DataSchema:
    pattern = re.compile(r"school_census_schema_(\d{4})(\.diff)?\.yml")

    # Build index of all schema files
    all_schema_files = list(SCHEMA_DIR.glob("school_census_schema_*.yml"))
    all_schema_files.sort()
    schema_lookup = []
    for fn in all_schema_files:
        match = pattern.match(fn.name)
        assert match, f"Unexpected schema name {fn}"
        schema_lookup.append((fn, int(match.group(1)), match.group(2) is not None))

    # Filter only those earlier than the year we're looking for
    schema_lookup = [x for x in schema_lookup if x[1] <= year]

    # If we have no schema files, raise an error
    if not schema_lookup:
        raise ValueError(f"No schema files found for year {year}")

    # Find the latest complete schema
    last_complete_schema = [x for x in schema_lookup if not x[2]][-1]

    # Now filter down to only include last complete and any diff files after that
    schema_lookup = [x for x in schema_lookup if x[1] >= last_complete_schema[1]]

    # We load the full schema
    logger.debug("Loading schema from %s", schema_lookup[0][0])
    full_schema = yaml.safe_load(schema_lookup[0][0].read_text())
    try:
        term_schema = full_schema[term]
    except KeyError as e:
        raise KeyError(f"{term} not found in {full_schema}") from e

    # Now loop over diff files and apply them
    for fn, _, _ in schema_lookup[1:]:
        logger.debug("Loading partial schema from %s", fn)
        try:
            diff = yaml.safe_load(fn.read_text())
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing diff file {fn}") from e

        # If term not in diff file, continue to next, otherwise load term
        if term not in diff:
            logger.debug(f"{term} not present in {fn}")
            continue
        diff = diff[term]

        for key, diff_obj in diff.items():
            diff_type = diff_obj["type"]
            if diff_type not in ("add", "modify", "rename", "remove"):
                raise ValueError(f"Unknown diff type {diff_type} in {fn}")
            path = key.split(".")
            parent = term_schema

            if diff_type in ["add", "modify"]:
                try:
                    for item in path[:-1]:
                        parent = parent[item]
                    parent[path[-1]] = diff_obj["value"]
                except KeyError as e:
                    raise KeyError(f"while applying {diff_type} in {fn} for {key}: {repr(e)}") from e

            elif diff_type == "rename":
                try:
                    for item in path[:-1]:
                        parent = parent[item]
                    parent[diff_obj["value"]] = parent.pop(path[-1])
                except KeyError as e:
                    raise KeyError(f"while renaming {key} in {fn}: {repr(e)}") from e

            elif diff_type == "remove":
                if len(path) == 2:  # Remove columns
                    try:
                        parent = parent[path[0]][path[1]]
                        for k in diff_obj["value"]:
                            if k in parent:
                                parent.pop(k)
                            else:
                                logger.debug(f"{k} not found under path")
                    except KeyError as e:
                        raise KeyError(f"while removing columns at {key} in {fn}: {repr(e)}") from e
                elif len(path) == 1:  # Remove files
                    try:
                        parent = parent[path[0]]
                        for k in diff_obj["value"]:
                            if k in parent:
                                parent.pop(k)
                            else:
                                logger.debug(f"{k} not found under path")
                    except KeyError as e:
                        raise KeyError(f"While removing file at {key} in {fn}: {repr(e)}") from e
                else:
                    logger.debug(f"remove diff {key} has length {len(path)}")

    # Now we can parse the full schema into a DataSchema object from the dict
    return DataSchema(**term_schema)
