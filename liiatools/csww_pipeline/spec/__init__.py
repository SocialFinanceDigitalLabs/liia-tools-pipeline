import importlib.resources
import logging
from functools import lru_cache
from pathlib import Path

import xmlschema
import yaml
from pydantic_yaml import parse_yaml_file_as

from liiatools.common.data import PipelineConfig
from liiatools.common.spec import load_region_env

# initialise a logger 
# A logger sends to the javascript terminal what you're doing 
# You can specify extra things you want to say
logger = logging.getLogger(__name__)

# Directory for where schema is
SCHEMA_DIR = Path(__file__).parent

# env is an environment
# load_region_env() is a function
region_config = load_region_env()


# @lru_cache is a decorator
# lru - least recently used
@lru_cache
def load_pipeline_config():
    # try - try to run this code
    # If there's an issue then we'll get an error that we've written and
    # makes sense to the reader
    # except - to do with errors
    # with - setting up a temporary variable name
    # as - ...
    # Find and use region's pipeline (e.g. London's)
        try:
            with importlib.resources.open_text(
                # f - format; lets you put variables in a string
                # You can concatenate like below or using + as we've learnt b4
                f"{region_config}_pipeline_config", "csww_pipeline.json"
                # f stands for resulting outcome from 2 lines of code above
            ) as f:
            # Now need a return statement
            # Use whatever came out of f in return statement
                return parse_yaml_file_as(PipelineConfig, f)
                # 2 potential errors 
                # 1. region_config not found
                # 2. json file not found
                # Tell the user what exactly the error(s) is/are
                # You can use single quotes '' in double quotes ""
        except ModuleNotFoundError:
            logger.info(f"Configuration region '{region_config}' not found.")
        except FileNotFoundError:
            logger.info(
                f"Configuration file 'cin_census_pipeline.json' not found in '{region_config}'."
            )


# Decorate the following with lru_cache
@lru_cache
# Load the schema from a year (it has to be an integer - if it isn't then tell
# them that they're wrong)
# xml schema bits are not from Social Finance
#XMLSchema is a class as it's uppercase; like functions; we don't run it
# df is a class
# Path is a path to a file and is a class as well
def load_schema(year: int) -> (xmlschema.XMLSchema, Path):
    return (
        # 04d checks it's a 4 digit year
        xmlschema.XMLSchema(SCHEMA_DIR / f"csww_schema_{year:04d}.xsd"),
        # Use Path function from library to join
        Path(SCHEMA_DIR, f"csww_schema_{year:04d}.xsd"),
    )


@lru_cache
def load_reports():
    # with open - ...
    #rt - read and write (when file is open)
    # Open the file and read and write on it
    # FILE - temporary file (when it's open)
    # Whatever comes out of load function, use yaml loader
    with open(SCHEMA_DIR / "reports.yml", "rt") as FILE:
        return yaml.load(FILE, Loader=yaml.FullLoader)