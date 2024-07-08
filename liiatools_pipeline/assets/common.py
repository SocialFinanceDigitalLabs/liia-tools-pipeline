from dagster import asset
from decouple import config as env_config
from fs import open_fs
from liiatools.cin_census_pipeline.spec import load_pipeline_config as load_pipeline_config_cin
from liiatools.ssda903_pipeline.spec import load_pipeline_config as load_pipeline_config_ssda903
from liiatools.common.reference import authorities

import logging
logger = logging.getLogger(__name__)


@asset
def dataset():
    dataset = env_config("DATASET", cast=str)
    return dataset


@asset
def pipeline_config():
    # add argument that will allow either 903 or CIN
    dataset = env_config("DATASET", cast=str)
    if dataset == "cin":
        return load_pipeline_config_cin()
    elif dataset == "ssda903":
        return load_pipeline_config_ssda903()
    # leave else statement for errors
    else:
        logger.info("Dataset specified isn't valid. Defaulting to None")
        dataset = None
        return dataset
# add log to check what return load_pipeline_config gives


@asset
def process_folder():
    output_location = env_config("OUTPUT_LOCATION", cast=str)
    return open_fs(output_location)


@asset
def incoming_folder():
    input_location = env_config("INPUT_LOCATION", cast=str)
    return open_fs(input_location)


@asset
def workspace_folder():
    workspace_location = env_config("WORKSPACE_LOCATION", cast=str)
    return open_fs(workspace_location)


@asset
def shared_folder():
    shared_location = env_config("SHARED_LOCATION", cast=str)
    return open_fs(shared_location)


@asset
def la_code():
    input_la_code = env_config("LA_CODE", cast=str, default=None)
    if input_la_code is not None and input_la_code not in authorities.codes:
        logger.info("LA code specified isn't valid. Defaulting to None")
        input_la_code = None
    return input_la_code