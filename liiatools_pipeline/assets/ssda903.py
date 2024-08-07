import logging
from dagster import asset
from decouple import config as env_config
from fs import open_fs

from liiatools.common.reference import authorities
from liiatools.ssda903_pipeline.spec import load_pipeline_config

logger = logging.getLogger(__name__)


@asset
def pipeline_config():
    return load_pipeline_config()


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
