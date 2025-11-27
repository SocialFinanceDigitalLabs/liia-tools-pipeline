import logging

from dagster import asset
from decouple import config as env_config
from fs import open_fs

from liiatools.annex_a_pipeline.spec import (
    load_pipeline_config as load_pipeline_config_annex_a,
)
from liiatools.cans_pipeline.spec import (
    load_pipeline_config as load_pipeline_config_cans,
)
from liiatools.cin_census_pipeline.spec import (
    load_pipeline_config as load_pipeline_config_cin,
)
from liiatools.pnw_census_pipeline.spec import (
    load_pipeline_config as load_pipeline_config_pnw_census,
)
from liiatools.ssda903_pipeline.spec import (
    load_pipeline_config as load_pipeline_config_ssda903,
)
from liiatools.csww_pipeline.spec import (
    load_pipeline_config as load_pipeline_config_csww,
)
from liiatools_pipeline.ops.common_config import CleanConfig

logger = logging.getLogger(__name__)


@asset
def pipeline_config(config: CleanConfig):
    try:
        return globals()[f"load_pipeline_config_{config.dataset}"]()
    except KeyError:
        logger.info(
            f"Dataset specified: {config.dataset} isn't valid. Defaulting to None"
        )
        return None


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
