from dagster import op, In, Out, get_dagster_logger
from fs.base import FS

from liiatools.common.aggregate import DataframeAggregator
from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesOrg
from liiatools.common.reference import authorities
from liiatools.common.transform import prepare_export, apply_retention
from liiatools_pipeline.ops.common_config import CleanConfig

from liiatools_pipeline.assets.common import (
    pipeline_config,
    incoming_folder,
    workspace_folder,
    shared_folder,
)

log = get_dagster_logger()


@op()
def deduplicate_pan():
    pass

