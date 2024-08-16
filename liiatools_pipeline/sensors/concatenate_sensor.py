from dagster import (
    RunRequest,
    RunsFilter,
    DagsterRunStatus,
    sensor,
    DefaultSensorStatus,
    RunConfig,
)
from decouple import config as env_config
from fs import open_fs

from liiatools_pipeline.jobs.common_la import move_current, concatenate
from liiatools_pipeline.ops.common_config import CleanConfig
from liiatools.common.checks import check_dataset

from dagster import get_dagster_logger

log = get_dagster_logger(__name__)


@sensor(
    job=concatenate,
    description="Runs concatenate job once move_current job is complete",
    default_status=DefaultSensorStatus.RUNNING,
)
def concatenate_sensor(context):

    folder_location = env_config("WORKSPACE_LOCATION")
    
    dir_pointer = open_fs(folder_location)
    context.log.info(folder_location)

    # dataset = check_dataset(folder_location)
    # concat_config = CleanConfig(
    #     dataset=dataset,
    # )
    #
    # run_records = context.instance.get_run_records(
    #     filters=RunsFilter(
    #         job_name=move_current.name,
    #         statuses=[DagsterRunStatus.SUCCESS],
    #     ),
    #     order_by="update_timestamp",
    #     ascending=False,
    # )
    #
    # if run_records:  # Ensure there is at least one run record
    #     latest_run_record = run_records[0]  # Get the most recent run record
    #     yield RunRequest(
    #         run_key=latest_run_record.dagster_run.run_id,
    #         run_config=RunConfig(
    #             ops={
    #                 "open_current": concat_config,
    #                 "create_concatenated_view": concat_config,
    #             }
    #         )
    #     )
