from dagster import (
    RunRequest,
    RunsFilter,
    DagsterRunStatus,
    sensor,
    DefaultSensorStatus,
    RunConfig,
)
from liiatools_pipeline.jobs.common_la import clean, move_current, concatenate
from liiatools_pipeline.jobs.ssda903_la import ssda903_fix_episodes
from liiatools_pipeline.ops.common_config import CleanConfig


def find_previous_matching_ssda903_run(run_records):
    previous_run_id = [
        run.dagster_run.run_id
        for run in run_records
        if "ssda903" == run.dagster_run.tags["dataset"]
    ]

    previous_run_id = previous_run_id[0]
    return previous_run_id


@sensor(
    job=move_current,
    description="Runs move_current job once clean job is complete",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=5,
)
def move_current_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=clean.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
    )

    if run_records:  # Ensure there is at least one run record
        latest_run_record = run_records[0]  # Get the most recent run record
        dataset = latest_run_record.dagster_run.run_config["ops"]["process_files"][
            "config"
        ]["dataset"]
        yield RunRequest(
            run_key=latest_run_record.dagster_run.run_id,
            tags={"dataset": dataset},
        )


@sensor(
    job=concatenate,
    description="Runs concatenate job once move_current job is complete",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=5,
)
def concatenate_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=move_current.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
    )

    if run_records:  # Ensure there is at least one run record
        latest_run_record = run_records[0]  # Get the most recent run record
        dataset = latest_run_record.dagster_run.tags["dataset"]
        concat_config = CleanConfig(
            dataset=dataset,
        )
        yield RunRequest(
            run_key=latest_run_record.dagster_run.run_id,
            run_config=RunConfig(
                ops={
                    "open_current": concat_config,
                    "create_concatenated_view": concat_config,
                }
            ),
            tags={"dataset": dataset},
        )


@sensor(
    job=ssda903_fix_episodes,
    description="Runs ssda903_fix_episodes job once concatenate job is complete",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=5,
)
def ssda903_fix_episodes_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=concatenate.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
    )

    if run_records:  # Ensure there is at least one run record
        latest_run_id = find_previous_matching_ssda903_run(
            run_records
        )  # Get the most recent ssda903 run record
        yield RunRequest(run_key=latest_run_id, run_config=RunConfig())
