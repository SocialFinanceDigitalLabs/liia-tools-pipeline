from dagster import (
    RunRequest,
    RunsFilter,
    DagsterRunStatus,
    sensor,
    DefaultSensorStatus,
    RunConfig,
)
from decouple import config as env_config

from liiatools_pipeline.jobs.common_la import clean, move_current_la, concatenate
from liiatools_pipeline.jobs.ssda903_la import ssda903_fix_episodes
from liiatools_pipeline.jobs.common_org import (
    move_error_reports,
    move_current_org,
    move_concat,
    reports,
)
from liiatools_pipeline.jobs.ssda903_org import ssda903_sufficiency
from liiatools_pipeline.ops.common_config import CleanConfig


def find_previous_matching_dataset_run(run_records, dataset):
    previous_run_id = [
        run.dagster_run.run_id
        for run in run_records
        if dataset == run.dagster_run.tags["dataset"]
    ]

    previous_run_id = previous_run_id[0] if previous_run_id else None
    return previous_run_id


@sensor(
    job=move_current_la,
    description="Runs move_current_la job once clean job is complete",
    default_status=DefaultSensorStatus.RUNNING,
)
def move_current_la_sensor(context):
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")
    context.log.info(f"Move current la allowed datasets: {allowed_datasets}")

    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=clean.name,
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"dataset": allowed_datasets},
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    if run_records:  # Ensure there is at least one run record
        context.log.info(f"Run records found for clean job")
        for dataset in allowed_datasets:
            latest_run_id = find_previous_matching_dataset_run(
                run_records,
                dataset,
            )  # Get the most recent dataset run id
            context.log.info(f"Run key: {latest_run_id}, dataset: {dataset}")
            yield RunRequest(
                run_key=latest_run_id,
                tags={"dataset": dataset},
            )


@sensor(
    job=concatenate,
    description="Runs concatenate job once move_current job is complete",
    default_status=DefaultSensorStatus.RUNNING,
)
def concatenate_sensor(context):
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")
    context.log.info(f"Concatenate allowed datasets: {allowed_datasets}")

    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=move_current_la.name,
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"dataset": allowed_datasets},
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    if run_records:  # Ensure there is at least one run record
        context.log.info(f"Run records found for move_current_la job")
        for dataset in allowed_datasets:
            latest_run_id = find_previous_matching_dataset_run(
                run_records,
                dataset,
            )  # Get the most recent dataset run id
            context.log.info(f"Run key: {latest_run_id}, dataset: {dataset}")
            concat_config = CleanConfig(
                dataset=dataset,
            )
            yield RunRequest(
                run_key=latest_run_id,
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
)
def ssda903_fix_episodes_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=concatenate.name,
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"dataset": "ssda903"},
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    latest_run_id = find_previous_matching_dataset_run(
        run_records,
        "ssda903",
    )  # Get the most recent ssda903 run id
    context.log.info(f"Run key: {latest_run_id}")
    if latest_run_id:  # Ensure there is at least one ssda903 run record
        yield RunRequest(run_key=latest_run_id, run_config=RunConfig())


@sensor(
    job=move_error_reports,
    description="Runs move_error_reports job once reports job is complete",
    default_status=DefaultSensorStatus.RUNNING,
)
def move_error_reports_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=reports.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    if run_records:  # Ensure there is at least one run record
        context.log.info(
            f"Run records found for reports job in move error reports sensor"
        )
        latest_run_record = run_records[0]  # Get the most recent run record
        context.log.info(f"Run key: {latest_run_record.dagster_run.run_id}")
        yield RunRequest(
            run_key=latest_run_record.dagster_run.run_id,
        )


@sensor(
    job=move_current_org,
    description="Runs move_current_org job once reports job is complete",
    default_status=DefaultSensorStatus.RUNNING,
)
def move_current_org_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=reports.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    if run_records:  # Ensure there is at least one run record
        context.log.info(f"Run records found for reports job")
        latest_run_record = run_records[0]
        context.log.info(f"Run key: {latest_run_record.dagster_run.run_id}")
        yield RunRequest(
            run_key=latest_run_record.dagster_run.run_id,
        )


@sensor(
    job=move_concat,
    description="Runs move_concat job once reports job is complete",
    default_status=DefaultSensorStatus.RUNNING,
)
def move_concat_sensor(context):
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")
    context.log.info(f"Move concat allowed datasets: {allowed_datasets}")

    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=reports.name,
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"dataset": allowed_datasets},
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    if run_records:  # Ensure there is at least one run record
        context.log.info(f"Run records found for reports job in move concat sensor")
        for dataset in allowed_datasets:
            latest_run_id = find_previous_matching_dataset_run(
                run_records,
                dataset,
            )  # Get the most recent dataset run id
            context.log.info(f"Run key: {latest_run_id}, dataset: {dataset}")
            clean_config = CleanConfig(
                dataset_folder=None,
                la_folder=None,
                input_la_code=None,
                dataset=dataset,
            )
            yield RunRequest(
                run_key=latest_run_id,
                run_config=RunConfig(
                    ops={
                        "move_concat_view": clean_config,
                    }
                ),
            )


@sensor(
    job=ssda903_sufficiency,
    description="Runs ssda903_sufficiency job once reports job is complete",
    default_status=DefaultSensorStatus.RUNNING,
)
def sufficiency_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=reports.name,
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"dataset": "ssda903"},
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    latest_run_id = find_previous_matching_dataset_run(
        run_records,
        "ssda903",
    )  # Get the most recent ssda903 run id
    if latest_run_id:  # Ensure there is at least one ssda903 run record
        context.log.info(f"Run key: {latest_run_id}")
        yield RunRequest(
            run_key=latest_run_id,
        )
