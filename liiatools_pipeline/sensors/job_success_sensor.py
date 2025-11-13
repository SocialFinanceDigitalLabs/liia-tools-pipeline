from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunConfig,
    RunRequest,
    RunsFilter,
    sensor,
)
from decouple import config as env_config

from liiatools_pipeline.assets.common import pipeline_config
from liiatools_pipeline.jobs.annex_a_org import deduplicate_annex_a
from liiatools_pipeline.jobs.cin_org import cin_reports
from liiatools_pipeline.jobs.common_la import (
    clean,
    concatenate,
    move_current_la,
    no_op_job,
    start_clean_dataset,
)
from liiatools_pipeline.jobs.common_org import (
    move_concat,
    move_current_org,
    move_error_reports,
    reports,
)
from liiatools_pipeline.jobs.pnw_census_org import pnw_census_joins
from liiatools_pipeline.jobs.ssda903_la import ssda903_fix_episodes
from liiatools_pipeline.jobs.ssda903_org import ssda903_sufficiency
from liiatools_pipeline.ops.common_config import CleanConfig
from liiatools_pipeline.sensors.location_schedule import (
    input_directory_walker,
    check_la,
)


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
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
)
def move_current_la_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=clean.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    if run_records:  # Ensure there is at least one run record
        context.log.info(f"Run records found for clean job")
        latest_run_id = run_records[0].dagster_run.run_id  # Get the most recent run id
        context.log.info(f"Run key: {latest_run_id}")
        yield RunRequest(
            run_key=latest_run_id,
        )


@sensor(
    job=concatenate,
    description="Runs concatenate job once move_current job is complete",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
)
def concatenate_sensor(context):
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")
    context.log.info(f"Concatenate allowed datasets: {allowed_datasets}")

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
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
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
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
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
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
)
def move_current_org_sensor(context):
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")
    context.log.info(f"Move current allowed datasets: {allowed_datasets}")

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
        context.log.info(
            f"Run records found for reports job in move current org sensor"
        )
        for dataset in allowed_datasets:
            clean_config = CleanConfig(
                dataset_folder=None,
                la_folder=None,
                input_la_code=None,
                dataset=dataset,
            )
            sensor_trigger = pipeline_config(clean_config).sensor_trigger[
                "move_current_org_sensor"
            ]
            if sensor_trigger:
                context.log.info(f"Sensor trigger is enabled for {dataset}")
                latest_run_id = find_previous_matching_dataset_run(
                    run_records,
                    dataset,
                )  # Get the most recent dataset run id
                context.log.info(f"Run key: {latest_run_id}, dataset: {dataset}")

                yield RunRequest(
                    run_key=latest_run_id,
                    run_config=RunConfig(
                        ops={
                            "move_current_view_org": clean_config,
                        }
                    ),
                )
            else:
                context.log.info(
                    f"Sensor trigger is disabled for {dataset}, skipping move_current_org"
                )


@sensor(
    job=move_concat,
    description="Runs move_concat job once reports job is complete",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
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
            clean_config = CleanConfig(
                dataset_folder=None,
                la_folder=None,
                input_la_code=None,
                dataset=dataset,
            )
            sensor_trigger = pipeline_config(clean_config).sensor_trigger[
                "move_concat_sensor"
            ]
            if sensor_trigger:
                context.log.info(f"Sensor trigger is enabled for {dataset}")
                latest_run_id = find_previous_matching_dataset_run(
                    run_records,
                    dataset,
                )  # Get the most recent dataset run id
                context.log.info(f"Run key: {latest_run_id}, dataset: {dataset}")
                yield RunRequest(
                    run_key=latest_run_id,
                    run_config=RunConfig(
                        ops={
                            "move_concat_view": clean_config,
                        }
                    ),
                )
            else:
                context.log.info(
                    f"Sensor trigger is disabled for {dataset}, skipping move_concat"
                )


@sensor(
    job=ssda903_sufficiency,
    description="Runs ssda903_sufficiency job once reports job is complete",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
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


@sensor(
    job=deduplicate_annex_a,
    description="Runs deduplicate_annex_a job once reports job is complete",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
)
def deduplicate_annex_a_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=reports.name,
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"dataset": "annex_a"},
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    latest_run_id = find_previous_matching_dataset_run(
        run_records,
        "annex_a",
    )  # Get the most recent annex_a run id
    if latest_run_id:  # Ensure there is at least one annex_a run record
        context.log.info(f"Run key: {latest_run_id}")
        clean_config = CleanConfig(
            dataset_folder=None,
            la_folder=None,
            input_la_code=None,
            dataset="annex_a",
        )
        yield RunRequest(
            run_key=latest_run_id,
            run_config=RunConfig(
                ops={
                    "create_deduplicate_session_folder": clean_config,
                    "deduplicate_pan": clean_config,
                }
            ),
        )


@sensor(
    job=pnw_census_joins,
    description="Runs pnw_census_ssda903_join job once reports job is complete",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
)
def pnw_census_joins_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=reports.name,
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"dataset": ["ssda903", "pnw_census"]},
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    # Get the most recent ssda903 & pnw_census run ids
    latest_run_id_ssda903 = find_previous_matching_dataset_run(
        run_records,
        "ssda903",
    )
    latest_run_id_pnw = find_previous_matching_dataset_run(
        run_records,
        "pnw_census",
    )
    # Ensure there is at least one of each record
    if latest_run_id_ssda903 and latest_run_id_pnw:
        run_key = f"{latest_run_id_ssda903}_{latest_run_id_pnw}"
        context.log.info(f"Run key: {run_key}")
        yield RunRequest(
            run_key=run_key,
        )


@sensor(
    job=cin_reports,
    description="Runs cin_reports job once reports job is complete",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
)
def cin_reports_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=reports.name,
            statuses=[DagsterRunStatus.SUCCESS],
            tags={"dataset": "cin"},
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    latest_run_id = find_previous_matching_dataset_run(
        run_records,
        "cin",
    )  # Get the most recent cin run id
    if latest_run_id:  # Ensure there is at least one cin run record
        context.log.info(f"Run key: {latest_run_id}")
        yield RunRequest(
            run_key=latest_run_id,
        )


@sensor(
    job=clean,
    description="Runs clean across dataset",
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=int(env_config("SENSOR_MIN_INTERVAL")),
)
def full_clean_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=start_clean_dataset.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
        limit=1000,
    )

    if not run_records:
        context.log.info(f"No previous run records found for start_clean_dataset job")
        return

    dataset = run_records[0].dagster_run.tags["dataset"]
    context.log.info(f"Dataset found: {dataset}")
    
    folder_location = env_config("INPUT_LOCATION")
    context.log.info(f"Opening folder location: {folder_location}")

    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")
    context.log.info(f"Allowed datasets: {allowed_datasets}")

    if dataset not in allowed_datasets:
        context.log.info(
            f"Dataset {dataset} not in allowed datasets, skipping clean sensor"
        )
        return


    context.log.info("Analysing folder contents")
    directory_contents = input_directory_walker(folder_location, context, dataset)

    latest_run_id = run_records[0].dagster_run.run_id  # Get the most recent run id
    context.log.info(f"Run key: {latest_run_id}")

    for la_path, files in directory_contents.items():
        try:
            la = check_la(la_path)
        except ValueError:
            context.log.error(
                f"LA code not found in the directory path: {folder_location}/{la_path.split('-')[-1]}/{dataset}"
            )
            continue

        clean_config = CleanConfig(
            dataset_folder=f"{folder_location}/{la_path}/{dataset}",
            la_folder=f"{folder_location}/{la_path}",
            input_la_code=la,
            dataset=dataset,
        )
        
        yield RunRequest(
            run_key=latest_run_id,
            tags={"dataset": dataset},
            run_config=RunConfig(
                ops={
                    "create_session_folder": clean_config,
                    "open_current": clean_config,
                    "process_files": clean_config,
                }
            ),
        )

