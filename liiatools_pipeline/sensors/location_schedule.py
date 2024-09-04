from hashlib import sha1

import fs.errors
from dagster import (
    RunRequest,
    RunConfig,
    schedule,
    RunsFilter,
    DagsterRunStatus,
    DefaultScheduleStatus,
)
from fs import open_fs
from fs.walk import Walker
from decouple import config as env_config

from liiatools_pipeline.jobs.common_la import clean
from liiatools_pipeline.jobs.common_org import reports
from liiatools_pipeline.ops.common_config import CleanConfig
from liiatools.common.checks import check_la


def input_directory_walker(folder_location, context, dataset):
    """
    Walks through the specified directory and returns a dictionary
    of files for each LA
    """
    walker = Walker()
    dir_pointer = open_fs(folder_location)
    directories = dir_pointer.listdir("/")
    dir_contents = {}

    for directory_str in directories:
        directory = open_fs(f"{folder_location}/{directory_str}/{dataset}")
        dir_contents[directory_str] = [
            file.lstrip("/") for file in walker.files(directory)
        ]

        if not dir_contents[directory_str]:
            context.log.info(f"No files in {folder_location} have been found")

    return dir_contents


def concat_directory_walker(folder_location, context, dataset):
    walker = Walker()
    try:
        concat_folder = open_fs(f"{folder_location}/concatenated/{dataset}")
        context.log.info(f"Opening folder location: {folder_location}")
        context.log.info("Analysing folder contents")
        dir_contents = [file.lstrip("/") for file in walker.files(concat_folder)]

    except fs.errors.CreateFailed:
        context.log.info(
            f"Failed to open folder location: {folder_location}/concatenated/{dataset}"
        )
        dir_contents = None

    return dir_contents


def generate_run_key(folder_location, files):
    """
    Generate a hash based on the last modified timestamps of the files.
    """
    hash_object = sha1()

    for file_path in files:
        with open_fs(folder_location) as filesystem:
            last_modified_time = filesystem.getinfo(
                file_path, namespaces=["details"]
            ).modified

            hash_object.update(f"{last_modified_time}_{folder_location}".encode())

    return hash_object.hexdigest()


def find_previous_matching_run(
    run_records, run_key, la_path, dataset, key_op, key_folder, context
):
    try:
        previous_run_id = [
            run.dagster_run.tags["dagster/run_key"]
            for run in run_records
            if all(
                var in run.dagster_run.run_config["ops"][key_op]["config"][key_folder]
                for var in (la_path, dataset)
            )
        ]

    except KeyError as e:
        if "dagster/run_key" in str(e):
            context.log.error(
                f"dagster/run_key not found in tags. No previous run key found: {run_records[0].dagster_run.tags}"
            )
        elif key_op in str(e):
            context.log.error(
                f"{key_op} not found in run_config. No previous run config found: {run_records[0].dagster_run.run_config}"
            )
        elif key_folder in str(e):
            context.log.error(
                f"{key_folder} not found in run_config. No previous run config found: {run_records[0].dagster_run.run_config}"
            )

        previous_run_id = None

    previous_run_id = previous_run_id[0] if previous_run_id else []
    previous_matching_run_id = previous_run_id if run_key == previous_run_id else None
    return previous_matching_run_id


@schedule(
    job=clean,
    cron_schedule=env_config("CLEAN_SCHEDULE"),
    description="Monitors specified location for data files according to the CLEAN_SCHEDULE environment variable",
    default_status=DefaultScheduleStatus.RUNNING,
)
def clean_schedule(context):
    folder_location = env_config("INPUT_LOCATION")
    context.log.info(f"Opening folder location: {folder_location}")

    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")
    context.log.info(f"Allowed datasets: {allowed_datasets}")
    for dataset in allowed_datasets:
        context.log.info("Analysing folder contents")
        directory_contents = input_directory_walker(folder_location, context, dataset)

        for la_path, files in directory_contents.items():
            context.log.info("Generating Run Key")
            run_key = generate_run_key(f"{folder_location}/{la_path}/{dataset}", files)

            run_records = context.instance.get_run_records(
                filters=RunsFilter(
                    job_name=clean.name,
                    statuses=[DagsterRunStatus.SUCCESS],
                ),
                order_by="update_timestamp",
                ascending=False,
            )

            previous_matching_run_id = find_previous_matching_run(
                run_records,
                run_key,
                la_path,
                dataset,
                "create_session_folder",
                "dataset_folder",
                context,
            )
            la = check_la(la_path)

            clean_config = CleanConfig(
                dataset_folder=f"{folder_location}/{la_path}/{dataset}",
                la_folder=f"{folder_location}/{la_path}",
                input_la_code=la,
                dataset=dataset,
            )

            if previous_matching_run_id is None:
                context.log.info("Differences found, executing run")
                yield RunRequest(
                    run_key=run_key,
                    run_config=RunConfig(
                        ops={
                            "create_session_folder": clean_config,
                            "open_current": clean_config,
                            "process_files": clean_config,
                        }
                    ),
                )
            else:
                context.log.info("No new files found, skipping run")


@schedule(
    job=reports,
    cron_schedule=env_config("REPORTS_SCHEDULE"),
    description="Monitors specified location for data files according to the REPORTS_SCHEDULE environment variable",
    default_status=DefaultScheduleStatus.RUNNING,
)
def reports_schedule(context):
    folder_location = env_config("INPUT_LOCATION")
    context.log.info(f"Opening folder location: {folder_location}")

    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")
    context.log.info(f"Allowed datasets: {allowed_datasets}")
    for dataset in allowed_datasets:
        context.log.info("Analysing folder contents")
        files = concat_directory_walker(folder_location, context, dataset)

        context.log.info("Generating Run Key")
        run_key = generate_run_key(f"{folder_location}/concatenated/{dataset}", files)

        run_records = context.instance.get_run_records(
            filters=RunsFilter(
                job_name=reports.name,
                statuses=[DagsterRunStatus.SUCCESS],
            ),
            order_by="update_timestamp",
            ascending=False,
        )

        previous_matching_run_id = find_previous_matching_run(
            run_records,
            run_key,
            "",
            dataset,
            "create_org_session_folder",
            "dataset",
            context,
        )

        clean_config = CleanConfig(
            dataset_folder=None,
            la_folder=None,
            input_la_code=None,
            dataset=dataset,
        )

        if previous_matching_run_id is None:
            context.log.info("Differences found, executing run")
            yield RunRequest(
                run_key=run_key,
                run_config=RunConfig(
                    ops={
                        "create_org_session_folder": clean_config,
                        "create_reports": clean_config,
                    }
                ),
            )
        else:
            context.log.info("No new files found, skipping run")