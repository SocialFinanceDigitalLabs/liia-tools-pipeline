from hashlib import sha1
from dagster import (
    RunRequest,
    RunConfig,
    schedule,
    RunsFilter,
    DagsterRunStatus,
)
from fs import open_fs
from fs.walk import Walker
from decouple import config as env_config

from liiatools_pipeline.jobs.common_la import clean
from liiatools_pipeline.ops.common_config import CleanConfig
from liiatools.common.checks import check_la


def directory_walker(folder_location, context, dataset):
    """
    Walks through the specified directory and returns a dictionary
    of files for each LA
    """
    walker = Walker()
    dir_pointer = open_fs(folder_location)
    directories = dir_pointer.listdir("/")
    dir_contents = {}

    for directory in directories:
        la_directory = open_fs(f"{folder_location}/{directory}/{dataset}")
        dir_contents[directory] = [
            file.lstrip("/") for file in walker.files(la_directory)
        ]

        if not dir_contents[directory]:
            context.log.info(f"No files in {folder_location} have been found")

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

            hash_object.update(str(last_modified_time).encode())

    return hash_object.hexdigest()


@schedule(
    job=clean,
    cron_schedule="* * * * *",
    description="Monitors specified location for 903 files every day at midnight",
)
def ssda903_schedule(context):
    dataset = "ssda903"
    folder_location = env_config("INPUT_LOCATION")
    context.log.info(f"Opening folder location: {folder_location}")

    context.log.info("Analysing folder contents")
    directory_contents = directory_walker(folder_location, context, dataset)

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

        previous_run_id = [
            run.dagster_run.tags["dagster/run_key"]
            for run in run_records
            if la_path
            in run.dagster_run.run_config["ops"]["create_session_folder"]["config"][
                "dataset_folder"
            ]
        ]

        previous_run_id = previous_run_id[0] if previous_run_id else []

        previous_matching_run_id = (
            previous_run_id if run_key == previous_run_id else None
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
