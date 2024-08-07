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

from liiatools_pipeline.jobs.ssda903_la import ssda903_clean
from liiatools_pipeline.ops.ssda903_la import FileConfig
from liiatools.common.checks import check_la


def directory_walker(folder_location, wildcards, context):
    """
    Walks through the specified directory and returns a dictionary
    of files for each LA
    """
    walker = Walker(filter=wildcards)
    dir_pointer = open_fs(folder_location)
    directories = dir_pointer.listdir("/")
    dir_contents = {}

    for directory in directories:
        la_directory = open_fs(f"{folder_location}/{directory}/ssda903")
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
    job=ssda903_clean,
    cron_schedule="0 0 * * *",
    description="Monitors specified location for 903 files every day at midnight",
)
def ssda903_schedule(context):
    folder_location = env_config("INPUT_LOCATION")
    context.log.info(f"Opening folder location: {folder_location}")

    wildcards = env_config("903_WILDCARDS").split(",")

    context.log.info("Analysing folder contents")
    directory_contents = directory_walker(folder_location, wildcards, context)

    for la_path, files in directory_contents.items():
        context.log.info("Generating Run Key")
        run_key = generate_run_key(f"{folder_location}/{la_path}/ssda903", files)

        run_records = context.instance.get_run_records(
            filters=RunsFilter(
                job_name=ssda903_clean.name,
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
                "incoming_folder"
            ]
        ]

        previous_run_id = previous_run_id[0] if previous_run_id else []

        previous_matching_run_id = (
            previous_run_id if run_key == previous_run_id else None
        )

        la = check_la(la_path)

        file_config = FileConfig(
            incoming_folder=f"{folder_location}/{la_path}/ssda903",
            input_la_code=la
        )

        if previous_matching_run_id is None:
            context.log.info("Differences found, executing run")
            yield RunRequest(
                run_key=run_key,
                run_config=RunConfig(
                    ops={
                        "create_session_folder": file_config,
                        "process_files": file_config
                    }
                ),
            )
        else:
            context.log.info("No new files found, skipping run")
