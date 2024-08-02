from dagster import SkipReason
from hashlib import sha1
from fs import open_fs
from fs.walk import Walker
from dagster import (
    RunRequest,
    RunsFilter,
    DagsterRunStatus,
    sensor,
    DefaultSensorStatus,
)
from liiatools_pipeline.jobs.common_la import move_current, concatenate, clean


def directory_walker(dir_pointer, wildcards):
    """
    Walks through the specified directory and returns a list
    of files
    """
    walker = Walker(filter=wildcards)
    dir_contents = walker.files(dir_pointer)
    if not dir_contents:
        return SkipReason("No new files in {} have been found".format())
    return dir_contents


def open_location(files_location):
    return open_fs(files_location)


def generate_run_key(folder_location, files):
    """
    Generate a hash based on the last modified timestamps of the files.
    """
    # Generate a hash based on the last modified timestamps of the files
    hash_object = sha1()

    for file_path in files:
        # Open the filesystem
        with open_fs(folder_location) as filesystem:
            # Get the last modified timestamp of the file
            last_modified_time = filesystem.getinfo(
                file_path, namespaces=["details"]
            ).modified

            # Update the hash object with the string representation of the timestamp
            hash_object.update(str(last_modified_time).encode())

    # Generate hex digest of the combined hash
    return hash_object.hexdigest()


@sensor(
    job=concatenate,
    description="Runs concatenate job once move_current job is complete",
    default_status=DefaultSensorStatus.RUNNING,
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
        yield RunRequest(run_key=latest_run_record.dagster_run.run_id)


@sensor(
    job=move_current,
    description="Runs move_current job once clean job is complete",
    default_status=DefaultSensorStatus.RUNNING,
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
        yield RunRequest(run_key=latest_run_record.dagster_run.run_id)
