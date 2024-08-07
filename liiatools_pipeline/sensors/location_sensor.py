import os
from hashlib import sha1
from dagster import RunRequest, SkipReason, RunConfig, sensor, DefaultSensorStatus
from fs import open_fs
from fs.walk import Walker
from liiatools_pipeline.jobs.ssda903_la import ssda903_clean
from decouple import config as env_config


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
    job=ssda903_clean,
    minimum_interval_seconds=60,
    description="Monitors Specified Location for 903 Files",
    default_status=DefaultSensorStatus.RUNNING,
)
def location_sensor(context):
    context.log.info("Opening folder location: {}".format(env_config("INPUT_LOCATION")))
    folder_location = env_config("INPUT_LOCATION")
    wildcards = env_config("903_WILDCARDS").split(",")
    directory_pointer = open_location(folder_location)

    context.log.info("Analysing folder contents:")
    directory_contents = directory_walker(directory_pointer, wildcards)

    context.log.info("Generating Run Key")
    files = []
    for filename in directory_contents:
        files.append(filename.lstrip("/"))

    if not files:
        context.log.info("No files found, skipping run")
        yield SkipReason("No files present")
    else:
        context.log.info("Differences found, executing run")
        yield RunRequest(
            run_key=generate_run_key(folder_location, files),
            run_config=RunConfig(),
        )
