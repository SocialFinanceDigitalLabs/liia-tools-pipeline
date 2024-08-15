from dagster import RunRequest, SkipReason, RunConfig, sensor, DefaultSensorStatus

from liiatools_pipeline.jobs.common_la import clean
from decouple import config as env_config
from liiatools_pipeline.sensors.common import (
    generate_run_key,
    open_location,
    directory_walker,
)


@sensor(
    # Changed job= "ssda903_clean" to "clean" from jobs.common
    job=clean,
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
