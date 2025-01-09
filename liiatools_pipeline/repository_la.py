from dagster import repository

from liiatools.common._fs_serializer import register
from liiatools_pipeline.jobs.common_la import (clean, concatenate,
                                               move_current_la)
from liiatools_pipeline.jobs.ssda903_la import ssda903_fix_episodes
from liiatools_pipeline.sensors.job_success_sensor import (
    concatenate_sensor, move_current_la_sensor, ssda903_fix_episodes_sensor)
from liiatools_pipeline.sensors.location_schedule import clean_schedule

register()


@repository
def sync():
    """
    The repository definition for this etl Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [
        clean,
        move_current_la,
        concatenate,
        ssda903_fix_episodes,
    ]
    schedules = [
        clean_schedule,
    ]
    sensors = [
        move_current_la_sensor,
        concatenate_sensor,
        ssda903_fix_episodes_sensor,
    ]

    return jobs + schedules + sensors
