from dagster import repository
from liiatools.common._fs_serializer import register

from liiatools_pipeline.jobs.ssda903_la import (
    ssda903_clean,
    ssda903_move_current,
    ssda903_concatenate,
    ssda903_fix_episodes,
)
from liiatools_pipeline.jobs.common_la import move_current, concatenate
from liiatools_pipeline.sensors.common import concatenate_sensor, move_current_sensor
from liiatools_pipeline.sensors.location_sensor import location_sensor

register()


@repository
def sync():
    """
    The repository definition for this etl Dagster repository.  This is for the LA account

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [ssda903_clean, ssda903_fix_episodes, move_current, concatenate]
    schedules = []
    sensors = [location_sensor, concatenate_sensor, move_current_sensor]

    return jobs + schedules + sensors


# sufficiency_sensor
