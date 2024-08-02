from dagster import repository
from liiatools.common._fs_serializer import register

from liiatools_pipeline.jobs.ssda903_la import (
    ssda903_clean,
    ssda903_move_current,
    ssda903_concatenate,
    ssda903_fix_episodes,
)
from liiatools_pipeline.jobs.ssda903_org import (
    ssda903_move_error_report
    ssda903_move_current_and_concat,
    ssda903_sufficiency,
    ssda903_reports,
)
from liiatools_pipeline.jobs.external_dataset import external_incoming
from liiatools_pipeline.sensors.location_sensor import location_sensor
from liiatools_pipeline.sensors.sufficiency_sensor import sufficiency_sensor

register()


@repository
def sync():
    """
    The repository definition for this etl Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [
        ssda903_clean,
        ssda903_move_current,
        ssda903_concatenate,
        ssda903_move_error_report,
        ssda903_fix_episodes,
        ssda903_move_current_and_concat,
        ssda903_reports,
        external_incoming,
        ssda903_sufficiency,
    ]
    schedules = []
    sensors = [location_sensor, sufficiency_sensor]

    return jobs + schedules + sensors


# sufficiency_sensor
