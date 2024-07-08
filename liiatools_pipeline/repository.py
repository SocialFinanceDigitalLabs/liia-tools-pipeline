from dagster import repository
from liiatools.common._fs_serializer import register

from liiatools_pipeline.jobs.common import (
    clean,
    move_current,
    concatenate,
    reports,
)
from liiatools_pipeline.jobs.ssda903_org import ssda903_sufficiency
from liiatools_pipeline.jobs.cin import cin_incoming
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
        clean,
        move_current,
        concatenate,
        reports,
        external_incoming,
        ssda903_sufficiency,
        cin_incoming,
    ]
    schedules = []
    sensors = [location_sensor, sufficiency_sensor]

    return jobs + schedules + sensors


# sufficiency_sensor
