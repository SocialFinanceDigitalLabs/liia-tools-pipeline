from dagster import repository
from liiatools.common._fs_serializer import register

from liiatools_pipeline.jobs.common_la import (
    clean,
    move_current,
    concatenate,
)
from liiatools_pipeline.jobs.common_org import (
    reports,
)
from liiatools_pipeline.jobs.common_org import ssda903_sufficiency
from liiatools_pipeline.jobs.external_dataset import external_incoming
from liiatools_pipeline.sensors.location_sensor import location_sensor
from liiatools_pipeline.sensors.sufficiency_sensor import sufficiency_sensor
from liiatools_pipeline.sensors.move_current_sensor import move_current_sensor
from liiatools_pipeline.sensors.concatenate_sensor import concatenate_sensor

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
    ]
    schedules = []
    sensors = [location_sensor,
               sufficiency_sensor,
               move_current_sensor,
               concatenate_sensor,
    ]

    return jobs + schedules + sensors
