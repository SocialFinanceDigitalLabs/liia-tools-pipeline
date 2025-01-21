from dagster import repository

from liiatools.common._fs_serializer import register
from liiatools_pipeline.jobs.common_org import (move_concat, move_current_org,
                                                move_error_reports, reports)
from liiatools_pipeline.jobs.external_dataset import external_incoming
from liiatools_pipeline.jobs.ssda903_org import ssda903_sufficiency
from liiatools_pipeline.sensors.job_success_sensor import (
    move_concat_sensor, move_current_org_sensor, move_error_reports_sensor,
    sufficiency_sensor)
from liiatools_pipeline.sensors.location_schedule import reports_schedule

register()


@repository
def sync():
    """
    The repository definition for this etl Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [
        move_error_reports,
        move_current_org,
        move_concat,
        reports,
        external_incoming,
        ssda903_sufficiency,
    ]
    schedules = [
        reports_schedule,
    ]
    sensors = [
        move_error_reports_sensor,
        move_current_org_sensor,
        move_concat_sensor,
        sufficiency_sensor,
    ]

    return jobs + schedules + sensors
