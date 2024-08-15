from dagster import repository
from liiatools.common._fs_serializer import register

from liiatools_pipeline.jobs.sufficiency_903 import ssda903_sufficiency
from liiatools_pipeline.sensors.sufficiency_sensor import sufficiency_sensor

register()


@repository
def sync():
    """
    The repository definition for this etl Dagster repository.  This is for the organisation account
    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [ssda903_sufficiency]
    schedules = []
    sensors = [sufficiency_sensor]

    return jobs + schedules + sensors