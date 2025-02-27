from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import pnw_census_org as pnw_census

log = get_dagster_logger()


@job
def pnw_census_ssda903_join():
    log.info("Joining SSDA903 with PNW Census...")
    session_folder = pnw_census.create_join_session_folder()
    pnw_census.join_pnw_census_ssda903(session_folder)
