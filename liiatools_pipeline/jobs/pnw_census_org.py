from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import pnw_census_org as pnw_census

log = get_dagster_logger()


@job
def pnw_census_joins():
    log.info("Joining additional datasets with PNW Census...")
    session_folder = pnw_census.create_join_session_folder()
    pnw_census.joins_pnw_census(session_folder)


@job
def transform_pan_pnw_census_data():
    log.info("Transforming PNW Census data...")
    session_folder = pnw_census.create_transform_session_folder()
    pnw_census.transform_pnw_census_data(session_folder)