from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import ssda903_org as ssda903

log = get_dagster_logger()


@job
def ssda903_sufficiency():
    log.info("Creating lookup tables...")
    session_folder = ssda903.create_sufficiency_session_folder()
    log.info("Creating Dim/Fact tables...")
    ssda903.create_dim_fact_tables(session_folder)


@job
def cdm_sufficiency_joins():
    log.info("Joining additional datasets with SSDA903...")
    session_folder = ssda903.create_cdm_sufficiency_join_session_folder()
    ssda903.joins_cdm_sufficiency(session_folder)


@job
def cdm_commissioning_joins():
    log.info("Joining additional datasets with SSDA903...")
    session_folder = ssda903.create_cdm_commissioning_join_session_folder()
    ssda903.joins_cdm_commissioning(session_folder)