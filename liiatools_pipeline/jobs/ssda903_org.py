from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import ssda903_org as ssda903

log = get_dagster_logger()


@job
def ssda903_sufficiency():
    log.info("Creating lookup tables...")
    session_folder = ssda903.create_sufficiency_session_folder()
    log.info("Creating Dim/Fact tables...")
    ssda903.create_dim_fact_tables(session_folder)
