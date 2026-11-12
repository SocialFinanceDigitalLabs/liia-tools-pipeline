from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import cans_org as cans

log = get_dagster_logger()


@job
def cans_summary_columns():
    log.info("Mapping CANS Summary Sheet Columns")
    session_folder = cans.create_cans_session_folder()
    cans.cans_summary_sheet_mapping(session_folder)
    log.info("CANS Summary Sheet Columns mapped successfully.")


@job
def transform_pan_cans_data():
    log.info("Transforming CANS Data")
    session_folder = cans.create_cans_session_folder()
    cans.transform_cans_data(session_folder)
    log.info("CANS Data transformed successfully.")


@job
def pan_cans_joins():
    log.info("Joining CANS Data with SSDA903 episodes Data")
    session_folder = cans.create_pan_cans_session_folder()
    cans.joins_pan_cans(session_folder)
    log.info("CANS Data joined successfully.")
