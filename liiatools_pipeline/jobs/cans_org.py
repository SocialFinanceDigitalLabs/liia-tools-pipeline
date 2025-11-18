from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import cans_org as cans

log = get_dagster_logger()


@job
def cans_summary_columns():
    log.info("Mapping CANS Summary Sheet Columns")
    session_folder = cans.create_cans_session_folder()
    cans.cans_summary_sheet_mapping(session_folder)
    log.info("CANS Summary Sheet Columns mapped successfully.")
