from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import placements_standard_org as placements_standard

log = get_dagster_logger()


@job
def cdm_placements_standard_joins():
    log.info("Joining additional datasets with Placements Standard...")
    session_folder = placements_standard.create_cdm_placements_standard_session_folder()
    placements_standard.joins_cdm_placements_standard(session_folder)