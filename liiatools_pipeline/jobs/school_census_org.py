from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import school_census_org as school_census

log = get_dagster_logger()


@job
def school_census_cross():
    log.info("Creating outputs for the school census CROSS report")
    session_folder = school_census.create_cross_session_folder()
    school_census.school_census_cross_outputs(session_folder)


@job
def school_census_region():
    log.info("Creating outputs for the school census REGION report")
    session_folder = school_census.create_region_session_folder()
    school_census.school_census_region_outputs(session_folder)
