from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import cin_org as cin

log = get_dagster_logger()

@job
def cin_reports():
    log.info("Starting Reports run. Creating session folder...")
    session_folder = cin.create_cin_reports_session_folder()
    log.info("Can now create reports...")

    try:
        cin.expanded_assessment_factors(session_folder)
        cin.referral_outcomes(session_folder)
        cin.s47_journeys(session_folder)
    except Exception as e:
        log.error(f"Error occurred while creating reports: {e}")
        raise