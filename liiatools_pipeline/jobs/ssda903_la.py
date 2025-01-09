from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import ssda903_la as ssda903

log = get_dagster_logger()


@job
def ssda903_fix_episodes():
    log.info("Fixing Episodes...")
    session_folder = ssda903.create_fix_episodes_session_folder()
    ssda903.fix_episodes(session_folder)
