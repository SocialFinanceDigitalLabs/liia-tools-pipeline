from dagster import job

from liiatools_pipeline.ops import ssda903_la as ssda903


@job
def ssda903_fix_episodes():
    session_folder = ssda903.create_fix_episodes_session_folder()
    ssda903.fix_episodes(session_folder)
