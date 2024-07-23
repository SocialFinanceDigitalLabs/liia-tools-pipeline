from dagster import job, RunConfig

from liiatools_pipeline.ops import ssda903_la as ssda903
from liiatools_pipeline.assets.ssda903 import incoming_folder


@job
def ssda903_clean():
    session_folder, session_id, incoming_files = ssda903.create_session_folder(
        config=RunConfig({"create_session_folder": ssda903.FileConfig(incoming_folder=incoming_folder())})
    )
    current = ssda903.open_current()

    ssda903.process_files(session_folder, incoming_files, current, session_id)


@job
def ssda903_move_current():
    ssda903.move_current_view()


@job
def ssda903_concatenate():
    current = ssda903.open_current()
    ssda903.create_concatenated_view(current)
