from dagster import job

from liiatools_pipeline.ops import ssda903_la as ssda903


@job
def ssda903_clean():
    session_folder, session_id, incoming_files = ssda903.create_session_folder()
    current = ssda903.open_current()

    ssda903.process_files(session_folder, incoming_files, current, session_id)


@job
def ssda903_move_current():
    ssda903.move_current_view()


@job
def ssda903_concatenate():
    current = ssda903.open_current()
    ssda903.create_concatenated_view(current)
