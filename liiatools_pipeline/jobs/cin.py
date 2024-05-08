from dagster import job

from liiatools_pipeline.ops import cin

@job
def cin_incoming():
    session_folder, session_id, incoming_files = cin.create_session_folder()
    archive = cin.open_archive(session_id)

    processed = cin.process_files(
        session_folder, incoming_files, archive, session_id
    )

    current_data = cin.create_current_view(archive, start=processed)

    cin.create_reports(current_data)
