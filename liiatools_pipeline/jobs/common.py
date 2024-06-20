from dagster import job
from liiatools_pipeline.ops import common1
from liiatools_pipeline.assets.common import dataset

@job
def globals()[f"{dataset}_incoming"]():
    session_folder, session_id, incoming_files = common1.create_session_folder()
    archive = common1.open_archive(session_id)

    processed = common1.process_files(
        session_folder, incoming_files, archive, session_id
    )

    current_data = common1.create_current_view(archive, start=processed)

    common1.create_reports(current_data)
