from dagster import job
# from liiatools_pipeline.ops import common1
# from liiatools_pipeline.assets.common import dataset
from liiatools_pipeline.ops import common_la
from liiatools_pipeline.ops.common_org import (
    create_reports,
    create_org_session_folder,
)


@job
def clean():
    session_folder, session_id, incoming_files = common_la.create_session_folder()
    current = common_la.open_current()

    common_la.process_files(session_folder, incoming_files, current, session_id)


@job
def move_current():
    common_la.move_current_view()


@job
def concatenate():
    current = common_la.open_current()
    common_la.create_concatenated_view(current)


@job
def reports():
    session_folder = create_org_session_folder()
    create_reports(session_folder)
