from dagster import job

from liiatools_pipeline.ops.common_org import (
    create_reports,
    create_org_session_folder,
    move_error_report,
    move_current_and_concat_view,
)


@job
def move_error_report():
    move_error_report()


@job
def move_current_and_concat():
    move_current_and_concat_view()


@job
def reports():
    session_folder = create_org_session_folder()
    create_reports(session_folder)
