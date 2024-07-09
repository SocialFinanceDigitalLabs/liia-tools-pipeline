from dagster import job
from liiatools_pipeline.ops.common_org import (
    create_reports,
    create_org_session_folder,
)


@job
def reports():
    session_folder = create_org_session_folder()
    create_reports(session_folder)
