from dagster import job, get_dagster_logger

from liiatools_pipeline.ops.common_org import (
    create_reports,
    create_org_session_folder,
    move_error_report,
    move_current_view,
    move_concat_view,
)

log = get_dagster_logger()


@job
def move_error_reports():
    move_error_report()


@job
def move_current_org():
    move_current_view()


@job
def move_concat():
    move_concat_view()


@job(
    tags={"dagster/max_runtime": 1800}
)
def reports():
    log.info("Starting Reports run. Creating session folder...")
    session_folder = create_org_session_folder()
    log.info("Can now create reports...")
    try:
        create_reports(session_folder)
    except Exception as err:
        log.error(f"Can't create reports: {err}")
