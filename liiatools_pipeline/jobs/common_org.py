from dagster import get_dagster_logger, job

from liiatools_pipeline.ops.common_org import (create_org_session_folder,
                                               create_reports,
                                               move_concat_view,
                                               move_current_view_org,
                                               move_error_report)

log = get_dagster_logger()


@job
def move_error_reports():
    move_error_report()


@job
def move_current_org():
    move_current_view_org()


@job
def move_concat():
    move_concat_view()


@job(tags={"dagster/max_runtime": 1800})
def reports():
    log.info("Starting Reports run. Creating session folder...")
    session_folder = create_org_session_folder()
    log.info("Can now create reports...")
    try:
        create_reports(session_folder)
    except Exception as err:
        log.error(f"Can't create reports: {err}")
