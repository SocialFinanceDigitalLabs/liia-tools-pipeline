from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import common_la

log = get_dagster_logger(__name__)


@job
def clean():
    log.info("Creating Session Folder...")
    session_folder, session_id, incoming_files = common_la.create_session_folder()
    current = common_la.open_current()

    log.info("Processing files...")
    common_la.process_files(session_folder, incoming_files, current, session_id)


@job
def move_current_la():
    common_la.move_current_view_la()


@job
def concatenate():
    current = common_la.open_current()
    common_la.create_concatenated_view(current)


@job
def no_op_job():
    pass


@job
def start_clean_dataset():
    log.info("Recording trigger for clean dataset")
