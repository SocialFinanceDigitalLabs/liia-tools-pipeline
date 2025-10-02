from dagster import get_dagster_logger, job

from liiatools_pipeline.ops import cin_la as cin

log = get_dagster_logger()


@job
def cin_deduplicate_files():
    log.info("Deduplicating CIN files...")
    session_folder = cin.create_cin_dedup_session_folder()
    cin.cin_deduplication(session_folder)
