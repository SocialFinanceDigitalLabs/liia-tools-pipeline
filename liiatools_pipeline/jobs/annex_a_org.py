from dagster import job
from liiatools_pipeline.ops import annex_a_org

from dagster import get_dagster_logger

log = get_dagster_logger(__name__)


@job
def deduplicate_annex_a():
    session_folder = annex_a_org.create_deduplicate_session_folder()
    annex_a_org.deduplicate_pan(session_folder)
