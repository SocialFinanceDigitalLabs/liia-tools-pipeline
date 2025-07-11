from dagster import op, In, Out, get_dagster_logger
from fs.base import FS

from liiatools.common.aggregate import DataframeAggregator
from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesOrg
from liiatools_pipeline.ops.common_config import CleanConfig

from liiatools_pipeline.assets.common import (
    pipeline_config,
    workspace_folder,
    shared_folder,
)

log = get_dagster_logger()


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_deduplicate_session_folder(config: CleanConfig) -> FS:
    log.info("Creating session folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesOrg
    )

    log.info("Opening incoming folder...")
    session_folder = session_folder.opendir(SessionNamesOrg.INCOMING_FOLDER)

    log.info("Opening PAN files...")
    pan_folder = workspace_folder().opendir(f"current/{config.dataset}")

    log.info("Moving PAN folder to session...")
    pl.move_files_for_sharing(pan_folder, session_folder)

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def deduplicate_pan(
    session_folder: FS,
    config: CleanConfig,
):
    log.info("Creating PAN Data Frames...")
    pan = DataframeAggregator(
        session_folder, pipeline_config(config), config.dataset
    )

    log.info("Deduplicating PAN Data Frames...")
    pan_data = pan.current(deduplicate=True)

    report = "PAN_DEDUP"
    existing_shared_files = shared_folder().listdir("/")
    log.info(f"Exporting report {report} to shared folder...")
    pl.remove_files(
        f"{report}_{config.dataset}", existing_shared_files, shared_folder()
    )
    pan_data.export(shared_folder(), f"{report}_{config.dataset}_", "csv")
