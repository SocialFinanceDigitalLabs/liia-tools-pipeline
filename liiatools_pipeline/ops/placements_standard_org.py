import re

import pandas as pd
from dagster import In, Out, get_dagster_logger, op
from decouple import config as env_config
from fs import errors
from fs.base import FS
from ruamel.yaml import YAML

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesPanPlacementsStandardJoins
from liiatools.common.data import DataContainer
from liiatools.common.pipeline import open_file
from liiatools.placements_standard_pipeline.placements_standard_dataset_join import (
    join_episodes_data,
)
from liiatools_pipeline.assets.common import (
    pipeline_config,
    shared_folder,
    workspace_folder,
)
from liiatools_pipeline.ops.common_config import CleanConfig
from liiatools_pipeline.util.utility import opendir_location

yaml = YAML()

log = get_dagster_logger()


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_placements_standard_session_folder() -> FS:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesPanPlacementsStandardJoins
    )
    """
    Create Placements Standard session folder and move required files for mapping into it
    """
    log.info("Creating Placements Standard session folder")
    session_folder = session_folder.opendir(SessionNamesPanPlacementsStandardJoins.INCOMING_FOLDER)

    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")

    clean_config = CleanConfig(
                    dataset_folder=None,
                    la_folder=None,
                    input_la_code=None,
                    dataset="placements_standard",
                )
    output_config = pipeline_config(clean_config)
    current_folder = opendir_location(workspace_folder(), f"current/placements_standard/{output_config.input_schema_version}")

    pl.move_files_for_sharing(
        current_folder,
        session_folder,
    )

    if "ssda903" in allowed_datasets:
            ssda903_reports_folder = opendir_location(workspace_folder(), "current/ssda903/PAN")
            pl.move_files_for_sharing(
                ssda903_reports_folder,
                session_folder,
                required_table_id=["episodes"],
                )

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def joins_pan_placements_standard(
    session_folder: FS,
):
    log.info("Checking necessary files are present...")
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")

    # Placements standard file patterns
    placements_standard_pattern = re.compile(r"placements_standard")

    # SSDA903 file patterns
    episodes_pattern = re.compile(r"episodes")

    files = session_folder.listdir("/")

    log.info("Removing existing PAN_PLACEMENTS_STANDARD file from the shared folder before processing a new one")
    existing_pan_placements_standard_files = shared_folder().listdir("/")
    pl.remove_files("PAN_PLACEMENTS_STANDARD", existing_pan_placements_standard_files, shared_folder())

    placements_standard_file = next((f for f in files if placements_standard_pattern.search(f)), None)
    if not placements_standard_file:
        log.error("No placements standard file to open")
        log.info("Exiting run as placements standard file not available")
        return

    # If no Placements Standard or SSDA903 files, terminate process
    if not any(
        any(pattern.search(f) for f in files) for pattern in [episodes_pattern]
    ) and not any(pattern.search(f) for f in files for pattern in [placements_standard_pattern]
    ):
        log.error("No SSDA903 or Placements Standard files found: terminating process.")
        return

    # Open the placements standard file
    placements_standard = open_file(session_folder, placements_standard_file)

    # Accumulates rows from joined datasets that could not be matched onto SSDA903 header
    unmatched_rows = pd.DataFrame(columns=["Row Number", "Dataset", "Year", "Month", "LA", "Matching Criteria"])

    # Check and process SSDA903 episodes file type
    if "ssda903" in allowed_datasets:
        if any(episodes_pattern.search(f) for f in files):
            log.info("Joining SSDA903 episodes data with placements standard data")
            episodes_file = next(f for f in files if episodes_pattern.search(f))
            episodes = open_file(session_folder, episodes_file)
            placements_standard, unmatched_episodes = join_episodes_data(episodes, placements_standard)
            unmatched_rows = pd.concat([unmatched_rows, unmatched_episodes], ignore_index=True)
        else:
            log.error("No SSDA903 episodes data to join with placements standard data")
            empty_episodes_cols = ["EPISODE_ID"]
            for col in empty_episodes_cols:
                placements_standard[col] = None

    # Export header file
    placements_standard_dc = DataContainer(
        {
            "PAN_PLACEMENTS_STANDARD": placements_standard,
            "PAN_PLACEMENTS_STANDARD_MATCHING_REPORT": unmatched_rows
         }
    )
    log.info("Writing joined placements standard output to shared folder")
    placements_standard_dc.export(shared_folder(), "", "csv")