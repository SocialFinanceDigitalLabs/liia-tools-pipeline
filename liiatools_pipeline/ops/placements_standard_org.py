import re

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
from liiatools_pipeline.assets.common import shared_folder, workspace_folder
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

    current_folder = opendir_location(workspace_folder(), "current/cans/PAN")
    pl.move_files_for_sharing(
        current_folder,
        session_folder,
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

    try:
        placements_standard_file = next((f for f in files if placements_standard_pattern.search(f)), None)
    except errors.ResourceNotFound as err:
        log.error(f"No placements standard file to open: {err}")
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

    # Check and process SSDA903 episodes file type
    if "ssda903" in allowed_datasets:
        if any(episodes_pattern.search(f) for f in files):
            log.info("Joining SSDA903 episodes data with placements standard data")
            episodes_file = next(f for f in files if episodes_pattern.search(f))
            episodes = open_file(session_folder, episodes_file)
            placements_standard = join_episodes_data(episodes, placements_standard)
        else:
            log.error("No SSDA903 episodes data to join with placements standard data")
            empty_episodes_cols = ["EPISODE_ID"]
            for col in empty_episodes_cols:
                placements_standard[col] = None

    # Export header file
    placements_standard_dc = DataContainer({"PAN_PLACEMENTS_STANDARD": placements_standard})
    log.info("Writing joined placements standard output to shared folder")
    output_folder = shared_folder()
    placements_standard_dc.export(output_folder, "", "csv")