import re

from dagster import In, Out, get_dagster_logger, op
from fs.base import FS

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesPNWCensusSSDA903
from liiatools.common.pipeline import open_file
from liiatools.pnw_census_pipeline.pnw_ssda903_join import join_episode_data
from liiatools_pipeline.assets.common import workspace_folder

log = get_dagster_logger(__name__)


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_join_session_folder() -> FS:
    log.info("Creating session folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesPNWCensusSSDA903
    )

    log.info("Opening incoming folder...")
    session_folder = session_folder.opendir(
        SessionNamesPNWCensusSSDA903.INCOMING_FOLDER
    )

    ssda903_reports_folder = workspace_folder().opendir("current/ssda903/SUFFICIENCY")
    pl.move_files_for_sharing(
        ssda903_reports_folder,
        session_folder,
        required_table_id=["episodes", "header", "uasc", "oc2", "missing"],
    )

    pnw_census_reports_folder = workspace_folder().opendir("current/pnw_census")
    pl.move_files_for_sharing(pnw_census_reports_folder, session_folder)

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def join_pnw_census_ssda903(
    session_folder: FS,
):
    log.info("Checking necessary files are present...")
    episodes = re.compile(r"episodes")
    header = re.compile(r"header")
    uasc = re.compile(r"uasc")
    oc2 = re.compile(r"oc2")
    pnw = re.compile(r"pnw")

    files = session_folder.listdir("/")

    pnw_census_file = next((f for f in files if pnw.search(f)), None)

    ssda903_pattern_list = [episodes, header, uasc, oc2]

    # Check which ssda903 files are present
    ssda903_files = [
        filename
        for filename in files
        if any(pattern.search(filename) for pattern in ssda903_pattern_list)
    ]

    if pnw_census_file:
        log.info(f"PNW Census found: {pnw_census_file}")
        # Execute logic based on available files
        if ssda903_files:
            log.info(f"SSDA903 files found: {ssda903_files}")
            # Open the PNW Census file
            pnw_census = open_file(session_folder, pnw_census_file)
            for matched in ssda903_files:
                log.info(f"Processing {matched}")
                # Execute logic based on matched files
                if "episode" in matched:
                    log.info(f"Joining SSDA903 episode data with PNW Census data")
                    # Open the file
                    episodes = open_file(session_folder, matched)
                    # Join the data
                    pnw_census = join_episode_data(episodes, pnw_census)
                if "header" in matched:
                    # TODO: implement header join
                    header = open_file(session_folder, matched)
                if "uasc" in matched:
                    # TODO: implement uasc join
                    uasc = open_file(session_folder, matched)
                if "oc2" in matched:
                    # TODO: implement oc2 join
                    oc2 = open_file(session_folder, matched)
        else:
            log.error("No SSDA903 files found")
