import re
import pandas as pd
from pandas.tseries.offsets import MonthEnd

from dagster import In, Out, get_dagster_logger, op
from fs.base import FS
from fs import errors

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesPNWCensusJoins
from liiatools.common.data import DataContainer
from liiatools.common.pipeline import open_file
from liiatools.pnw_census_pipeline.pnw_dataset_join import (
    join_episode_data,
    join_header_data,
    join_uasc_data,
    join_oc2_data,
    join_missing_data,
    join_onspd_data,
    )
from liiatools_pipeline.assets.common import workspace_folder, shared_folder
from liiatools_pipeline.assets.external_dataset import external_data_folder

log = get_dagster_logger(__name__)


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_join_session_folder() -> FS:
    log.info("Creating session folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesPNWCensusJoins
    )

    log.info("Opening incoming folder...")
    session_folder = session_folder.opendir(
        SessionNamesPNWCensusJoins.INCOMING_FOLDER
    )

    ssda903_reports_folder = workspace_folder().opendir("current/ssda903/PAN")
    pl.move_files_for_sharing(
        ssda903_reports_folder,
        session_folder,
        required_table_id=["episodes", "header", "uasc", "oc2", "missing"],
    )

    pnw_census_reports_folder = workspace_folder().opendir("current/pnw_census/PAN")
    pl.move_files_for_sharing(pnw_census_reports_folder, session_folder)

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def joins_pnw_census(
    session_folder: FS,
):
    log.info("Checking necessary files are present...")
    episodes = re.compile(r"episodes")
    header = re.compile(r"header")
    uasc = re.compile(r"uasc")
    oc2 = re.compile(r"oc2")
    missing = re.compile(r"missing")
    pnw = re.compile(r"pnw")

    files = session_folder.listdir("/")

    try:
        pnw_census_file = next((f for f in files if pnw.search(f)), None)
    except errors.ResourceNotFound as err:
        log.error(f"No PNW file to open: {err}")
        log.info("Exiting run as PNW Census file not available")
        return

    ssda903_pattern_list = [episodes, header, uasc, oc2, missing]

    # Check which ssda903 files are present
    ssda903_files = [
        filename
        for filename in files
        if any(pattern.search(filename) for pattern in ssda903_pattern_list)
    ]

    # Find external data folder for ONSPD file
    ext_folder = external_data_folder()

    # Open the PNW Census file
    pnw_census = open_file(session_folder, pnw_census_file)
    # Derive 'snapshot' date used in every table join equal to the last day of the snapshot month
    pnw_census["snapshot_date"] = pd.to_datetime(pnw_census[["Year", "Month"]].assign(day=1)) + MonthEnd(0)
    # Execute logic based on available files
    if ssda903_files:
        log.info(f"SSDA903 files found: {ssda903_files}")
        for matched in ssda903_files:
            log.info(f"Processing {matched}")
            # Execute logic based on matched files
            if "episodes" in matched:
                log.info("Joining SSDA903 episode data with PNW Census data")
                # Open the file
                episodes = open_file(session_folder, matched)
                # Join the data
                pnw_census = join_episode_data(episodes, pnw_census)
                # With episodes data, ONSPD data can be matched
                try:
                    postcode = open_file(ext_folder, "ONSPD_reduced_to_postcode_sector.csv")
                    log.info("ONSPD file found")
                    # Join the data
                    pnw_census = join_onspd_data(postcode, pnw_census)
                except errors.ResourceNotFound as err:
                    log.error(f"No ONSPD postcode file to open: {err}")
            if "header" in matched:
                log.info("Joining SSDA903 header data with PNW Census data")
                # Open the file
                header = open_file(session_folder, matched)
                # Join the data
                pnw_census = join_header_data(header, pnw_census)
            if "uasc" in matched:
                log.info("Joining SSDA903 UASC data with PNW Census data")
                # Open the file
                uasc = open_file(session_folder, matched)
                # Join the data
                pnw_census = join_uasc_data(uasc, pnw_census)
            if "oc2" in matched:
                log.info("Joining SSDA903 OC2 data with PNW Census data")
                # Open the file
                oc2 = open_file(session_folder, matched)
                # Join the data
                pnw_census = join_oc2_data(oc2, pnw_census)
            if "missing" in matched:
                log.info("Joining SSDA903 missing data with PNW Census data")
                # Open the file
                missing = open_file(session_folder, matched)
                # Join the data
                pnw_census = join_missing_data(missing, pnw_census)
    else:
        log.error("No SSDA903 files found")
        return

    # Export PNW file
    pnw_dc = DataContainer({"pnw_census":pnw_census})
    log.info("Writing joined PNW Census output to shared folder")
    output_folder = shared_folder()
    pnw_dc.export(output_folder, "", "csv")
