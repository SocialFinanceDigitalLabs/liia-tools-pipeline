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
    episodes_pattern = re.compile(r"episodes")
    header_pattern = re.compile(r"header")
    uasc_pattern = re.compile(r"uasc")
    oc2_pattern = re.compile(r"oc2")
    missing_pattern = re.compile(r"missing")
    pnw_pattern = re.compile(r"pnw")

    files = session_folder.listdir("/")

    try:
        pnw_census_file = next((f for f in files if pnw_pattern.search(f)), None)
    except errors.ResourceNotFound as err:
        log.error(f"No PNW file to open: {err}")
        log.info("Exiting run as PNW Census file not available")
        return

    # Check if there are any SSDA903 files
    ssda903_patterns = [episodes_pattern, header_pattern, uasc_pattern, oc2_pattern, missing_pattern]
    if not any(any(pattern.search(f) for f in files) for pattern in ssda903_patterns):
        log.error("No SSDA903 files found: terminating process.")
        return

    # Open the PNW Census file
    pnw_census = open_file(session_folder, pnw_census_file)
    # Derive 'snapshot' date used in every table join equal to the last day of the snapshot month
    pnw_census["snapshot_date"] = pd.to_datetime(pnw_census[["Year", "Month"]].assign(day=1)) + MonthEnd(0)

    # Check and process each SSDA903 file type
    if any(episodes_pattern.search(f) for f in files):
        log.info("Joining SSDA903 episode data with PNW Census data")
        episodes_file = next(f for f in files if episodes_pattern.search(f))
        episodes = open_file(session_folder, episodes_file)
        pnw_census = join_episode_data(episodes, pnw_census)
        try:
            ext_folder = external_data_folder()
            postcode = open_file(ext_folder, "ONSPD_reduced_to_postcode_sector.csv")
            log.info("ONSPD file found")
            pnw_census = join_onspd_data(postcode, pnw_census)
        except errors.ResourceNotFound as err:
            log.error(f"No ONSPD postcode file to open: {err}")
            empty_ONSPD_cols = ["Home eastings", "Home northings", "Placement eastings", "Placement northings", "Placement LA code"]
            for col in empty_ONSPD_cols:
                pnw_census[col] = None
    else:
        log.error("No 903 episodes data to join")
        empty_episode_cols = ["# placements in last 12 months", "903 placement type", "903 provider type", "Home postcode", "Placement postcode", "Home eastings", "Home northings", "Placement eastings", "Placement northings", "Placement LA code"]
        for col in empty_episode_cols:
            pnw_census[col] = None

    if any(header_pattern.search(f) for f in files):
        log.info("Joining SSDA903 header data with PNW Census data")
        header_file = next(f for f in files if header_pattern.search(f))
        header = open_file(session_folder, header_file)
        pnw_census = join_header_data(header, pnw_census)
    else:
        log.error("No 903 header data to join")
        empty_header_cols = ["Gender 903", "Ethnicity 903"]
        for col in empty_header_cols:
            pnw_census[col] = None

    if any(uasc_pattern.search(f) for f in files):
        log.info("Joining SSDA903 UASC data with PNW Census data")
        uasc_file = next(f for f in files if uasc_pattern.search(f))
        uasc = open_file(session_folder, uasc_file)
        pnw_census = join_uasc_data(uasc, pnw_census)
    else:
        log.error("No 903 uasc data to join")
        empty_uasc_cols = ["UASC 903"]
        for col in empty_uasc_cols:
            pnw_census[col] = None

    if any(oc2_pattern.search(f) for f in files):
        log.info("Joining SSDA903 OC2 data with PNW Census data")
        oc2_file = next(f for f in files if oc2_pattern.search(f))
        oc2 = open_file(session_folder, oc2_file)
        pnw_census = join_oc2_data(oc2, pnw_census)
    else:
        log.error("No 903 oc2 data to join")
        empty_oc2_cols = ["Child convicted during the year", "Child identified as having a substance misuse problem", "Child received intervention for substance misuse problem", "Child offered intervention for substance misuse problem"]
        for col in empty_oc2_cols:
            pnw_census[col] = None

    if any(missing_pattern.search(f) for f in files):
        log.info("Joining SSDA903 missing data with PNW Census data")
        missing_file = next(f for f in files if missing_pattern.search(f))
        missing = open_file(session_folder, missing_file)
        pnw_census = join_missing_data(missing, pnw_census)
    else:
        log.error("No 903 missing data to join")
        empty_missing_cols = ["# missing episodes in last 12 months"]
        for col in empty_missing_cols:
            pnw_census[col] = None

    # Drop snapshot date field
    pnw_census = pnw_census.drop(columns="snapshot_date")
    
    # Export PNW file
    pnw_dc = DataContainer({"ENRICHED_pnw_census_pnw_census":pnw_census})
    log.info("Writing joined PNW Census output to shared folder")
    output_folder = shared_folder()
    pnw_dc.export(output_folder, "", "csv")
