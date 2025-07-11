from dagster import In, Out, op, get_dagster_logger
from fs.base import FS
from fs import errors
import re

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesSufficiency
from liiatools.common.data import DataContainer
from liiatools.ssda903_pipeline.sufficiency_transform import (
    dict_to_dfs, ofsted_transform, ons_transform, open_file,
    postcode_transform, ss903_transform)
from liiatools_pipeline.assets.common import shared_folder, workspace_folder
from liiatools_pipeline.assets.external_dataset import external_data_folder

log = get_dagster_logger()

@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_sufficiency_session_folder() -> FS:
    log.info("Creating session folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesSufficiency
    )
    session_folder = session_folder.opendir(SessionNamesSufficiency.INCOMING_FOLDER)

    reports_folder = workspace_folder().opendir("current/ssda903/SUFFICIENCY")
    log.info("Moving 903 input files for sufficiency to session folder")
    pl.move_files_for_sharing(
        reports_folder, session_folder, required_table_id=["episodes", "header", "uasc"]
    )

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def create_dim_fact_tables(
    session_folder: FS,
):
    # Check that the files necessary for the job are in the folder
    log.info("Checking required input files are all present")
    episodes = re.compile(r"episodes")
    header = re.compile(r"header")
    uasc = re.compile(r"uasc")
    pattern_list = [episodes, header, uasc]

    files = session_folder.listdir("/")

    all_files_present = all(
        any(pattern.search(filename) for filename in files) for pattern in pattern_list
    )

    # Run the data transformation if all necessary files are present
    if all_files_present:
        log.info("All 903 input files present in session folder; proceeding to data transformation")
        ext_folder = external_data_folder()
        output_folder = shared_folder()

        # Create dictionary to store tables, starting with basic dim tables
        dim_tables = dict_to_dfs()

        # Create dimONSArea table
        # Open external file
        try:
            ONSArea = open_file(ext_folder, "ONS_Area.csv")
        except errors.ResourceNotFound as err:
            log.error(f"No ONS_Area file to open: {err}")
            log.info("Exiting run as external dataset resources not available")
            return
        
        # Transform ONSArea table
        ONSArea = ons_transform(ONSArea)
        log.info("Creating dimONSArea table")
        dim_tables["dimONSArea"] = ONSArea

        # Create dimPostcode table
        # Open external file
        try:
            Postcode = open_file(ext_folder, "ONSPD_reduced_to_postcode_sector.csv")
        except errors.ResourceNotFound as err:
            log.error(f"No ONSPD postcode file to open: {err}")
            log.info("Exiting run as external dataset resources not available")
            return
                    
        # Transform Postcode table
        Postcode = postcode_transform(Postcode)
        log.info("Creating dimPostcode table")
        dim_tables["dimPostcode"] = Postcode

        # Create dimOfstedProvider and factOfstedInspection tables
        # Open and transform files
        OfstedProvider, factOfstedInspection = ofsted_transform(ext_folder, ONSArea, log)
        if OfstedProvider is None and factOfstedInspection is None:
            log.info("Terminating process")
            return
        
        log.info("Creating dimOfstedProvider table")
        dim_tables["dimOfstedProvider"] = OfstedProvider
        log.info("Creating factOfstedInspection table")
        dim_tables["factOfstedInspection"] = factOfstedInspection

        # Create dimLookedAfterChild and factEpisode table
        # Open ssda903 files
        LookedAfterChild = open_file(session_folder, "ssda903_header.csv")
        UASC = open_file(session_folder, "ssda903_uasc.csv")
        Episode = open_file(session_folder, "ssda903_episodes.csv")

        # Transform tables
        LookedAfterChild, factEpisode = ss903_transform(
            LookedAfterChild, UASC, ONSArea, Episode, Postcode, OfstedProvider
        )
        log.info("Creating dimLookedAfterChild table")
        dim_tables["dimLookedAfterChild"] = LookedAfterChild
        log.info("Creating factEpisode table")
        dim_tables["factEpisode"] = factEpisode

        # Export tables
        dim_tables = DataContainer(dim_tables)

        reports_folder = workspace_folder().opendir("current/ssda903/SUFFICIENCY")
        log.info("Exporting output tables to org current folder")
        dim_tables.export(reports_folder, "", "csv")

        log.info("Exporting output tables to org shared folder")
        dim_tables.export(output_folder, "", "csv")

    # If three 903 input files not present, terminate process with log
    else:
        log.info("903 input files for sufficiency not all present: terminating process")