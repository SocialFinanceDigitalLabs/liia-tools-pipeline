import os
import re
from importlib import resources

import pandas as pd
from dagster import In, Out, get_dagster_logger, op
from decouple import config as env_config
from fs import errors
from fs.base import FS
from ruamel.yaml import YAML

from liiatools.cans_pipeline.cans_dataset_join import join_episodes_data
from liiatools.cans_pipeline.spec import (
    load_category_mapping,
    load_summary_sheet_column_order,
    load_summary_sheet_mapping,
)
from liiatools.cans_pipeline.summary_sheet_mapping import (
    add_summary_sheet_columns,
    map_summary_planning,
)
from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesCANSMapping
from liiatools.common.data import DataContainer
from liiatools.common.pipeline import open_file
from liiatools_pipeline.assets.common import shared_folder, workspace_folder
from liiatools_pipeline.util.utility import opendir_location

yaml = YAML()

log = get_dagster_logger()


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_cans_session_folder() -> FS:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesCANSMapping
    )
    """
    Create CANS session folder and move required files for mapping into it
    """
    log.info("Creating CANS session folder")
    session_folder = session_folder.opendir(SessionNamesCANSMapping.INCOMING_FOLDER)

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
def cans_summary_sheet_mapping(
    session_folder: FS,
):
    """
    Add CANS Summary sheet mapping columns to CANS files
    and export enriched files to shared folder
    """
    files = session_folder.listdir("/")
    log.info(f"Files in session folder: {files}")

    mapping = load_summary_sheet_mapping()
    column_order = load_summary_sheet_column_order()

    for file in files:
        log.info(f"Adding Summary Sheet mapping for {file}")
        # assign name of file without suffix
        name, _ = os.path.splitext(file)
        # get table name from file name
        table_name = "_".join(name.split("_")[-2:])
        # fetch the correct mapping for the table
        file_mapping = mapping[table_name]
        file_column_order = column_order[table_name]

        data = open_file(session_folder, file)

        try:
            data = add_summary_sheet_columns(data, file_mapping, file_column_order)
        except TypeError as err:
            log.error(f"Summary sheet mapping failed: {err}")

        data = DataContainer({f"ENRICHED_{name}": data})

        log.info(f"Writing Enriched CANS output to shared folder for {file}")
        output_folder = shared_folder()
        data.export(output_folder, "", "csv")

        log.info(f"Writing Enriched CANS output to reports folder for {file}")
        reports_folder = opendir_location(workspace_folder(), "current/cans").makedirs(
            "ENRICHED", recreate=True
        )
        data.export(reports_folder, "", "csv")


@op(
    ins={
        "session_folder": In(FS),
    },
)
def transform_cans_data(
    session_folder: FS,
):
    """
    Transform CANS data by pivoting it to be vertical instead of horizontal
    and export enriched files to shared folder
    """
    files = session_folder.listdir("/")
    log.info(f"Files in session folder: {files}")

    category_mapping = load_category_mapping()
    summary_sheet_mapping = load_summary_sheet_mapping()

    for file in files:
        log.info(f"Transforming CANS data for {file}")
        # assign name of file without suffix
        name, _ = os.path.splitext(file)
        # get table name from file name
        table_name = "_".join(name.split("_")[-2:])
        # fetch the correct mapping for the table
        file_category_mapping = category_mapping[table_name]
        file_summary_sheet_mapping = summary_sheet_mapping[table_name]
        identifier_col = "Child Unique ID" if table_name == "0_5" else "Youth Unique ID"

        data = open_file(session_folder, file)

        try:
            subcategory_cols = [col for col in file_category_mapping if col in data.columns]

            vertical_cans = data.melt(
                id_vars=[identifier_col, "Assessment Date"],
                value_vars=subcategory_cols,
                var_name="CANS subcategory",
                value_name="Score",
            )
            vertical_cans["CANS category"] = vertical_cans["CANS subcategory"].map(file_category_mapping)
            vertical_cans = vertical_cans[[identifier_col, "Assessment Date", "CANS category", "CANS subcategory", "Score"]]

            # Replace empty strings with NA and drop rows with NA in the "Score" column
            vertical_cans["Score"] = vertical_cans["Score"].replace(r"^\s*$", pd.NA, regex=True)
            vertical_cans = vertical_cans.dropna(subset=["Score"])

            vertical_cans["Summary for Planning"] = vertical_cans.apply(
                map_summary_planning,
                axis=1,
                args=(file_summary_sheet_mapping,),
            )

            vertical_cans = vertical_cans.sort_values(by=[identifier_col, "Assessment Date"])

        except TypeError as err:
            log.error(f"Transforming CANS data failed: {err}")

        data = DataContainer({f"PAN_CDM_{name}": vertical_cans})

        log.info(f"Writing Pan Common Data Model CANS output to shared folder for {file}")
        output_folder = shared_folder()
        data.export(output_folder, "", "csv")


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_pan_cans_session_folder() -> FS:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesCANSMapping
    )
    """
    Create CANS session folder and move required files for mapping into it
    """
    log.info("Creating CANS session folder")
    session_folder = session_folder.opendir(SessionNamesCANSMapping.INCOMING_FOLDER)

    current_folder = opendir_location(workspace_folder(), "current/cans/PAN_CDM")
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
def joins_pan_cans(
    session_folder: FS,
):
    log.info("Checking necessary files are present...")
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")

    # CANS file patterns
    child_pattern = re.compile(r"0_5")
    youth_pattern = re.compile(r"6_21")

    # SSDA903 file patterns
    episodes_pattern = re.compile(r"episodes")

    files = session_folder.listdir("/")

    child_file = next((f for f in files if child_pattern.search(f)), None)
    youth_file = next((f for f in files if youth_pattern.search(f)), None)

    if child_file is None and youth_file is None:
        log.error("No CANS file to open")
        log.info("Exiting run as CANS file not available")
        return

    cans_patterns = [
        child_pattern,
        youth_pattern,
    ]

    # If no SSDA903 or CANS terminate process
    if not any(
        any(pattern.search(f) for f in files) for pattern in cans_patterns
    ) and not any(pattern.search(f) for f in files for pattern in [episodes_pattern]):
        log.error("No SSDA903 or CANS files found: terminating process.")
        return

    # Open the CANS files
    if child_file is not None:
        child_file = open_file(session_folder, child_file)

    if youth_file is not None:
        youth_file = open_file(session_folder, youth_file)

    # Check and process SSDA903 episodes file
    if "ssda903" in allowed_datasets:
        if any(episodes_pattern.search(f) for f in files):
            log.info("Joining CANS data with SSDA903 episodes data")
            episodes_file = next(f for f in files if episodes_pattern.search(f))
            episodes = open_file(session_folder, episodes_file)
            if child_file is not None:
                child_file = join_episodes_data(child_file, episodes, identifier_col="Child Unique ID")
            if youth_file is not None:
                youth_file = join_episodes_data(youth_file, episodes, identifier_col="Youth Unique ID")
        else:
            log.error("No 903 episodes data to join with CANS")
            empty_episodes_cols = ["EPISODE_ID"]
            for col in empty_episodes_cols:
                child_file[col] = None
                youth_file[col] = None

    # Export CANS files
    if child_file is not None:
        cans_child_dc = DataContainer({"PAN_CDM_cans_0_5": child_file})
        log.info("Writing joined CANS child file output to shared folder")
        output_folder = shared_folder()
        cans_child_dc.export(output_folder, "", "csv")
    if youth_file is not None:
        cans_youth_dc = DataContainer({"PAN_CDM_cans_6_21": youth_file})
        log.info("Writing joined CANS youth file output to shared folder")
        output_folder = shared_folder()
        cans_youth_dc.export(output_folder, "", "csv")