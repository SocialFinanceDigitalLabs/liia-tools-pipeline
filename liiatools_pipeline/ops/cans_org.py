import os
from importlib import resources

import pandas as pd
from dagster import In, Out, get_dagster_logger, op
from fs.base import FS
from ruamel.yaml import YAML

from liiatools.cans_pipeline.summary_sheet_mapping import add_summary_sheet_columns
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

    with resources.files("liiatools").joinpath(
        "cans_pipeline", "spec", "summary_sheet_mapping.yml"
    ).open("r") as f:
        mapping = yaml.load(f)

    with resources.files("liiatools").joinpath(
        "cans_pipeline", "spec", "summary_column_order.yml"
    ).open("r") as f:
        column_order = yaml.load(f)

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
