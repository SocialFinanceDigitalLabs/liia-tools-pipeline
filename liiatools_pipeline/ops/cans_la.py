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
    session_folder = session_folder.opendir(SessionNamesCANSMapping.INCOMING_FOLDER)

    current_folder = opendir_location(workspace_folder(), "current")
    pl.move_files_for_sharing(
        current_folder,
        session_folder,
        required_table_id=["0_5", "6_21"],
    )
    files = session_folder.listdir("/")
    log.info(f"Files in session folder: {files}")

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def cans_summary_sheet_mapping(
    session_folder: FS,
):
    current_folder = opendir_location(workspace_folder(), "current")
    files = session_folder.listdir("/")
    log.info(f"Files in session folder: {files}")

    with resources.files("liiatools").joinpath(
        "cans_pipeline", "spec", "summary_sheet_mapping.yml"
    ).open("r") as f:
        mapping = yaml.load(f)

    for file in files:
        log.info(f"Adding Summary Sheet mapping for {file}")
        name, _ = os.path.splitext(file)
        table_name = "_".join(name.split("_")[-2:])
        la_code = "_".join(name.split("_")[:1])
        file_mapping = mapping[table_name]

        log.info(f"name {name}, table_name {table_name}")

        with session_folder.open(file, "r") as f:
            try:
                data = pd.read_csv(f)
                data = add_summary_sheet_columns(data, file_mapping)
            except TypeError as err:
                log.error(f"Summary sheet mapping failed: {err}")
        log.info(f"Exporting summary sheet mapping for {file}...")
        data = DataContainer({table_name: data})

        # File prefix is everything but the table name, might include years, months etc.
        file_prefix = "_".join(name.split("_")[:-2])

        with current_folder.opendir(la_code) as la_folder:
            with la_folder.opendir("cans") as subfolder:
                data.export(subfolder, f"{file_prefix}_", "csv")
