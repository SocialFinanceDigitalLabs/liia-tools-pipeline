import re

import pandas as pd
from dagster import In, Out, get_dagster_logger, op
from fs.base import FS

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesCINDedup
from liiatools.common.data import DataContainer
from liiatools.cin_census_pipeline.dedup_cin import dedup_cin
from liiatools_pipeline.assets.common import shared_folder, workspace_folder

log = get_dagster_logger()

@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_cin_dedup_session_folder() -> FS:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesCINDedup
    )
    session_folder = session_folder.opendir(SessionNamesCINDedup.INCOMING_FOLDER)

    concat_folder = shared_folder().opendir("concatenated/cin")
    pl.move_files_for_sharing(
        concat_folder, session_folder
    )

    return session_folder

@op(
    ins={
        "session_folder": In(FS),
    },
)
def cin_deduplication(
    session_folder: FS,
):
    log.info("Opening Concatenated folder for CIN...")
    concat_folder = shared_folder().opendir("concatenated/cin")
    files = session_folder.listdir("/")

    for file in files:
        log.info(f"Deduplicating for {file}")
        data = DataContainer()
        cin_table = re.search(r"cin", file)
        la_code = re.search(r"([A-Za-z0-9]*)_", file)
        with session_folder.open(file, "r") as f:
            try:
                df = pd.read_csv(f)
                df = dedup_cin(df)
                data[cin_table.group(0)] = df
            except TypeError as err:
                log.error(f"Deduplicating {file} failed: {err}")
        log.info(f"Exporting deduplicated data for {file}...")
        data.export(concat_folder, f"{la_code.group(1)}_cin_", "csv")