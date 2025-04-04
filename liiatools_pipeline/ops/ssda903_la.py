import re

import pandas as pd
from dagster import In, Out, get_dagster_logger, op
from fs.base import FS

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesFixEpisodes
from liiatools.common.data import DataContainer
from liiatools.ssda903_pipeline.fix_episodes import stage_1, stage_2
from liiatools_pipeline.assets.common import shared_folder, workspace_folder

log = get_dagster_logger()


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_fix_episodes_session_folder() -> FS:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesFixEpisodes
    )
    session_folder = session_folder.opendir(SessionNamesFixEpisodes.INCOMING_FOLDER)

    concat_folder = shared_folder().opendir("concatenated/ssda903")
    pl.move_files_for_sharing(
        concat_folder, session_folder, required_table_id=["episodes"]
    )

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def fix_episodes(
    session_folder: FS,
):
    log.info("Opening Concatenated folder for 903...")
    concat_folder = shared_folder().opendir("concatenated/ssda903")
    files = session_folder.listdir("/")

    for file in files:
        log.info(f"Fixing episodes for {file}")
        data = DataContainer()
        episode_table = re.search(r"episodes", file)
        la_code = re.search(r"([A-Za-z0-9]*)_", file)
        if episode_table is not None:
            log.info("Episode table found...")
            with session_folder.open(file, "r") as f:
                try:
                    df = pd.read_csv(f)
                    df = stage_1(df)
                    df = stage_2(df)
                    data[episode_table.group(0)] = df
                except TypeError as err:
                    log.error(f"Fixing episodes table failed: {err}")
        log.info(f"Exporting episodes fix for {file}...")
        data.export(concat_folder, f"{la_code.group(1)}_ssda903_", "csv")
