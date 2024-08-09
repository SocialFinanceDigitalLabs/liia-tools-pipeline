import re
import pandas as pd
from dagster import In, Out, op
from fs.base import FS

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNames, SessionNamesFixEpisodes
from liiatools.common.data import FileLocator, ErrorContainer, DataContainer
from liiatools.ssda903_pipeline.fix_episodes import stage_1, stage_2

from liiatools_pipeline.assets.common import (
    workspace_folder,
    shared_folder,
)


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
        concat_folder, session_folder, required_table_id="episodes"
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
    concat_folder = shared_folder().opendir("concatenated/ssda903")
    files = session_folder.listdir("/")

    for file in files:
        data = DataContainer()
        episode_table = re.search(r"episodes", file)
        la_code = re.search(r"([A-Za-z0-9]*)_", file)
        if episode_table is not None:
            with session_folder.open(file, "r") as f:
                df = pd.read_csv(f)
                df = stage_1(df)
                df = stage_2(df)
                data[episode_table.group(0)] = df

        data.export(concat_folder, f"{la_code.group(1)}_ssda903_", "csv")