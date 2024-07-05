import logging
import re
from typing import Iterable, Literal, Dict

import pandas as pd
from fs.base import FS

from liiatools.common.data import DataContainer, PipelineConfig, TableConfig

logger = logging.getLogger(__name__)


def _normalise_table(df: pd.DataFrame, table_spec: TableConfig) -> pd.DataFrame:
    """
    Normalise the dataframe to match the table spec.
    """
    df = df.copy()

    # Add any columns that are in the table spec but not in the dataframe
    for c in table_spec.columns:
        if c.id not in df.columns:
            df[c.id] = None

    # Reorder and limit the columns to match the table spec
    df = df[[c.id for c in table_spec.columns]]

    return df


class DataframeArchive:
    """
    The dataframe archive is a collection of dataframes that are stored in a filesystem.

    Every time a set of dataframes are added, a new 'snapshot' is created. The complete archive
    is created by combining all the snapshots in chronological order.

    Snapshots can be 'rolled-up' to create a complete archive of the dataframes at a given point in time. When restoring
    at a point in time, the process will find the latest roll-up, and then apply the snapshots after that point.

    Only tables and columns defined in the pipeline config are stored in the archive.

    Because files are not always loaded in chronological order, the 'primary keys' and 'sort' configurations are used
    to ensure that the dataframes are deduplicated in the right order.
    """

    def __init__(self, fs: FS, config: PipelineConfig, dataset: str):
        self.fs = fs
        self.config = config
        self.dataset = dataset

    def add(self, data: DataContainer, la_code: str, year: int):
        """
        Add a new snapshot to the archive.
        """
        la_dir = self.fs.makedirs(f"{la_code}/{self.dataset}", recreate=True)

        for table_spec in self.config.table_list:
            if table_spec.id in data:
                self._add_table(la_dir, la_code, year, table_spec, data[table_spec.id])

    def _add_table(
        self,
        la_dir: FS,
        la_code: str,
        year: int,
        table_spec: TableConfig,
        df: pd.DataFrame,
    ):
        """
        Add a table to the archive.
        """
        with la_dir.open(f"{la_code}_{year}_{table_spec.id}.csv", "w") as f:
            df = _normalise_table(df, table_spec)
            df.to_csv(f, index=False)

    def list_snapshots(self) -> Dict:
        """
        List the snapshots in the archive.
        """
        directories = sorted(self.fs.listdir("/"))
        la_snapshots = {}

        for directory in directories:
            snapshots = self.fs.listdir(f"/{directory}/{self.dataset}")
            for snap in snapshots:
                la_snapshots.setdefault(directory, []).append(
                    f"{directory}/{self.dataset}/{snap}"
                )

        return la_snapshots

    def delete_snapshot(self, *snap_ids: str):
        """
        Deletes one or more snapshots from the archive.
        """
        assert len(snap_ids) > 0, "At least one snapshot must be specified"

        for snap_id in snap_ids:
            self.fs.removetree(snap_id)

    def current(self, la_code: str) -> DataContainer:
        """
        Get the current session as a datacontainer.
        """
        try:
            directories = self.list_snapshots()
            snap_ids = directories[la_code]
            return self.combine_snapshots(snap_ids)

        except KeyError:
            return

    def load_snapshot(self, snap_id) -> DataContainer:
        """
        Load a snapshot from the archive.
        """
        data = DataContainer()
        table_id = re.search(r"_([a-zA-Z0-9]*)\.", snap_id)

        for table_spec in self.config.table_list:
            if table_id and table_id.group(1) == table_spec.id:
                with self.fs.open(snap_id, "r") as f:
                    df = pd.read_csv(f)
                    df = _normalise_table(df, table_spec)
                    data[table_spec.id] = df

        return data

    def combine_snapshots(
        self, snap_ids: Iterable[str], deduplicate_mode: Literal["E", "A", "N"] = "E"
    ) -> DataContainer:
        """
        Combine a list of snapshots into a single dataframe.

        The deduplicate_mode parameter controls how the dataframes are deduplicated. The options are:
        * E: Deduplicate after each snapshot is added
        * A: Deduplicate after all snapshots are added
        * N: Do not deduplicate

        """
        assert deduplicate_mode in ["E", "A", "N"]

        combined = DataContainer()
        for snap_id in snap_ids:
            combined = self._combine_snapshots(
                combined,
                self.load_snapshot(snap_id),
                deduplicate=deduplicate_mode == "E",
            )

        if deduplicate_mode == "A":
            combined = self.deduplicate(combined)

        return combined

    def deduplicate(self, data: DataContainer) -> DataContainer:
        """
        Deduplicate the dataframes in the container.

        If a dataframe has a 'sort' configuration, then the dataframe is sorted by the specified columns before deduplication.
        """
        for table_spec in self.config.table_list:
            if table_spec.id in data:
                sort_keys = table_spec.sort_keys

                df = data[table_spec.id]
                if sort_keys:
                    df = df.sort_values(by=sort_keys, ascending=True)
                df = df.drop_duplicates(
                    subset=[c.id for c in table_spec.columns if c.unique_key]
                    if [c.id for c in table_spec.columns if c.unique_key]
                    else None,
                    keep="last",
                )
                data[table_spec.id] = df

        return data

    def _combine_snapshots(
        self, *sources: DataContainer, deduplicate: bool = True
    ) -> DataContainer:
        """
        Combine a new snapshot into an existing set of dataframes.
        """
        data = DataContainer()

        for table_spec in self.config.table_list:
            table_id = table_spec.id
            all_sources = []
            for source in sources:
                if table_id in source:
                    all_sources.append(source[table_id])

            if len(all_sources) == 0:
                continue
            elif len(all_sources) == 1:
                data[table_id] = all_sources[0].copy()
            else:
                data[table_id] = pd.concat(all_sources, ignore_index=True)

        if deduplicate:
            data = self.deduplicate(data)

        return data
