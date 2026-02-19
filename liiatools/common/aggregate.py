import os
import re
from typing import Iterable, List

import pandas as pd
import psutil
from dagster import get_dagster_logger
from fs.base import FS

from liiatools.common.archive import _normalise_table
from liiatools.common.data import DataContainer, PipelineConfig

p = psutil.Process(os.getpid())
logger = get_dagster_logger()


def snap(msg):
    rss = p.memory_info().rss / (1024**2)
    logger.info(f"[create_reports] {msg} | rss_mb={rss:.1f}")


class DataframeAggregator:
    """
    The dataframe aggregator aggregates dataframes that are stored in a filesystem.

    Only tables and columns defined in the pipeline config are aggregated.
    """

    def __init__(self, fs: FS, config: PipelineConfig, dataset: str):
        self.fs = fs
        self.config = config
        self.dataset = dataset

    def list_files(self) -> List[str]:
        """
        List the files in the current directory.
        """
        return sorted(self.fs.listdir("/"))

    def current(self, deduplicate: bool = False) -> DataContainer:
        """
        Get the current session as a datacontainer.
        """
        snap("start of running current")
        files = self.list_files()
        snap("listed files")
        return self.combine_files(files, deduplicate)

    def load_file(self, file) -> DataContainer:
        """
        Load a file from the current directory.
        """
        data = DataContainer()
        table_id = re.search(rf"{self.dataset}_([a-zA-Z0-9_]*)\.", file)

        for table_spec in self.config.table_list:
            if table_id and table_id.group(1) == table_spec.id:
                with self.fs.open(file, "r") as f:
                    df = pd.read_csv(f)
                    df = _normalise_table(df, table_spec)
                    data[table_spec.id] = df

        return data

    def combine_files(self, files: Iterable[str], deduplicate: bool) -> DataContainer:
        """Combine a list of files into a single dataframe."""
        tables: dict[str, list[pd.DataFrame]] = {}
        snap("starting to combine files")
        for file in files:
            loaded = self.load_file(file)
            for table_id, df in loaded.items():
                tables.setdefault(table_id, []).append(df)

        snap("before final concat")
        combined = DataContainer()
        for table_id, dfs in tables.items():
            if len(dfs) == 1:
                combined[table_id] = dfs[0]
            else:
                combined[table_id] = pd.concat(dfs, ignore_index=True, copy=False)

        # release list of inputs ASAP
        tables.clear()

        snap("before deduplication")
        if deduplicate:
            combined = self.deduplicate(combined)

        snap("after deduplication")
        return combined

    def deduplicate(self, data: DataContainer) -> DataContainer:
        """
        Deduplicate the dataframes in the container.

        If a dataframe has a 'sort' configuration, then the dataframe is sorted by the specified columns before deduplication.
        """
        for table_spec in self.config.table_list:
            if table_spec.id in data:
                sort_tuples = table_spec.sort_keys

                df = data[table_spec.id]
                if sort_tuples:
                    by = [col_id for col_id, _ in sort_tuples]
                    asc = [asc for _, asc in sort_tuples]
                    df = df.sort_values(by=by, ascending=asc)
                df = df.drop_duplicates(
                    subset=[c.id for c in table_spec.columns if c.unique_key]
                    if [c.id for c in table_spec.columns if c.unique_key]
                    else None,
                    keep="last",
                )
                data[table_spec.id] = df

        return data
