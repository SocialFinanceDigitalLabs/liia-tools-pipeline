import re
from typing import Iterable, List

import pandas as pd
from fs.base import FS

from liiatools.common.archive import _normalise_table
from liiatools.common.data import DataContainer, PipelineConfig


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

    def current(self) -> DataContainer:
        """
        Get the current session as a datacontainer.
        """
        files = self.list_files()
        return self.combine_files(files)

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

    def combine_files(
        self,
        files: Iterable[str],
    ) -> DataContainer:
        """
        Combine a list of files into a single dataframe.

        """
        combined = DataContainer()
        for file in files:
            combined = self._combine_files(
                combined,
                self.load_file(file),
            )

        return combined

    def _combine_files(self, *sources: DataContainer) -> DataContainer:
        """
        Combine a new files into an existing set of dataframes.
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

        return data
