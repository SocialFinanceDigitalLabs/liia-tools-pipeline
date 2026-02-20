import re
from typing import Dict, Iterable, Literal

import dask.dataframe as dd
import fs.errors
import pandas as pd
from dagster import get_dagster_logger
from fs.base import FS

from liiatools.common.data import (
    DataContainer,
    ErrorContainer,
    PipelineConfig,
    ProcessResult,
    TableConfig,
)

log = get_dagster_logger(__name__)


def _normalise_table(
    df: pd.DataFrame | dd.DataFrame, table_spec: TableConfig
) -> pd.DataFrame | dd.DataFrame:
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

    def add(
        self,
        data: DataContainer,
        la_code: str,
        year: int,
        month: str | None,
        term: str | None,
        school_type: str | None,
        identifier: str | None,
    ):
        """
        Add a new snapshot to the archive.
        """
        la_dir = self.fs.makedirs(f"{la_code}/{self.dataset}", recreate=True)

        for table_spec in self.config.table_list:
            if table_spec.id in data:
                self._add_table(
                    la_dir,
                    la_code,
                    year,
                    month,
                    term,
                    school_type,
                    identifier,
                    table_spec,
                    data[table_spec.id],
                )

    def _add_table(
        self,
        la_dir: FS,
        la_code: str,
        year: int,
        month: str | None,
        term: str | None,
        school_type: str | None,
        identifier: str | None,
        table_spec: TableConfig,
        df: pd.DataFrame,
    ):
        """
        Add a table to the archive.
        """
        if identifier is not None and month is not None:
            path = f"{la_code}_{identifier}_{year}_{month}_{table_spec.id}.csv"
        elif term is not None and school_type is not None:
            path = f"{la_code}_{year}_{term}_{school_type}_{table_spec.id}.csv"
        elif term is not None and school_type is None:
            path = f"{la_code}_{year}_{term}_{table_spec.id}.csv"
        elif month is not None:
            path = f"{la_code}_{year}_{month}_{table_spec.id}.csv"
        else:
            path = f"{la_code}_{year}_{table_spec.id}.csv"

        with la_dir.open(path, "w") as f:
            df = _normalise_table(df, table_spec)
            df.to_csv(f, index=False)

    def list_snapshots(self) -> Dict:
        """
        List the snapshots in the archive.
        """
        try:
            directories = sorted(self.fs.listdir("/"))
        except fs.errors.ResourceNotFound:
            log.error(f"Resource not found error when listing snapshots")
            return {}
        la_snapshots = {}

        for directory in directories:
            try:
                snapshots = self.fs.listdir(f"/{directory}/{self.dataset}")
                for snap in snapshots:
                    la_snapshots.setdefault(directory, []).append(
                        f"{directory}/{self.dataset}/{snap}"
                    )
            except fs.errors.ResourceNotFound:
                log.error(f"Resource not found error: /{directory}/{self.dataset}")
                continue

        return la_snapshots

    def delete_snapshot(self, *snap_ids: str):
        """
        Deletes one or more snapshots from the archive.
        """
        assert len(snap_ids) > 0, "At least one snapshot must be specified"

        for snap_id in snap_ids:
            self.fs.removetree(snap_id)

    def current(
        self, la_code: str, deduplicate_mode: Literal["E", "A", "N"] = "E"
    ) -> DataContainer:
        """
        Get the current session as a datacontainer.
        """
        try:
            directories = self.list_snapshots()
            snap_ids = directories[la_code]
            return self.combine_snapshots(snap_ids, deduplicate_mode)

        except KeyError:
            return

    def load_snapshot(self, snap_id) -> DataContainer:
        """
        Load a snapshot from the archive.
        """
        data = DataContainer()
        table_id = re.search(
            r"_(?:\d{4}_)?\d{4}(?:_(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|autumn|spring|summer))?(?:_(?:acad|la))?_([a-zA-Z0-9_]+)\.",
            snap_id,
        )

        for table_spec in self.config.table_list:
            if table_id and table_id.group(1) == table_spec.id:
                log.info(f"table id match: {table_spec.id}")
                with self.fs.open(snap_id, "r") as f:
                    df = pd.read_csv(f)
                    df = _normalise_table(df, table_spec)
                    data[table_spec.id] = df

        return data

    def combine_snapshots(
        self, snap_ids: Iterable[str], deduplicate_mode: Literal["E", "A", "N"]
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
            combined = self.deduplicate(combined).data

        return combined

    def deduplicate(self, data: DataContainer) -> ProcessResult:
        """
        Deduplicate the dataframes in the container.

        If a dataframe has a 'sort' configuration, then the dataframe is sorted by the specified columns before deduplication.
        """
        errors = ErrorContainer()

        for table_spec in self.config.table_list:
            if table_spec.id in data:
                sort_tuples = table_spec.sort_keys

                df = data[table_spec.id]
                if sort_tuples:
                    by = [col_id for col_id, _ in sort_tuples]
                    asc = [asc for _, asc in sort_tuples]
                    df = df.sort_values(by=by, ascending=asc)

                subset = [c.id for c in table_spec.columns if c.unique_key]
                duplicate_mask = df.duplicated(
                    subset=subset if subset else None,
                    keep="first",
                )

                duplicate_rows = df.index[duplicate_mask].tolist()

                df = df[~duplicate_mask]

                for index in duplicate_rows:
                    # CIN xml file cannot give rows TO DO: add node information instead
                    if self.dataset == "cin":
                        errors.append(
                            dict(
                                type="DuplicateRemoval",
                                message=f"Row removed as it was a duplicate",
                                table_name=table_spec.id,
                            )
                        )
                    # For other csv files, row can be given as index + 2
                    else:
                        errors.append(
                            dict(
                                type="DuplicateRemoval",
                                message=f"Row {index + 2} removed as it was a duplicate",
                                row_number=index + 2,
                                table_name=table_spec.id,
                            )
                        )
                data[table_spec.id] = df

        return ProcessResult(data=data, errors=errors)

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
            data = self.deduplicate(data).data

        return data
