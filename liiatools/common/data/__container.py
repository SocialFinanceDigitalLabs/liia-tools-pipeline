import logging
import io
import csv
from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
from fs.base import FS
from tablib import Databook, Dataset, import_set
from tablib.formats import registry as tablib_registry

logger = logging.getLogger(__name__)


class DataContainer(Dict[str, pd.DataFrame]):
    """
    DataContainer is a dictionary of DataFrames, with some helper methods to convert to tablib objects and export to filesystems.

    The object can be passed around between pipeline jobs.
    """

    def to_dataset(self, key: str) -> Dataset:
        dataset = import_set(self[key], "df")
        dataset.title = key
        return dataset

    def to_databook(self) -> Databook:
        return Databook([self.to_dataset(k) for k in self.keys()])

    def copy(self) -> "DataContainer":
        """
        Returns a deep copy of the DataContainer
        """
        return DataContainer({k: v.copy() for k, v in self.items()})

    def export(self, fs: FS, basename: str, format="csv", max_file_size_mb=None):
        """
        Export the data to a filesystem. Supports any format supported by tablib, plus parquet.

        If the format supports multiple sheets (e.g. xlsx), then each table will be exported to a separate sheet in the same file,
        otherwise each table will be exported to a separate file.
        """
        logger.debug("Exporting data to %s", basename)
        if format == "parquet":
            return self._export_parquet(fs, basename)

        fmt = tablib_registry.get_format(format)
        fmt_ext = fmt.extensions[0]

        if hasattr(fmt, "export_book"):
            book = self.to_databook()
            data = book.export(format)
            self._write(fs, f"{basename}.{fmt_ext}", data)
        else:
            for table_name in self:
                dataset = self.to_dataset(table_name)

                if max_file_size_mb and format=="csv":
                    self._export_csv_chunked(fs, basename, table_name, dataset, max_file_size_mb)
                else:
                    data = dataset.export(format)
                    self._write(fs, f"{basename}{table_name}.{fmt_ext}", data)

    def _export_parquet(self, fs: FS, basename: str):
        for table_name in self:
            df = self[table_name]
            with fs.open(f"{basename}{table_name}.parquet", "wb") as f:
                df.to_parquet(f, index=False)

    def _write(self, fs: FS, path: str, data: Any):
        format = "wt" if isinstance(data, str) else "wb"
        with fs.open(path, format) as f:
            if isinstance(data, str):
                data = data.replace("<NA>", "").replace("nan", "").replace("NaT", "")
            f.write(data)

    def _export_csv_chunked(self, fs, basename, table_name, dataset, max_file_size_mb):

        max_bytes = max_file_size_mb * 1024 * 1024

        part = 1
        split_occurred = False

        buffer = io.StringIO()
        writer = csv.writer(buffer)

        # Write header
        writer.writerow(dataset.headers)

        for row in dataset:
            row_buffer = io.StringIO()
            row_writer = csv.writer(row_buffer)
            row_writer.writerow(row)

            row_data = row_buffer.getvalue()
            row_size = len(row_data.encode("utf-8"))

            # Flush current file if writing row would exceed max size
            if buffer.tell() + row_size > max_bytes:
                split_occurred = True
                filename = f"{basename}{table_name}_part{part}.csv"
                self._write(fs, filename, buffer.getvalue())

                part += 1
                buffer = io.StringIO()
                writer = csv.writer(buffer)
                writer.writerow(dataset.headers)

            writer.writerow(row)

        # Final write
        if split_occurred:
            filename = f"{basename}{table_name}_part{part}.csv"
        else:
            filename = f"{basename}{table_name}.csv"
        self._write(fs, filename, buffer.getvalue())


class ErrorContainer(List[Dict[str, Any]]):
    """
    Used for holding data quality errors during processing. Can be used to filter the list of errors by a property so can limit to specific context, e.g. for a particular file.
    """

    def set_property(self, prop: str, value: Any, override: bool = False):
        """
        Sets the property on all errors in the container. If override is False, then the property will only be set if it is not already set.
        """
        for e in self:
            if override or prop not in e:
                e[prop] = value

    def filter(self, prop: str, value: Any) -> "ErrorContainer":
        """
        Returns a new ErrorContainer with only the errors that have the specified property set to the specified value.
        """
        return ErrorContainer([e for e in self if e.get(prop) == value])

    def with_prop(self, prop: str) -> "ErrorContainer":
        """
        Returns a new ErrorContainer with only the errors that have the specified property set.
        """
        return ErrorContainer([e for e in self if prop in e])

    def to_dataframe(self):
        """
        Converts the ErrorContainer to a pandas DataFrame
        """
        return pd.DataFrame(self)


@dataclass
class ProcessResult:
    data: DataContainer
    errors: ErrorContainer
