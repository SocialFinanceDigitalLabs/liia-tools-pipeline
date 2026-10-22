import pandas as pd
import pytest
from fs import open_fs

from liiatools.common.data import FileLocator, PipelineConfig
from liiatools.common.spec.__data_schema import Column, DataSchema
from liiatools.common.transform import prepare_export
from liiatools.ssda903_pipeline.stream_pipeline import task_cleanfile


@pytest.mark.parametrize("format", ["csv", "parquet"])
@pytest.mark.parametrize("include_row_number", [True, False])
def test_cleanfile_row_numbers_in_configured_exports(format, include_row_number):
    schema = DataSchema(column_map={"header": {"CHILD": Column(string="alphanumeric")}})
    columns = [{"id": "CHILD", "type": "string"}]
    if include_row_number:
        columns.append({"id": "row_number", "type": "integer"})
    config = PipelineConfig(
        sensor_trigger={},
        retention_columns={},
        retention_period={},
        degrade_at_clean={},
        reports_to_shared={},
        la_signed={},
        table_list=[{"id": "header", "retain": ["test"], "columns": columns}],
    )

    with open_fs("mem://") as filesystem:
        filesystem.writetext("input.csv", "CHILD\n123\n456\n")
        result = task_cleanfile(FileLocator(filesystem, "input.csv"), schema)

        assert not result.errors
        assert result.data["header"].to_dict("records") == [
            {"CHILD": "123", "row_number": 2},
            {"CHILD": "456", "row_number": 3},
        ]

        result.data["header"] = result.data["header"].iloc[::-1]
        prepared = prepare_export(result.data, config, "test")
        prepared.export(filesystem, "cleaned_", format)

        with filesystem.open(f"cleaned_header.{format}", "rb") as output:
            exported = (
                pd.read_csv(output) if format == "csv" else pd.read_parquet(output)
            )

        assert exported["CHILD"].astype(str).tolist() == ["456", "123"]
        if include_row_number:
            assert exported["row_number"].tolist() == [3, 2]
        else:
            assert "row_number" not in exported.columns
