import pandas as pd
import pytest
from fs import open_fs

from liiatools.common.archive import DataframeArchive
from liiatools.common.data import ColumnConfig, PipelineConfig, TableConfig


@pytest.fixture
def cfg():
    cfg = PipelineConfig(
        retention_columns={"year_column": "Year", "la_column": "LA"},
        retention_period={"PAN": 12, "SUFFICIENCY": 7},
        la_signed={
            "BAR": "Yes",
            "CAM": "No",
        },
        table_list=[
            TableConfig(
                id="table1",
                columns=[
                    ColumnConfig(id="id", type="integer", unique_key=True),
                    ColumnConfig(id="name", type="string"),
                ],
            ),
            TableConfig(
                id="table2",
                columns=[
                    ColumnConfig(id="id", type="integer", unique_key=True),
                    ColumnConfig(id="date", type="date"),
                ],
            ),
        ],
    )
    return cfg


@pytest.fixture(scope="function")
def fs():
    fs = open_fs("mem://")
    return fs


@pytest.fixture
def archive(fs, cfg: PipelineConfig) -> DataframeArchive:
    archive = DataframeArchive(fs, cfg, "ssda903")
    return archive


def test_archive(archive: DataframeArchive):
    la_code = "BAR"
    year = 2022
    dataset = {
        "table1": pd.DataFrame([{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]),
        "table2": pd.DataFrame(
            [
                {"id": 1, "date": pd.to_datetime("2022-01-01")},
                {"id": 2, "date": pd.to_datetime("2022-05-03")},
            ]
        ),
    }
    archive.add(dataset, la_code, year, month=None)

    snapshots = archive.list_snapshots()
    assert snapshots == {
        la_code: ["BAR/ssda903/BAR_2022_table1.csv", "BAR/ssda903/BAR_2022_table2.csv"]
    }

    snap = archive.load_snapshot("BAR/ssda903/BAR_2022_table1.csv")
    assert snap["table1"].shape == (2, 2)
    assert snap["table1"]["name"].tolist() == ["foo", "bar"]


def test_combine(archive: DataframeArchive):
    la_code = "BAR"
    year = 2022
    dataset = {
        "table1": pd.DataFrame([{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]),
    }
    archive.add(dataset, la_code, year, month=None)
    current = archive.current(la_code)
    table_1 = current["table1"]

    assert sorted(table_1.id.tolist()) == [1, 2]
    assert sorted(table_1.name.tolist()) == sorted(["foo", "bar"])

    dataset = {
        "table1": pd.DataFrame([{"id": 4, "name": "SNAFU"}]),
    }
    archive.add(dataset, la_code, year, month=None)
    current = archive.current(la_code)
    table_1 = current["table1"]

    assert sorted(table_1.id.tolist()) == [4]
    assert sorted(table_1.name.tolist()) == sorted(["SNAFU"])
