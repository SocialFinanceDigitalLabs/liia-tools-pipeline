import pandas as pd
import pytest
from fs import open_fs

from liiatools.common.data import DataContainer


@pytest.fixture
def sample_data():
    return DataContainer(
        {
            "table1": pd.DataFrame(
                [{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]
            ),
            "table2": pd.DataFrame(
                [
                    {"id": 1, "date": pd.to_datetime("2022-01-01")},
                    {"id": 2, "date": pd.to_datetime("2022-05-03")},
                ]
            ),
        }
    )


def test_to_dataset(sample_data: DataContainer):
    dataset = sample_data.to_dataset("table1")
    assert dataset
    assert dataset.title == "table1"
    assert dataset.headers == ["id", "name"]


def test_to_databook(sample_data: DataContainer):
    databook = sample_data.to_databook()
    assert databook
    assert len(databook.sheets()) == 2
    assert databook.sheets()[0].title == "table1"
    assert databook.sheets()[1].title == "table2"


def test_export(sample_data: DataContainer):
    fs = open_fs("mem://")
    sample_data.export(fs, "test_", format="csv")

    assert fs.exists("test_table1.csv")
    assert fs.exists("test_table2.csv")

    sample_data.export(fs, "excel_test", format="xlsx")
    assert fs.exists("excel_test.xlsx")


def test_export_parquet(sample_data: DataContainer):
    fs = open_fs("mem://")
    sample_data.export(fs, "test_", format="parquet")

    assert fs.exists("test_table1.parquet")
    assert fs.exists("test_table1.parquet")


def test_export_csv_chunked_creates_multiple_files(tmp_path):
    import pandas as pd
    from liiatools.common.data.__container import DataContainer
    from tablib import Dataset
    from fs.memoryfs import MemoryFS

    # Create a DataContainer with enough rows to force chunking
    df = pd.DataFrame({"a": range(100), "b": ["x" * 50] * 100})
    data = DataContainer({"bigtable": df})
    dataset = data.to_dataset("bigtable")
    fs = MemoryFS()

    # Use a very small max_file_size_mb to force chunking
    data._export_csv_chunked(fs, "chunked_", "bigtable", dataset, max_file_size_mb=0.001)

    # Should create multiple part files
    files = list(fs.walk.files())
    part_files = [f for f in files if "part" in f]
    assert len(part_files) > 1
    # All files should be non-empty
    for f in part_files:
        with fs.open(f, "r") as file:
            content = file.read()
            assert content.strip() != ""


def test_export_csv_chunked_creates_single_file_when_small():
    import pandas as pd
    from liiatools.common.data.__container import DataContainer
    from tablib import Dataset
    from fs.memoryfs import MemoryFS

    df = pd.DataFrame({"a": [1, 2], "b": ["foo", "bar"]})
    data = DataContainer({"smalltable": df})
    dataset = data.to_dataset("smalltable")
    fs = MemoryFS()

    # Large enough chunk size to avoid splitting
    data._export_csv_chunked(fs, "single_", "smalltable", dataset, max_file_size_mb=1)

    files = list(fs.walk.files())
    assert len(files) == 1
    assert files[0].endswith("smalltable.csv")
    with fs.open(files[0], "r") as file:
        content = file.read()
        assert "a,b" in content
        assert "foo" in content
