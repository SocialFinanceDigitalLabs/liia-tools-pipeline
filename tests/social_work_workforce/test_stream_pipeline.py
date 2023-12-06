from fs import open_fs

from liiatools.csww_pipeline.spec import load_schema, load_schema_path
from liiatools.csww_pipeline.spec.samples import CSWW_2022
from liiatools.csww_pipeline.spec.samples import DIR as SAMPLES_DIR
from liiatools.csww_pipeline.stream_pipeline import task_cleanfile
from liiatools.common.data import FileLocator


def test_task_cleanfile():
    samples_fs = open_fs(SAMPLES_DIR.as_posix())
    locator = FileLocator(samples_fs, CSWW_2022.name)

    result = task_cleanfile(
        locator, schema=load_schema(2022), schema_path=load_schema_path(2022)
    )

    data = result.data
    errors = result.errors

    assert len(data) == 3
    assert len(data["Worker"]) == 1
    assert len(data["Worker"].columns) == 23
    assert len(data["LA_Level"]) == 1
    assert len(data["LA_Level"].columns) == 3

    assert errors[0]["type"] == "ConversionError"
    assert errors[0]["message"] == "Could not convert to date"
    assert errors[0]["exception"] == "Invalid date: 2023-03-28T14:54:55Z"
    assert errors[0]["filename"] == "social_work_workforce_2022.xml"
    assert errors[0]["header"] == "DateTime"
