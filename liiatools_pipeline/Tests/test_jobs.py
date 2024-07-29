import pytest
import os

from dagster import ExecuteInProcessResult

from liiatools_pipeline.jobs.common_la import clean, move_current, concatenate
from liiatools_pipeline.jobs.common_org import reports


def test_clean():
    result = clean.execute_in_process()
    assert result.success, f"Clean job failed with result: {result}"
    # assert os.path.exists('expected_file_path'), "Expected file was not created."


def test_move_current():
    result = move_current.execute_in_process()
    assert result.success, f"Move current job failed with result: {result}"


def test_concatenate():
    result = concatenate.execute_in_process()
    assert result.success, f"Concatenate job failed with result: {result}"


def test_reports():
    result = reports.execute_in_process()
    # assert isinstance(result, ExecuteInProcessResult)
    assert result.success, f"Reports job failed with result: {result}"


# if __name__ == "__main__":
#     pytest.main()


