import os
import shutil
import pytest
from dagster import execute_job
from liiatools_pipeline.ops.common_la import create_session_folder  # Replace 'your_module' with the actual module name


def test_create_session_folder():
    result = create_session_folder.execute_in_process()

    if result is None:
        raise Exception("execute_op_in_process returned None. There might be an issue with the op execution.")

    # Ensure the op was successful
    assert result.success, f"Op failed with errors: {result.all_events}"

