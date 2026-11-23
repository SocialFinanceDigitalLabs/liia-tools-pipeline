from unittest.mock import MagicMock

from fs.memoryfs import MemoryFS

from liiatools_pipeline.jobs.common_la import (
    clean,
    concatenate,
    move_current_la,
    no_op_job,
    start_clean_dataset,
)
from liiatools_pipeline.ops import common_la


def test_no_op_job():
    result = no_op_job.execute_in_process()

    assert result.success


def test_start_clean_dataset():
    result = start_clean_dataset.execute_in_process()

    assert result.success


def test_clean_job(monkeypatch):
    monkeypatch.setenv(
        "WORKSPACE_LOCATION",
        "mem://workspace",
    )

    monkeypatch.setenv(
        "HASH_SECRET_KEY",
        "test-secret",
    )

    monkeypatch.setenv(
        "HASH_SECRET_VERSION",
        "test-version",
    )

    test_config = {
        "dataset": "ssda903",
        "dataset_folder": "mem://incoming",
        "la_folder": "mem://incoming",
        "input_la_code": "TT1",
    }

    result = clean.execute_in_process(
        run_config={
            "ops": {
                "create_session_folder": {
                    "config": test_config,
                },
                "open_current": {
                    "config": test_config,
                },
                "process_files": {
                    "config": test_config,
                },
            },
        }
    )

    assert result.success


def test_move_current_la_job(monkeypatch):
    test_fs = MemoryFS()

    test_fs.makedirs("current")

    monkeypatch.setattr(
        common_la,
        "workspace_folder",
        lambda: test_fs,
    )

    result = move_current_la.execute_in_process()

    assert result.success


def test_concatenate_job(monkeypatch):
    archive_fs = MemoryFS()
    shared_fs = MemoryFS()

    monkeypatch.setattr(
        common_la,
        "workspace_folder",
        lambda: archive_fs,
    )

    monkeypatch.setattr(
        common_la,
        "shared_folder",
        lambda: shared_fs,
    )

    output_config = MagicMock()
    output_config.table_list = []

    monkeypatch.setattr(
        common_la,
        "pipeline_config",
        lambda config: output_config,
    )

    test_config = {
        "dataset": "ssda903",
        "dataset_folder": "mem://incoming",
        "la_folder": "mem://incoming",
        "input_la_code": "TT1",
    }

    result = concatenate.execute_in_process(
        run_config={
            "ops": {
                "open_current": {
                    "config": test_config,
                },
                "create_concatenated_view": {
                    "config": test_config,
                },
            },
        }
    )

    assert result.success

    assert shared_fs.exists("concatenated/ssda903")
