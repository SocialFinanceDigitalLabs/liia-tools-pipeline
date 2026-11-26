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
    workspace = MemoryFS()
    shared = MemoryFS()
    incoming = MemoryFS()

    monkeypatch.setattr(
        common_la,
        "workspace_folder",
        lambda: workspace,
    )

    monkeypatch.setattr(
        common_la,
        "shared_folder",
        lambda: shared,
    )

    monkeypatch.setattr(
        common_la,
        "open_fs",
        lambda location: incoming,
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
    workspace = MemoryFS()
    shared = MemoryFS()
    current = MemoryFS()

    monkeypatch.setattr(
        common_la,
        "workspace_folder",
        lambda: workspace,
    )

    monkeypatch.setattr(
        common_la,
        "shared_folder",
        lambda: shared,
    )

    monkeypatch.setattr(
        common_la,
        "opendir_location",
        lambda workspace_fs, name: current,
    )

    move_files = MagicMock()

    monkeypatch.setattr(
        common_la.pl,
        "move_files_for_sharing",
        move_files,
    )

    result = move_current_la.execute_in_process()

    assert result.success
    assert shared.exists("current")

    move_files.assert_called_once()

    source, destination = move_files.call_args.args

    assert source is current


def test_concatenate_job(monkeypatch):
    workspace = MemoryFS()
    shared = MemoryFS()

    monkeypatch.setattr(
        common_la,
        "workspace_folder",
        lambda: workspace,
    )

    monkeypatch.setattr(
        common_la,
        "shared_folder",
        lambda: shared,
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
    assert shared.exists("concatenated/ssda903")
