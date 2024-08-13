import pytest
import shutil
from fs import open_fs
from pathlib import Path
import os
from liiatools.common.archive import DataframeArchive
import unittest
from liiatools_pipeline.assets.common import pipeline_config
import liiatools
from liiatools.common.data import PipelineConfig
from liiatools_pipeline.ops.common_la import (
    create_session_folder,
    open_current,
    process_files,
    move_current_view,
    create_concatenated_view,
    dataset,
)


@pytest.fixture(scope="session")
def liiatools_dir():
    return Path(liiatools.__file__).parent


@pytest.fixture(scope="session")
def build_dir(liiatools_dir):
    build_dir = liiatools_dir / "../build/tests/"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)

    for directory in ["process", "incoming", "workspace", "shared"]:
        (build_dir / directory).mkdir(parents=True, exist_ok=True)

    return build_dir


class MockData:
    def export(self, folder, filename, format):
        # Mock export function for testing
        with open(folder / f"{filename}.{format}", "w") as f:
            f.write(f"Mock data for {filename}")


# @pytest.fixture(autouse=True)
# def cleanup_files(build_dir):
#     yield
#     if build_dir.exists():
#         shutil.rmtree(build_dir.parents[0])


@pytest.fixture
def mock_env_vars(monkeypatch, build_dir):
    monkeypatch.setenv("DATASET", "cin")
    monkeypatch.setenv("OUTPUT_LOCATION", f"{str(build_dir)}/process")
    monkeypatch.setenv("INPUT_LOCATION", f"{str(build_dir)}/incoming")
    monkeypatch.setenv("WORKSPACE_LOCATION", f"{str(build_dir)}/workspace")
    monkeypatch.setenv("SHARED_LOCATION", f"{str(build_dir)}/shared")


def test_create_session_folder(mock_env_vars, build_dir):
    source_fs = open_fs(f"{str(build_dir)}/incoming")
    source_fs.writetext("file1.txt", "foo")
    source_fs.writetext("file2.txt", "bar")

    session_folder, session_id, incoming_files = create_session_folder()

    assert session_folder
    assert session_id
    assert len(incoming_files) == 2


def test_open_current(mock_env_vars, build_dir):
    current = open_current()

    assert current.config.table_list[0].columns[0].id == "LAchildID"
    assert current.config.table_list[0].id == "cin"
    assert current.dataset == "cin"


@pytest.fixture
def setup_directories(mock_env_vars, build_dir):
    # Ensure the 'workspace/current' directory is created
    os.makedirs(f"{str(build_dir)}/workspace/current", exist_ok=True)
    os.makedirs(f"{str(build_dir)}/shared/current", exist_ok=True)
    os.makedirs(f"{str(build_dir)}/shared/concatenated", exist_ok=True)

    # Create a file in the workspace "current" directory
    # workspace_current = Path(f"{str(build_dir)}/workspace/current")
    csv_file_1 = Path(f"{str(build_dir)}/workspace/current/822_cin_test_1.csv")
    with open(csv_file_1, "w") as f:
        f.write("dummy data 1, dummy data line 2")
    csv_file_2 = Path(f"{str(build_dir)}/workspace/current/822_cin_test_2.csv")
    with open(csv_file_2, "w") as f:
        f.write("dummy data 2, dummy data line 3")

    yield {
        "workspace_current": Path(f"{str(build_dir)}/workspace/current"),
        "shared_current": Path(f"{str(build_dir)}/shared/current"),
        "csv_file_1": csv_file_1,
        "concat_folder": Path(f"{str(build_dir)}/shared/concatenated"),
        "csv_file_2": csv_file_2,
    }

    # Cleanup after test
    # shutil.rmtree(temp_workspace, ignore_errors=True)
    # shutil.rmtree(temp_shared, ignore_errors=True)


def test_move_current_view(setup_directories, mock_env_vars, build_dir):
    # Move the CSV file using the function under test
    move_current_view()

    # Check that the CSV file has been moved to the shared folder
    shared_current = setup_directories["shared_current"]
    csv_file_moved_1 = shared_current / "822_cin_test_1.csv"
    csv_file_moved_2 = shared_current / "822_cin_test_2.csv"

    assert csv_file_moved_1.exists()
    assert csv_file_moved_2.exists()
    assert setup_directories[
        "csv_file_1"
    ].exists()  # Ensure the original file is also present in workspace
    assert setup_directories[
        "csv_file_2"
    ].exists()  # Ensure the original file is also present in workspace


# def create_concatenated_view(setup_directories, current: DataframeArchive):
#     concat_folder = setup_directories[f"concat_folder/{dataset()}"]
#     concat_folder.mkdir(parents=True, exist_ok=True)
#
#     authorities = ["822", "823"]  # Mocked list of authorities for this example
#
#     for la_code in authorities:
#         concat_data = current.current(la_code)
#         if concat_data:
#             concat_data.export(concat_folder, f"{la_code}_{dataset}_", "csv")


def test_create_concatenated_view(mock_env_vars, build_dir):
    # Create mock data and DataframeArchive
    source_fs = open_fs(f"{str(build_dir)}/workspace")
    source_fs.makedirs("current/822/cin")
    source_fs.writetext("current/822/cin/822_2022_cin.csv", "LAchildID,Date\n249901_822, 1970-10-06\n249901_822, 1971-02-27")
    source_fs.writetext("current/822/cin/822_2023_cin.csv", "LAchildID,Date\n123456_822, 1970-10-06\n249901_822, 1971-02-27")

    # mock_data = {"822": MockData(), "823": MockData()}

    mock_pipeline_config = PipelineConfig(
        table_list=
        [
            {
                "id": "cin",
                "retain": [
                    "PAN"
                ],
                "columns": [
                    {
                        "id": "LAchildID",
                        "type": "string",
                        "unique_key": True,
                        "enrich":
                            [
                                "integer",
                                "add_la_suffix"
                            ]
                    },
                    {
                        "id": "Date",
                        "type": "date",
                        "unique_key": True
                    }
                ]
            }
        ]
    )

    current = DataframeArchive(source_fs.opendir("current"), mock_pipeline_config, dataset())

    # Run the function
    create_concatenated_view(current)

    # Check that the concatenated file is created in the shared folder
    concat_folder = setup_directories[f"concat_folder/{dataset()}"]
    assert concat_folder.exists()