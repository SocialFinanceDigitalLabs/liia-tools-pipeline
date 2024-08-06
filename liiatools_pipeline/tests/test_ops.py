import pytest
import shutil
from fs import open_fs
from pathlib import Path

import liiatools
from liiatools_pipeline.ops.common_la import (
    create_session_folder,
    open_current,
    process_files,
    move_current_view,
    create_concatenated_view,
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


@pytest.fixture(autouse=True)
def cleanup_files(build_dir):
    yield
    if build_dir.exists():
        shutil.rmtree(build_dir.parents[0])


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


def move_current_view():
    current_folder = workspace_folder() / "current"
    destination_folder = shared_folder() / "current"

    # Ensure the destination folder exists
    destination_folder.mkdir(parents=True, exist_ok=True)

    # pl.move_files_for_sharing(current_folder, destination_folder)

    # Move all files from current_folder to destination_folder
    for file in current_folder.iterdir():
        if file.is_file():
            file.rename(destination_folder / file.name)


@pytest.fixture
def setup_directories():
    # Create the directories for the test
    temp_workspace = workspace_folder()
    temp_shared = shared_folder()

    temp_workspace.mkdir(parents=True, exist_ok=True)
    temp_shared.mkdir(parents=True, exist_ok=True)

    # Create a file in the workspace "current" directory
    workspace_current = temp_workspace / "current"
    workspace_current.mkdir(parents=True, exist_ok=True)

    # Create 'current' directory with test CSV files
    shared_current = temp_shared / "current"
    shared_current.mkdir(parents=True, exist_ok=True)

    csv_file = workspace_current / "822_2022_CIN_dummy.csv"
    with open(csv_file, 'w') as f:
        f.write("dummy data, dummy data line 2")

    yield {
        'workspace_current': workspace_current,
        'shared_current': temp_shared / "current",
        'csv_file': csv_file

    }

    # Cleanup after test
    # shutil.rmtree(temp_workspace, ignore_errors=True)
    # shutil.rmtree(temp_shared, ignore_errors=True)


def test_move_current_view(setup_directories):
    # Move the CSV file using the function under test
    move_current_view()

    # Check that the CSV file has been moved to the shared folder
    shared_current = setup_directories['shared_current']
    csv_file_moved = shared_current / "822_2022_CIN_dummy.csv"

    assert csv_file_moved.exists()
    assert not setup_directories['csv_file'].exists()  # Ensure the original file was removed from the workspace
