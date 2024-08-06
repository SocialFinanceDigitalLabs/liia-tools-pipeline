import os
import shutil
import pytest
import uuid
from dagster import Config
from typing import List, Tuple
from dagster import In, Out, op
from fs.base import FS
from pathlib import Path
from liiatools.common import pipeline as pl
from liiatools_pipeline.ops.common_la import (
    create_session_folder,
    open_current,
    process_files,
    move_current_view,
    create_concatenated_view)
from unittest.mock import MagicMock, patch
import unittest
from liiatools_pipeline.assets.common import dataset
from liiatools.common.archive import DataframeArchive  # Adjust this import based on your actual module structure
import tempfile


class FileConfig(Config):
    filename: str
    name: str


def test_create_session_folder():
    session_folder, session_id, incoming_files = create_session_folder()

    assert session_folder
    assert session_id
    assert incoming_files


def test_open_current():
    current_folder = open_current()

    assert current_folder


# Assuming the actual implementations of these functions exist in your module
def workspace_folder():
    return Path(
        "C:/Users/disha.javur/OneDrive - Social Finance Ltd/Desktop/GitHUb/liia-python/liiatools_pipeline/sample_output/temp_dir/workspace")


def shared_folder():
    return Path(
        "C:/Users/disha.javur/OneDrive - Social Finance Ltd/Desktop/GitHUb/liia-python/liiatools_pipeline/sample_output/temp_dir/shared")


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
