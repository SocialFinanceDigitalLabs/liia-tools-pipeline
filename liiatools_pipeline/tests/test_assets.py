from dagster import asset
import pytest
from fs.base import FS
from dagster import build_op_context
from unittest.mock import patch, MagicMock
from liiatools_pipeline.assets.common import dataset, pipeline_config, process_folder, incoming_folder, workspace_folder, shared_folder, la_code
from fs.memoryfs import MemoryFS


# Mock environment variables for testing
@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("DATASET", "cin")
    monkeypatch.setenv("OUTPUT_LOCATION", "mem://process")
    monkeypatch.setenv("INPUT_LOCATION", "mem://incoming")
    monkeypatch.setenv("WORKSPACE_LOCATION", "mem://workspace")
    monkeypatch.setenv("SHARED_LOCATION", "mem://shared")
    # monkeypatch.setenv("LA_CODE", "valid_code")


def test_dataset(mock_env_vars):
    result = dataset()
    assert result == "cin"


@patch("liiatools_pipeline.assets.common.load_pipeline_config_cin")
def test_pipeline_config(mock_env_vars):
    result = pipeline_config()
    assert result.table_list[0].id == "ad1"
    assert result.table_list[0].columns[0].id == "CHILD"


def test_process_folder(mock_env_vars):
    result = process_folder()
    assert isinstance(result, FS)


def test_incoming_folder(mock_env_vars):
    result = incoming_folder()
    assert isinstance(result, FS)


def test_workspace_folder(mock_env_vars):
    result = workspace_folder()
    assert isinstance(result, FS)


def test_shared_folder(mock_env_vars):
    result = shared_folder()
    assert isinstance(result, FS)


@pytest.fixture
def mock_env_config():
    with patch("liiatools_pipeline.assets.common.env_config") as mock:
        yield mock


@pytest.fixture
def mock_authorities():
    with patch("liiatools_pipeline.assets.common.authorities") as mock:
        yield mock


def test_la_code_valid(mock_env_config, mock_authorities):
    mock_env_config.return_value = "VALID_LA"
    mock_authorities.codes = ["VALID_LA", "ANOTHER_LA"]

    result = la_code()
    assert result == "VALID_LA"


def test_la_code_invalid(mock_env_config, mock_authorities):
    mock_env_config.return_value = "INVALID_LA"
    mock_authorities.codes = ["VALID_LA", "ANOTHER_LA"]

    result = la_code()
    assert result is None


def test_la_code_none(mock_env_config, mock_authorities):
    mock_env_config.return_value = None
    mock_authorities.codes = ["VALID_LA", "ANOTHER_LA"]

    result = la_code()
    assert result is None