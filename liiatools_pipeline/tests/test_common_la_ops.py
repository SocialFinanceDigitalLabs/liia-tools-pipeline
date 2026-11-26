from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fs.base import FS
from fs.memoryfs import MemoryFS

from liiatools.common.archive import DataframeArchive
from liiatools.common.data.__file import FileLocator
from liiatools_pipeline.ops import common_la
from liiatools_pipeline.ops.common_config import CleanConfig


def test_create_session_folder(monkeypatch):
    workspace = MagicMock(spec=FS)
    session_folder = MagicMock(spec=FS)

    monkeypatch.setattr(
        common_la,
        "workspace_folder",
        lambda: workspace,
    )

    monkeypatch.setattr(
        common_la.pl,
        "create_session_folder",
        lambda workspace, session_names: (
            session_folder,
            "test-session-id",
        ),
    )

    monkeypatch.setattr(
        common_la,
        "open_fs",
        lambda path: MagicMock(spec=FS),
    )

    monkeypatch.setattr(
        common_la.pl,
        "move_files_for_processing",
        lambda source, destination: [],
    )

    config = CleanConfig(
        dataset_folder="mem://incoming",
    )

    result = common_la.create_session_folder(config)

    assert result == (
        session_folder,
        "test-session-id",
        [],
    )


def test_open_current(monkeypatch):
    workspace = MemoryFS()

    monkeypatch.setattr(
        common_la,
        "workspace_folder",
        lambda: workspace,
    )

    config = CleanConfig(
        dataset="ssda903",
        dataset_folder="mem://incoming",
    )

    result = common_la.open_current(config)

    assert isinstance(result, DataframeArchive)
    assert workspace.exists("current")


def test_move_current_view_la(monkeypatch):
    workspace = MemoryFS()
    current = MemoryFS()
    shared = MemoryFS()

    monkeypatch.setattr(
        common_la,
        "workspace_folder",
        lambda: workspace,
    )

    monkeypatch.setattr(
        common_la,
        "opendir_location",
        lambda workspace_fs, name: current,
    )

    monkeypatch.setattr(
        common_la,
        "shared_folder",
        lambda: shared,
    )

    move_files = MagicMock()

    monkeypatch.setattr(
        common_la.pl,
        "move_files_for_sharing",
        move_files,
    )

    common_la.move_current_view_la()

    move_files.assert_called_once()

    source, destination = move_files.call_args.args

    assert source is current
    assert shared.exists("current")


def test_create_concatenated_view(monkeypatch):
    shared = MemoryFS()
    archive_fs = MemoryFS()

    config = CleanConfig(
        dataset="ssda903",
        dataset_folder="mem://incoming",
    )

    # A real DataframeArchive is required for Dagster type checking.
    current = DataframeArchive(
        archive_fs,
        config,
        "ssda903",
    )

    concat_data = MagicMock()

    monkeypatch.setattr(
        common_la,
        "shared_folder",
        lambda: shared,
    )

    # Pretend there is only one LA.
    authorities = MagicMock()
    authorities.codes = ["TT1"]

    monkeypatch.setattr(
        common_la,
        "authorities",
        authorities,
    )

    remove_files = MagicMock()

    monkeypatch.setattr(
        common_la.pl,
        "remove_files",
        remove_files,
    )

    current_method = MagicMock(
        return_value=concat_data,
    )

    monkeypatch.setattr(
        current,
        "current",
        current_method,
    )

    common_la.create_concatenated_view(
        current=current,
        config=config,
    )

    assert shared.exists("concatenated/ssda903")

    remove_files.assert_called_once()

    remove_args = remove_files.call_args.args

    assert remove_args[0] == "TT1_ssda903_"

    current_method.assert_called_once_with("TT1")

    concat_data.export.assert_called_once()

    export_args = concat_data.export.call_args.args

    assert export_args[1] == "TT1_ssda903_"
    assert export_args[2] == "csv"


@pytest.fixture
def process_files_setup(monkeypatch):
    session = MemoryFS()
    shared = MemoryFS()
    incoming = MemoryFS()
    archive_fs = MemoryFS()

    config = CleanConfig(
        dataset="ssda903",
        dataset_folder="mem://incoming",
        la_folder="mem://incoming",
        input_la_code="TT1",
    )

    # A real DataframeArchive is required for Dagster type checking.
    current = DataframeArchive(
        archive_fs,
        MagicMock(),
        "ssda903",
    )

    current_add = MagicMock()
    current_deduplicate = MagicMock()

    monkeypatch.setattr(
        current,
        "add",
        current_add,
    )

    monkeypatch.setattr(
        current,
        "deduplicate",
        current_deduplicate,
    )

    # Fake LA lookup.
    monkeypatch.setattr(
        common_la.authorities,
        "get_by_code",
        lambda code: "Test One",
    )

    # Default output configuration for a valid setup.
    output_config = MagicMock()

    output_config.la_signed = {
        "Test One": {
            "test": "Yes",
        }
    }

    output_config.input_schema_version = None

    output_config.retention_period = {
        "default": 5,
    }

    # A valid year is discovered by default.
    monkeypatch.setattr(
        common_la.pl,
        "discover_year",
        MagicMock(return_value=2025),
    )

    # The year passes the retention check by default.
    monkeypatch.setattr(
        common_la,
        "check_year_within_range",
        MagicMock(return_value=True),
    )

    monkeypatch.setattr(
        common_la,
        "pipeline_config",
        lambda clean_config: output_config,
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

    hashing_secret = MagicMock()
    hashing_secret.key = "test-secret"

    # A real FileLocator is required for Dagster type checking.
    file_fs = MemoryFS()

    file = FileLocator(
        file_fs,
        "TT1_ssda903_2025.csv",
        metadata={"uuid": "12345"},
    )

    return SimpleNamespace(
        session=session,
        shared=shared,
        incoming=incoming,
        current=current,
        current_add=current_add,
        current_deduplicate=current_deduplicate,
        config=config,
        output_config=output_config,
        hashing_secret=hashing_secret,
        file=file,
    )


def test_process_files_missing_year(
    monkeypatch,
    process_files_setup,
):
    setup = process_files_setup

    # Pretend that a year cannot be found.
    discover_year = MagicMock(
        return_value=None,
    )

    monkeypatch.setattr(
        common_la.pl,
        "discover_year",
        discover_year,
    )

    common_la.process_files(
        session_folder=setup.session,
        incoming_files=[setup.file],
        current=setup.current,
        session_id="test-session",
        config=setup.config,
        hashing_secret=setup.hashing_secret,
    )

    discover_year.assert_called_once_with(setup.file)

    setup.current_add.assert_not_called()

    report_name = "TT1_ssda903_test-session_error_report.csv"

    assert setup.session.exists(report_name)

    with setup.session.open(
        report_name,
        "r",
    ) as report:
        report_text = report.read()

    assert "MissingYear" in report_text


def test_process_files_year_outside_retention(
    monkeypatch,
    process_files_setup,
):
    setup = process_files_setup

    # A year is found.
    monkeypatch.setattr(
        common_la.pl,
        "discover_year",
        MagicMock(return_value=2020),
    )

    # The year fails the retention check.
    monkeypatch.setattr(
        common_la,
        "check_year_within_range",
        MagicMock(return_value=False),
    )

    common_la.process_files(
        session_folder=setup.session,
        incoming_files=[setup.file],
        current=setup.current,
        session_id="test-session",
        config=setup.config,
        hashing_secret=setup.hashing_secret,
    )

    setup.current_add.assert_not_called()

    report_name = "TT1_ssda903_test-session_error_report.csv"

    assert setup.session.exists(report_name)

    with setup.session.open(
        report_name,
        "r",
    ) as report:
        report_text = report.read()

    assert "RetentionPeriod" in report_text


def test_process_files_not_signed(
    process_files_setup,
):
    setup = process_files_setup

    setup.output_config.la_signed = {
        "Test One": {
            "test": "No",
        }
    }

    common_la.process_files(
        session_folder=setup.session,
        incoming_files=[setup.file],
        current=setup.current,
        session_id="test-session",
        config=setup.config,
        hashing_secret=setup.hashing_secret,
    )

    setup.current_add.assert_not_called()

    report_name = "TT1_ssda903_test-session_error_report.csv"

    assert setup.session.exists(report_name)

    with setup.session.open(
        report_name,
        "r",
    ) as report:
        report_text = report.read()

    assert "NotSigned" in report_text


def test_process_files_removes_existing_current_files(
    process_files_setup,
):
    setup = process_files_setup

    setup.current.fs.makedirs(
        "TT1/ssda903",
        recreate=True,
    )

    setup.current.fs.writetext(
        "TT1/ssda903/old_file.csv",
        "old data",
    )

    assert setup.current.fs.exists("TT1/ssda903/old_file.csv")

    common_la.process_files(
        session_folder=setup.session,
        incoming_files=[],
        current=setup.current,
        session_id="test-session",
        config=setup.config,
        hashing_secret=setup.hashing_secret,
    )

    assert not setup.current.fs.exists("TT1/ssda903/old_file.csv")


def test_process_files_degrades_when_enabled(
    monkeypatch,
    process_files_setup,
):
    setup = process_files_setup

    setup.output_config.degrade_at_clean = {
        "default": True,
    }

    schema = MagicMock()

    monkeypatch.setattr(
        common_la,
        "load_schema_ssda903",
        MagicMock(return_value=schema),
    )

    clean_data = MagicMock()

    clean_result = MagicMock()
    clean_result.data = clean_data
    clean_result.errors = []

    monkeypatch.setattr(
        common_la,
        "task_cleanfile_ssda903",
        MagicMock(return_value=clean_result),
    )

    monkeypatch.setattr(
        common_la,
        "prepare_export",
        MagicMock(return_value=clean_data),
    )

    enriched_data = MagicMock()

    enrich_result = MagicMock()
    enrich_result.data = enriched_data
    enrich_result.errors = []

    monkeypatch.setattr(
        common_la,
        "enrich_data",
        MagicMock(return_value=enrich_result),
    )

    degraded_data = MagicMock()

    degraded_result = MagicMock()
    degraded_result.data = degraded_data
    degraded_result.errors = []

    degrade = MagicMock(
        return_value=degraded_result,
    )

    monkeypatch.setattr(
        common_la,
        "degrade_data",
        degrade,
    )

    setup.session.makedirs(
        common_la.SessionNames.CLEANED_FOLDER,
        recreate=True,
    )

    setup.session.makedirs(
        common_la.SessionNames.ENRICHED_FOLDER,
        recreate=True,
    )

    setup.session.makedirs(
        common_la.SessionNames.DEGRADED_FOLDER,
        recreate=True,
    )

    deduplicate_result = MagicMock()
    deduplicate_result.errors = []

    setup.current_deduplicate.return_value = deduplicate_result

    common_la.process_files(
        session_folder=setup.session,
        incoming_files=[setup.file],
        current=setup.current,
        session_id="test-session",
        config=setup.config,
        hashing_secret=setup.hashing_secret,
    )

    degrade.assert_called_once()

    setup.current_add.assert_called_once_with(
        degraded_data,
        "TT1",
        2025,
        None,
        None,
        None,
        None,
    )


def test_process_files_stream_error(
    monkeypatch,
    process_files_setup,
):
    setup = process_files_setup

    monkeypatch.setattr(
        common_la,
        "load_schema_ssda903",
        MagicMock(return_value=MagicMock()),
    )

    clean_file = MagicMock(side_effect=common_la.StreamError("Test stream error"))

    monkeypatch.setattr(
        common_la,
        "task_cleanfile_ssda903",
        clean_file,
    )

    common_la.process_files(
        session_folder=setup.session,
        incoming_files=[setup.file],
        current=setup.current,
        session_id="test-session",
        config=setup.config,
        hashing_secret=setup.hashing_secret,
    )

    clean_file.assert_called_once()

    setup.current_add.assert_not_called()

    report_name = "TT1_ssda903_test-session_error_report.csv"

    assert setup.session.exists(report_name)

    with setup.session.open(
        report_name,
        "r",
    ) as report:
        report_text = report.read()

    assert "StreamError" in report_text
    assert "Test stream error" in report_text


def test_process_files_success(
    monkeypatch,
    process_files_setup,
):
    setup = process_files_setup

    schema = MagicMock()

    load_schema = MagicMock(
        return_value=schema,
    )

    monkeypatch.setattr(
        common_la,
        "load_schema_ssda903",
        load_schema,
    )

    clean_data = MagicMock()

    clean_result = MagicMock()
    clean_result.data = clean_data
    clean_result.errors = []

    clean_file = MagicMock(
        return_value=clean_result,
    )

    monkeypatch.setattr(
        common_la,
        "task_cleanfile_ssda903",
        clean_file,
    )

    prepare_export = MagicMock(
        return_value=clean_data,
    )

    monkeypatch.setattr(
        common_la,
        "prepare_export",
        prepare_export,
    )

    enriched_data = MagicMock()

    enrich_result = MagicMock()
    enrich_result.data = enriched_data
    enrich_result.errors = []

    enrich = MagicMock(
        return_value=enrich_result,
    )

    monkeypatch.setattr(
        common_la,
        "enrich_data",
        enrich,
    )

    setup.output_config.degrade_at_clean = {
        "default": False,
    }

    deduplicate_result = MagicMock()
    deduplicate_result.errors = []

    setup.current_deduplicate.return_value = deduplicate_result

    setup.session.makedirs(
        common_la.SessionNames.CLEANED_FOLDER,
        recreate=True,
    )

    setup.session.makedirs(
        common_la.SessionNames.ENRICHED_FOLDER,
        recreate=True,
    )

    common_la.process_files(
        session_folder=setup.session,
        incoming_files=[setup.file],
        current=setup.current,
        session_id="test-session",
        config=setup.config,
        hashing_secret=setup.hashing_secret,
    )

    load_schema.assert_called_once()
    clean_file.assert_called_once()
    prepare_export.assert_called_once()
    enrich.assert_called_once()

    setup.current_add.assert_called_once_with(
        enriched_data,
        "TT1",
        2025,
        None,
        None,
        None,
        None,
    )

    report_name = "TT1_ssda903_test-session_error_report.csv"

    assert setup.session.exists(report_name)

    with setup.session.open(
        report_name,
        "r",
    ) as report:
        report_text = report.read()

    assert "NoErrors" in report_text
