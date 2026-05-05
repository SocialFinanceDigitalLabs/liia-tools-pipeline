from collections import namedtuple
from unittest.mock import MagicMock

import pytest
from dagster import build_op_context

from liiatools_pipeline.sensors.location_schedule import find_previous_matching_run_key


@pytest.fixture
def mock_run_record():
    la = "BAR"
    dataset = "ssda903"
    key_op = "create_session_folder"
    context = build_op_context()

    run = namedtuple("run", ["dagster_run"])
    dagster_run = namedtuple("dagster_run", ["tags", "run_config"])

    return la, dataset, key_op, context, run, dagster_run


def test_find_previous_matching_run(mock_run_record):
    la, dataset, key_op, context, run, dagster_run = mock_run_record

    run_1 = run(
        dagster_run(
            {"dagster/run_key": "BAR_ssda903:hash123"},
            {"ops": {key_op: {"config": {"input_la_code": la, "dataset": dataset}}}},
        )
    )

    run_records = [
        run_1,
    ]

    previous_run_id = find_previous_matching_run_key(
        run_records,
        lambda run: (
            run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["input_la_code"] == la
            and run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["dataset"] == dataset
        ),
        context=context,
    )

    assert previous_run_id == "BAR_ssda903:hash123"


def test_find_previous_matching_run_multiple_runs(mock_run_record):
    la, dataset, key_op, context, run, dagster_run = mock_run_record

    run_1 = run(
        dagster_run(
            {"dagster/run_key": "BAR_ssda903:hash123"},
            {"ops": {key_op: {"config": {"input_la_code": la, "dataset": dataset}}}},
        )
    )
    run_2 = run(
        dagster_run(
            {"dagster/run_key": "BAR_ssda903:hash234"},
            {"ops": {key_op: {"config": {"input_la_code": la, "dataset": dataset}}}},
        )
    )

    run_records = [
        run_2,
        run_1,
    ]

    previous_run_id = find_previous_matching_run_key(
        run_records,
        lambda run: (
            run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["input_la_code"] == la
            and run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["dataset"] == dataset
        ),
        context=context,
    )

    assert previous_run_id == "BAR_ssda903:hash234"


def test_find_previous_matching_run_no_records(mock_run_record):
    la, dataset, key_op, context, run, dagster_run = mock_run_record

    run_records = []

    previous_run_id = find_previous_matching_run_key(
        run_records,
        lambda run: (
            run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["input_la_code"] == la
            and run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["dataset"] == dataset
        ),
        context=context,
    )

    assert previous_run_id is None


def test_find_previous_matching_run_missing_run_key(mock_run_record):
    la, dataset, key_op, context, run, dagster_run = mock_run_record

    context.log.error = MagicMock()

    run_3 = run(
        dagster_run(
            {"dagster/run_key_missing": "BAR_ssda903:hash345"},
            {"ops": {key_op: {"config": {"input_la_code": la, "dataset": dataset}}}},
        )
    )

    run_records = [run_3]

    previous_run_id = find_previous_matching_run_key(
        run_records,
        lambda run: (
            run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["input_la_code"] == la
            and run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["dataset"] == dataset
        ),
        context=context,
    )

    assert previous_run_id is None


def test_find_previous_matching_run_missing_key_op(mock_run_record):
    la, dataset, key_op, context, run, dagster_run = mock_run_record

    context.log.error = MagicMock()

    run_4 = run(
        dagster_run(
            {"dagster/run_key": "BAR_ssda903:hash456"},
            {"ops": {"key_op_missing": {"config": {"input_la_code": la, "dataset": dataset}}}},
        )
    )

    run_records = [run_4]

    previous_run_id = find_previous_matching_run_key(
        run_records,
        lambda run: (
            run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["input_la_code"] == la
            and run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["dataset"] == dataset
        ),
        context=context,
    )

    assert previous_run_id is None


def test_find_previous_matching_run_missing_key_folder(mock_run_record):
    la, dataset, key_op, context, run, dagster_run = mock_run_record

    context.log.error = MagicMock()

    run_5 = run(
        dagster_run(
            {"dagster/run_key": "BAR_ssda903:hash567"},
            {"ops": {key_op: {"config": {"input_la_code_missing": la, "dataset": dataset}}}},
        )
    )

    run_records = [run_5]

    previous_run_id = find_previous_matching_run_key(
        run_records,
        lambda run: (
            run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["input_la_code"] == la
            and run.dagster_run.run_config["ops"]["create_session_folder"]["config"]["dataset"] == dataset
        ),
        context=context,
    )

    assert previous_run_id is None
