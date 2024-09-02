import pytest
from collections import namedtuple
from dagster import build_op_context

from liiatools_pipeline.sensors.location_schedule import find_previous_matching_run


@pytest.fixture
def mock_run_record():
    la_path = "BAR"
    dataset = "ssda903"
    key_op = "create_session_folder"
    key_folder = "dataset_folder"
    context = build_op_context()

    run = namedtuple("run", ["dagster_run"])
    dagster_run = namedtuple("dagster_run", ["tags", "run_config"])

    return la_path, dataset, key_op, key_folder, context, run, dagster_run


def test_find_previous_matching_run(mock_run_record):
    la_path, dataset, key_op, key_folder, context, run, dagster_run = mock_run_record

    run_1 = run(
        dagster_run(
            {"dagster/run_key": "run_key_1"},
            {"ops": {key_op: {"config": {key_folder: "BAR/ssda903"}}}},
        )
    )

    run_records = [
        run_1,
    ]

    previous_run_id = find_previous_matching_run(
        run_records,
        run_key="run_key_1",
        la_path=la_path,
        dataset=dataset,
        key_op=key_op,
        key_folder=key_folder,
        context=context,
    )

    assert previous_run_id == "run_key_1"


def test_find_previous_matching_run_multiple_runs(mock_run_record):
    la_path, dataset, key_op, key_folder, context, run, dagster_run = mock_run_record

    run_1 = run(
        dagster_run(
            {"dagster/run_key": "run_key_1"},
            {"ops": {key_op: {"config": {key_folder: "BAR/ssda903"}}}},
        )
    )
    run_2 = run(
        dagster_run(
            {"dagster/run_key": "run_key_2"},
            {"ops": {key_op: {"config": {key_folder: "BAR/ssda903"}}}},
        )
    )

    run_records = [
        run_2,
        run_1,
    ]

    previous_run_id = find_previous_matching_run(
        run_records,
        run_key="run_key_2",
        la_path=la_path,
        dataset=dataset,
        key_op=key_op,
        key_folder=key_folder,
        context=context,
    )

    assert previous_run_id == "run_key_2"


def test_find_previous_matching_run_no_records(mock_run_record):
    la_path, dataset, key_op, key_folder, context, run, dagster_run = mock_run_record

    run_records = []

    previous_run_id = find_previous_matching_run(
        run_records,
        run_key="run_key_2",
        la_path=la_path,
        dataset=dataset,
        key_op=key_op,
        key_folder=key_folder,
        context=context,
    )

    assert previous_run_id is None


def test_find_previous_matching_run_missing_run_key(mock_run_record):
    la_path, dataset, key_op, key_folder, context, run, dagster_run = mock_run_record

    run_3 = run(
        dagster_run(
            {"dagster/run_key_missing": "run_key_3"},
            {"ops": {key_op: {"config": {key_folder: "BAR/ssda903"}}}},
        )
    )

    run_records = [run_3]

    previous_run_id = find_previous_matching_run(
        run_records,
        run_key="run_key_3",
        la_path=la_path,
        dataset=dataset,
        key_op=key_op,
        key_folder=key_folder,
        context=context,
    )

    assert previous_run_id is None


def test_find_previous_matching_run_missing_key_op(mock_run_record):
    la_path, dataset, key_op, key_folder, context, run, dagster_run = mock_run_record

    run_4 = run(
        dagster_run(
            {"dagster/run_key": "run_key_4"},
            {"ops": {"key_op_missing": {"config": {key_folder: "BAR/ssda903"}}}},
        )
    )

    run_records = [run_4]

    previous_run_id = find_previous_matching_run(
        run_records,
        run_key="run_key_4",
        la_path=la_path,
        dataset=dataset,
        key_op=key_op,
        key_folder=key_folder,
        context=context,
    )

    assert previous_run_id is None


def test_find_previous_matching_run_missing_key_folder(mock_run_record):
    la_path, dataset, key_op, key_folder, context, run, dagster_run = mock_run_record

    run_5 = run(
        dagster_run(
            {"dagster/run_key": "run_key_4"},
            {"ops": {key_op: {"config": {"key_folder_missing": "BAR/ssda903"}}}},
        )
    )

    run_records = [run_5]

    previous_run_id = find_previous_matching_run(
        run_records,
        run_key="run_key_5",
        la_path=la_path,
        dataset=dataset,
        key_op=key_op,
        key_folder=key_folder,
        context=context,
    )

    assert previous_run_id is None
