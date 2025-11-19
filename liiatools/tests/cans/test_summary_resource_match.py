from importlib import resources
from pathlib import Path

import pandas as pd
from ruamel.yaml import YAML

yaml = YAML()


def load_top_level_keys(package: str, *path_parts: str):
    resource = resources.files(package).joinpath(*path_parts)
    with resource.open("r") as f:
        data = yaml.load(f)
    return set(data.keys())


def test_top_level_keys_match():
    keys_a = load_top_level_keys(
        "liiatools", "cans", "spec", "summary_sheet_mapping.yml"
    )
    keys_b = load_top_level_keys(
        "liiatools", "cans", "spec", "summary_column_order.yml"
    )

    assert (
        keys_a == keys_b
    ), f"Table name keys for CANS mapping differ: {keys_a ^ keys_b}"


def test_values_match():
    with resources.files("liiatools").joinpath(
        "cans_pipeline", "spec", "summary_sheet_mapping.yml"
    ).open("r") as f:
        mapping = yaml.load(f)

    with resources.files("liiatools").joinpath(
        "cans_pipeline", "spec", "summary_column_order.yml"
    ).open("r") as f:
        column_order = yaml.load(f)

    for table_name in mapping.keys():
        column_order_list = column_order[table_name]
        summary_columns_a = set()
        for field_map in mapping[table_name].values():
            summary_columns_a.update(field_map.values())
        summary_columns_b = set(column_order_list)
        assert (
            summary_columns_a == summary_columns_b
        ), f"Summary columns for CANS resources {table_name} differ"
