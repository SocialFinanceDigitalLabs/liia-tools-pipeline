from importlib import resources

from ruamel.yaml import YAML

from liiatools.cans_pipeline.spec import (
    load_summary_sheet_column_order,
    load_summary_sheet_mapping,
)

yaml = YAML()


def load_top_level_keys(data):
    return set(data.keys())


def test_top_level_keys_match():
    keys_a = load_top_level_keys(load_summary_sheet_mapping())
    keys_b = load_top_level_keys(load_summary_sheet_column_order())

    assert (
        keys_a == keys_b
    ), f"Table name keys for CANS mapping differ: {keys_a ^ keys_b}"


def test_values_match():
    mapping = load_summary_sheet_mapping()
    column_order = load_summary_sheet_column_order()

    for table_name in mapping.keys():
        column_order_list = column_order[table_name]
        summary_columns_a = set()
        for field_map in mapping[table_name].values():
            summary_columns_a.update(field_map.values())
        summary_columns_b = set(column_order_list)
        assert (
            summary_columns_a == summary_columns_b
        ), f"Summary columns for CANS resources {table_name} differ"
