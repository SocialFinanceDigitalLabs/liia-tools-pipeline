import pandas as pd
import pytest

from liiatools.cans_pipeline.summary_sheet_mapping import add_summary_sheet_columns


def test_add_summary_sheet_columns_mapping():
    df = pd.DataFrame({"field1": ["1", "2", "3"], "field2": ["1", "2", "3"]})
    mapping = {
        "field1": {"1": "Col1", "2": "Col2"},
        "field2": {"1": "Col1", "3": "Col3"},
    }
    column_order = ["Col1", "Col2", "Col3"]

    result = add_summary_sheet_columns(df.copy(), mapping, column_order)

    # check columns added
    for col in column_order:
        assert col in result.columns

    # check mapping
    assert result.at[0, "Col1"] == "field1;field2"  # 1 maps to Col1, 1 maps to Col1
    assert result.at[1, "Col2"] == "field1"  # 2 maps to Col2
    assert result.at[2, "Col3"] == "field2"  # 3 maps to Col3


def test_add_summary_sheet_columns_empty_mapping():
    df = pd.DataFrame({"field1": ["1"]})
    mapping = {}
    column_order = ["Col1"]
    result = add_summary_sheet_columns(df.copy(), mapping, column_order)
    # columns added but empty
    assert result.at[0, "Col1"] == ""
