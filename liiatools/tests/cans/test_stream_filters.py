import pytest
import pandas as pd
from unittest.mock import MagicMock

from liiatools.cans_pipeline.stream_filters import (
    _align_headers,
    _match_headers,
    transform_input,
    StreamError
)


def test_align_headers_direct_match():
    input_headers = pd.Series(["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)"])
    schema_headers = ["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)"]
    result = _align_headers(input_headers, schema_headers)
    assert result.tolist() == ["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)"]


def test_align_headers_regex_match():
    input_headers = pd.Series(["Assessment Date", "Developmental/Intellectual*", "Cognitive"])
    schema_headers = ["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)"]
    result = _align_headers(input_headers, schema_headers)
    assert result.tolist() == ["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)"]


def test_align_headers_exhausted_schema():
    input_headers = pd.Series(["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)", "Sleep"])
    schema_headers = ["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)"]
    result = _align_headers(input_headers, schema_headers)
    assert result.tolist() == ["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)", "Sleep"]


def test_align_headers_with_nan():
    input_headers = pd.Series(["Assessment Date", None, "Cognitive (Developmental/Intellectual*)"])
    schema_headers = ["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)"]
    result = _align_headers(input_headers, schema_headers)
    assert result.tolist() == ["Assessment Date", None, "Cognitive (Developmental/Intellectual*)"]


def test_match_headers_all_direct_match():
    input_headers = pd.Series(["Assessment Date", "Developmental/Intellectual*"])
    schema_headers = ["Assessment Date", "Developmental/Intellectual*"]
    # Should not raise
    _match_headers(input_headers, schema_headers)

def test_match_headers_regex_match():
    input_headers = pd.Series(["Assessment Date", "Developmental/Intellectual*", "Cognitive", "Sleep"])
    schema_headers = ["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)", "Sleep"]
    # Should not raise
    _match_headers(input_headers, schema_headers)

def test_match_headers_partial_match():
    input_headers = pd.Series(["Assessment Date", "Developmental/Intellectual*", "Some Other Header"])
    schema_headers = ["Assessment Date", "Developmental/Intellectual*", "Cognitive (Developmental/Intellectual*)"]
    with pytest.raises(StreamError, match="Could not find all expected headers"):
        _match_headers(input_headers, schema_headers)

def test_match_headers_missing_header():
    input_headers = pd.Series(["Assessment Date", "Cognitive"])
    schema_headers = ["Assessment Date", "Cognitive (Developmental/Intellectual*)", "Developmental (Developmental/Intellectual*)"]
    with pytest.raises(StreamError, match="Could not find all expected headers"):
        _match_headers(input_headers, schema_headers)

def test_match_headers_duplicate_match():
    input_headers = pd.Series(["Assessment Date", "Cognitive", "Developmental"])
    schema_headers = ["Assessment Date", "Cognitive (Developmental/Intellectual*)", "Developmental (Developmental/Intellectual*)"]
    # Should not raise
    _match_headers(input_headers, schema_headers)


class DummyFileLocator:
    def __init__(self, df):
        self.df = df
        self.name = "dummy.xlsx"
    def open(self, mode):
        # Simulate a file-like object for pd.read_excel
        # We'll patch pd.read_excel to use self.df instead
        return MagicMock()


def test_transform_input_success(monkeypatch):
    # Prepare mock DataFrame as if read from Excel
    df = pd.DataFrame({
        "Unnamed: 1": ["A", "B", "C"],
        "Unnamed: 2": [1, 2, 3],
    })
    table_info = {
        "sheetname": "Sheet1",
        "table_spec": {"A": None, "B": None, "C": None}
    }
    locator = DummyFileLocator(df)
    # Patch pd.read_excel to return our DataFrame
    monkeypatch.setattr(pd, "read_excel", lambda f, sheet_name: df)
    result = transform_input(locator, table_info)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["A", "B", "C"]
    assert result.iloc[0].tolist() == [1, 2, 3]


def test_transform_input_missing_columns(monkeypatch):
    df = pd.DataFrame({
        "Unnamed: 1": ["A", "B", "C"],
        "Other": [1, 2, 3]
    })
    table_info = {
        "sheetname": "Sheet1",
        "table_spec": {"A": None, "B": None, "C": None}
    }
    locator = DummyFileLocator(df)
    monkeypatch.setattr(pd, "read_excel", lambda f, sheet_name: df)
    with pytest.raises(StreamError, match="Could not find Unnamed: 1 and Unnamed: 2 columns"):
        transform_input(locator, table_info)


def test_transform_input_strip_and_dropna(monkeypatch):
    df = pd.DataFrame({
        "Unnamed: 1": ["  A  ", None, "B", "  ", "C"],
        "Unnamed: 2": [1, 2, 3, 4, 5]
    })
    table_info = {
        "sheetname": "Sheet1",
        "table_spec": {"A": None, "B": None, "C": None}
    }
    locator = DummyFileLocator(df)
    monkeypatch.setattr(pd, "read_excel", lambda f, sheet_name: df)
    result = transform_input(locator, table_info)
    assert list(result.columns) == ["A", "B", "C"]
    assert result.iloc[0].tolist() == [1, 3, 5]


def test_transform_input_missing_rows(monkeypatch):
    df = pd.DataFrame({
        "Unnamed: 1": ["A", "B"],
        "Unnamed: 2": [1, 2]
    })
    table_info = {
        "sheetname": "Sheet1",
        "table_spec": {"A": None, "B": None, "C": None}
    }
    locator = DummyFileLocator(df)
    monkeypatch.setattr(pd, "read_excel", lambda f, sheet_name: df)
    with pytest.raises(StreamError, match="Could not find all expected headers"):
        transform_input(locator, table_info)