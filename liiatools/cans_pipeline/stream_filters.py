import re
from typing import List
import pandas as pd

from liiatools.common.data import FileLocator
from liiatools.common.stream_errors import StreamError


def _match_headers(input_headers: pd.Series, schema_headers: List[str]):
    """
    Given a series of input headers and a list of schema headers, checks that all schema headers are present in the input headers.
    If a match is found, the header is removed from the input headers to prevent duplicate matches.

    :param input_headers: Series of input headers
    :param schema_headers: List of schema headers
    :return: None
    """
    regex = r"([a-zA-Z\s\-\–\*\/\(\)]*)\s\([a-zA-Z\s\-\*\/]*\*?"
    matched_headers = []

    for header in schema_headers:
        # Direct match
        mask = input_headers.str.fullmatch(re.escape(header))
        if mask.any():
            first_true_idx = mask[mask].index[0]
            input_headers = input_headers.drop(first_true_idx)
            matched_headers.append(header)
            continue

        # Regex match
        m = re.match(regex, header)
        if m:
            mask = input_headers.str.fullmatch(re.escape(m.group(1)))
            if mask.any():
                first_true_idx = mask[mask].index[0]
                input_headers = input_headers.drop(first_true_idx)
                matched_headers.append(header)
    
    if matched_headers != schema_headers:
        raise StreamError("Could not find all expected headers")


def _align_headers(input_headers: pd.Series, schema_headers: List[str]) -> pd.Series:
    """
    Replace matching input_headers with schema_headers in order.
    Input headers are expected to be checked with _match_headers first.
    If no match or schema_headers are exhausted, leave as is.
    """
    aligned_headers = input_headers.copy()
    regex = r"([a-zA-Z\s\-\–\*\/\(\)]*)\s\([a-zA-Z\s\-\*\/]*\*?"
    schema_iter = iter(schema_headers)

    for idx, val in aligned_headers.items():
        try:
            schema_header = next(schema_iter)
        except StopIteration:
            # No more schema headers, leave the rest as is
            break

        # Direct match
        if pd.isna(val):
            continue
        if isinstance(val, str) and re.fullmatch(re.escape(schema_header), val):
            aligned_headers.at[idx] = schema_header
            continue

        # Regex match
        m = re.match(regex, schema_header)
        if m and isinstance(val, str) and re.fullmatch(re.escape(m.group(1)), val):
            aligned_headers.at[idx] = schema_header
            continue

        # If no match, leave as is and do not advance schema_header
        schema_iter = iter([schema_header] + list(schema_iter))

    return aligned_headers


def transform_input(source: FileLocator, table_info: dict) -> pd.DataFrame:
    with source.open("rb") as f:
        try:
            data = pd.read_excel(f, sheet_name=table_info["sheetname"])
        except ValueError:
            raise StreamError(f"Sheet {table_info['sheetname']} not found")

    if not {"Unnamed: 1", "Unnamed: 2"}.issubset(data.columns):
        raise StreamError("Could not find Unnamed: 1 and Unnamed: 2 columns")
    
    data["Unnamed: 1"] = data["Unnamed: 1"].str.strip()
    data = data.dropna(subset=["Unnamed: 1"])
    

    schema_headers = list(table_info["table_spec"].keys())
    _match_headers(data["Unnamed: 1"], schema_headers)
    data["Unnamed: 1"] = _align_headers(data["Unnamed: 1"], schema_headers)
    data = data[data["Unnamed: 1"].isin(schema_headers)]

    transposed_data = data[["Unnamed: 1", "Unnamed: 2"]].set_index("Unnamed: 1").T
    return transposed_data