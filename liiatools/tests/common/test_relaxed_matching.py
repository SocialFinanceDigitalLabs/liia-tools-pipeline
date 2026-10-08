import pytest
from sfdata_stream_parser.events import Cell

from liiatools.common.converters import to_category
from liiatools.common.spec.__data_schema import Column, DataSchema
from liiatools.common.matching import normalise_text
from liiatools.common.stream_filters import convert_column_header_to_match


@pytest.mark.parametrize(
    "value, expected",
    [
        ("  Date   of  Birth ", "date of birth"),
        ("Date_of-Birth", "date of birth"),
        ("Date\u00a0of\u2013Birth*", "date of birth"),
        ("ÄBC", "äbc"),
        ("--", ""),
    ],
)
def test_normalise_text(value, expected):
    assert normalise_text(value) == expected


def test_to_category_relaxed():
    column = Column(
        category=[
            {"code": "a) Male", "name": "Male"},
            {"code": "c) Not stated/recorded", "name": "Not stated/recorded"},
        ]
    )
    assert to_category("MALE", column) == "a) Male"
    assert to_category("a)  male", column) == "a) Male"
    assert to_category("Not stated recorded", column) == "c) Not stated/recorded"
    assert to_category("Not-stated / recorded", column) == "c) Not stated/recorded"

    with pytest.raises(ValueError):
        to_category("Mael", column)


def test_to_category_relaxed_ambiguous():
    column = Column(
        category=[{"code": "A", "name": "x-y"}, {"code": "B", "name": "x y"}]
    )
    # Exact match still wins; an ambiguous relaxed match is rejected
    assert to_category("x-y", column) == "A"
    with pytest.raises(ValueError):
        to_category("x_y", column)


def test_schema_whitespace_is_ignored():
    column = Column(
        category=[
            {"code": "M ", "name": " Male "},
            {"code": "1 ", "name": ["One "]},
        ]
    )
    assert to_category("M", column) == "M "
    assert to_category("male", column) == "M "
    assert to_category(1.0, column) == "1 "
    assert to_category("one", column) == "1 "

    schema = DataSchema(column_map={"list_1": {"Date of Birth ": Column()}})
    assert schema.get_table_from_headers(["Date of Birth"]) == "list_1"
    stream = [Cell(table_name="list_1", header="Date of Birth")]
    stream = list(convert_column_header_to_match(stream, schema=schema))
    assert stream[0].header == "Date of Birth "


def test_get_table_from_headers_relaxed():
    schema = DataSchema(
        column_map={"list_1": {"Date of Birth": Column(), "Child ID": Column()}}
    )
    assert schema.get_table_from_headers(["date  of birth", "child_id"]) == "list_1"
    assert schema.get_table_from_headers(["Date-of-Birth", "Child ID*"]) == "list_1"
    assert schema.get_table_from_headers(["Date of Birth", "Chiild ID"]) is None


def test_convert_column_header_relaxed():
    schema = DataSchema(column_map={"list_1": {"Date of Birth": Column()}})
    stream = [Cell(table_name="list_1", header="date_of_birth")]
    stream = list(convert_column_header_to_match(stream, schema=schema))
    assert stream[0].header == "Date of Birth"
