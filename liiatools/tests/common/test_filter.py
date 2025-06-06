from datetime import datetime

import pytest
from sfdata_stream_parser import events
from sfdata_stream_parser.filters import generic

from liiatools.common import stream_filters
from liiatools.common.spec.__data_schema import Column
from liiatools.ssda903_pipeline.spec import load_schema as s903_schema


def test_collect_row():
    spec_int = Column(numeric={"type": "integer"})
    spec_str = Column(string="alphanumeric")
    stream = [
        events.StartContainer(),
        events.StartTable(),
        events.StartRow(),
        events.Cell(header="CHILD", cell=123, column_spec=spec_int),
        events.Cell(header="DOB", cell="01/01/2019", column_spec=spec_str),
        events.EndRow(),
        events.StartRow(),
        events.Cell(header="CHILD", cell=123, column_spec=spec_int),
        events.Cell(header="DOB", cell="01/01/2019", column_spec=spec_str),
        events.Cell(header="OTHER", cell="other"),
        events.EndRow(),
        events.EndTable(),
        events.EndContainer(),
    ]
    stream = stream_filters.collect_cell_values_for_row(stream)
    rows = [event for event in stream if isinstance(event, events.StartRow)]

    assert rows[0].row_values == {"CHILD": 123, "DOB": "01/01/2019"}
    assert rows[1].row_values == {"CHILD": 123, "DOB": "01/01/2019"}


def test_collect_tables():
    stream = [
        events.StartContainer(),
        events.StartTable(),
        events.StartRow(
            table_name="Episodes", row_values={"CHILD": 123, "DOB": "01/01/2019"}
        ),
        events.Cell(),
        events.Cell(),
        events.EndRow(),
        events.StartRow(
            table_name="Episodes", row_values={"CHILD": 123, "DOB": "01/01/2019"}
        ),
        events.Cell(),
        events.Cell(),
        events.EndRow(),
        events.EndTable(),
        events.EndContainer(),
    ]

    dataset, stream = stream_filters.collect_tables(stream)
    generic.consume(stream)

    dataset = dataset.value

    assert dataset["Episodes"] == [
        {"CHILD": 123, "DOB": "01/01/2019"},
        {"CHILD": 123, "DOB": "01/01/2019"},
    ]


def test_add_table_name():
    schema = s903_schema(2040)

    def get_table_name(headers):
        stream = [events.StartTable(headers=headers)]
        stream = stream_filters.add_table_name(stream, schema=schema)
        event = list(stream)[0]
        table_name = getattr(event, "table_name", None)
        errors = getattr(event, "errors", None)
        return {"table_name": table_name, "errors": errors}

    assert (
        get_table_name(["CHILD", "SEX", "DOB", "ETHNIC", "UPN", "MOTHER", "MC_DOB"])[
            "table_name"
        ]
        == "header"
    )

    for table_name, table_data in schema.table.items():
        headers = list(table_data.keys())
        assert get_table_name(headers)["table_name"] == table_name

    assert get_table_name(["incorrect", "header", "values"])["table_name"] is None
    assert list(get_table_name(["incorrect", "header", "values"])["errors"]) == [
        {
            "message": "Failed to identify table based on headers",
            "type": "UnidentifiedTable",
        }
    ]

    assert get_table_name([""])["table_name"] is None
    assert list(get_table_name([""])["errors"]) == [
        {
            "message": "Could not identify headers as first row is blank",
            "type": "BlankHeaders",
        }
    ]

    assert get_table_name([])["table_name"] is None
    assert list(get_table_name([])["errors"]) == [
        {
            "message": "Failed to identify table based on headers",
            "type": "UnidentifiedTable",
        }
    ]

    assert get_table_name(None)["table_name"] is None
    assert list(get_table_name(None)["errors"]) == [
        {
            "message": "Failed to identify table based on headers",
            "type": "UnidentifiedTable",
        }
    ]


def test_match_config_to_cell():
    schema = s903_schema(2040)

    def match_cell(**cell_properties):
        stream = [events.Cell(**cell_properties)]
        stream = stream_filters.match_config_to_cell(stream, schema=schema)
        event = list(stream)[0]
        return getattr(event, "column_spec", None)

    assert match_cell(table_name="header", header="CHILD").string == "alphanumeric"

    assert match_cell(header="CHILD") is None

    assert match_cell(table_name="header") is None

    assert match_cell() is None

    assert match_cell(table_name="UNKNOWN", header="CHILD") is None

    assert match_cell(table_name="header", header="UNKNOWN") is None

    assert match_cell(table_name="header", header=None) is None

    assert match_cell(table_name=None, header="CHILD") is None


def assert_errors(event, *types):
    errors = getattr(event, "errors", [])
    error_types = [error["type"] for error in errors]
    if types:
        assert error_types == list(types)
    else:
        assert not errors


def test_clean_dates():
    date_spec = Column(date="%d/%m/%Y")

    event = events.Cell(cell=datetime(2019, 1, 15), column_spec=date_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == datetime(2019, 1, 15).date()
    assert_errors(cleaned_event)

    event = events.Cell(cell="15/1/2019", column_spec=date_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == datetime(2019, 1, 15).date()
    assert_errors(cleaned_event)

    event = events.Cell(cell="2019/1/15", column_spec=date_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    event = events.Cell(
        cell=datetime(2019, 1, 15), config_dict={"not_date": "%d/%m/%Y"}
    )
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == datetime(2019, 1, 15)
    assert_errors(cleaned_event)

    event = events.Cell(cell="string", config_dict={"not_date": "%d/%m/%Y"})
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "string"
    assert_errors(cleaned_event)

    event = events.Cell(cell=None, column_spec=date_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)

    event = events.Cell(cell="", column_spec=date_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)


def test_clean_categories():
    category_spec = Column(
        category=[{"code": "0", "name": "False"}, {"code": "1", "name": "True"}]
    )

    event = events.Cell(cell="0", column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "0"
    assert_errors(cleaned_event)

    event = events.Cell(cell="0.0", column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "0"
    assert_errors(cleaned_event)

    event = events.Cell(cell=0, column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "0"
    assert_errors(cleaned_event)

    event = events.Cell(cell="true", column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "1"
    assert_errors(cleaned_event)

    event = events.Cell(cell=123, column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    event = events.Cell(cell="string", column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    event = events.Cell(cell=None, column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)

    event = events.Cell(cell="", column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)

    category_spec = Column(
        category=[
            {
                "code": "b) Female",
                "name": "F",
                "cell_regex": ["/.*fem.*/i", "/b\).*/i"],
            },
            {"code": "a) Male", "name": "M", "cell_regex": ["/^mal.*/i", "/a\).*/i"]},
        ],
    )

    event = events.Cell(cell="F", column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "b) Female"
    assert_errors(cleaned_event)

    event = events.Cell(cell="a)", column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "a) Male"
    assert_errors(cleaned_event)

    event = events.Cell(cell="string", column_spec=category_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")


def test_clean_numeric():
    integer_spec = Column(numeric={"type": "integer"})
    event = events.Cell(cell=123, column_spec=integer_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == 123
    assert_errors(cleaned_event)

    event = events.Cell(cell="123", column_spec=integer_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == 123
    assert_errors(cleaned_event)

    event = events.Cell(cell="string", column_spec=integer_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    event = events.Cell(cell="", column_spec=integer_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)

    event = events.Cell(cell=None, column_spec=integer_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)

    event = events.Cell(cell=datetime(2017, 3, 17), column_spec=integer_spec)
    cleaned_event = list(stream_filters.conform_cell_types([event]))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    float_spec = Column(
        numeric={"type": "float", "min_value": 0, "max_value": 1, "decimal_places": 2}
    )
    event = events.Cell(cell=0.123, column_spec=float_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == 0.12
    assert_errors(cleaned_event)

    event = events.Cell(cell="0.2", column_spec=float_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == 0.2
    assert_errors(cleaned_event)

    event = events.Cell(cell="string", column_spec=float_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    event = events.Cell(cell=-1, column_spec=float_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")


def test_clean_postcodes():
    pc_spec = Column(string="postcode")
    event = events.Cell(cell="G62 7PS", column_spec=pc_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "G62 7PS"
    assert_errors(cleaned_event)

    event = events.Cell(cell="CW3 9PU", column_spec=pc_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "CW3 9PU"
    assert_errors(cleaned_event)

    event = events.Cell(cell="string", column_spec=pc_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    event = events.Cell(cell=123, column_spec=pc_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    event = events.Cell(cell="", column_spec=pc_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)

    event = events.Cell(cell=None, column_spec=pc_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)


def test_clean_regex():
    regex_spec = Column(string="regex", cell_regex=r"[A-Za-z]{2}\d{10}")
    event = events.Cell(cell="AB1234567890", column_spec=regex_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "AB1234567890"
    assert_errors(cleaned_event)

    event = events.Cell(cell="  AB1234567890  ", column_spec=regex_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "AB1234567890"
    assert_errors(cleaned_event)

    event = events.Cell(cell="AB1234567890abcd", column_spec=regex_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    event = events.Cell(cell="AB123", column_spec=regex_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event, "ConversionError")

    event = events.Cell(cell="", column_spec=regex_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)

    event = events.Cell(cell=None, column_spec=regex_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == ""
    assert_errors(cleaned_event)

    regex_spec = Column(string="regex", cell_regex=r"[A-Za-z]\d{11}(\d|[A-Za-z])")
    event = events.Cell(cell="A123456789012", column_spec=regex_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "A123456789012"
    assert_errors(cleaned_event)

    event = events.Cell(cell="A12345678901B", column_spec=regex_spec)
    cleaned_event = list(stream_filters.conform_cell_types(event))[0]
    assert cleaned_event.cell == "A12345678901B"
    assert_errors(cleaned_event)
