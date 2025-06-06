from sfdata_stream_parser.events import Cell

from liiatools.common.spec.__data_schema import Column, DataSchema
from liiatools.common.stream_filters import convert_column_header_to_match


def test_convert_column_header_to_match():
    schema = DataSchema(
        column_map={
            "list_1": {
                "Child Unique ID": Column(
                    header_regex=[
                        r"/\bchild\b.*\b(id|identifier)\b|\b(id|identifier)\b.*\bchild\b/i"
                    ]
                ),
                "Gender": Column(),
            }
        }
    )
    stream = [
        Cell(table_name="list_1", header="Child Unique ID"),
        Cell(table_name="list_1", header="Child ID"),
        Cell(table_name="list_1", header="Gender"),
    ]
    stream = list(convert_column_header_to_match(stream, schema=schema))

    assert stream[0].header == "Child Unique ID"
    assert stream[1].header == "Child Unique ID"
    assert stream[2].header == "Gender"
