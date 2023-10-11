import sys
import xml.etree.ElementTree as ET
from io import BytesIO
from typing import Iterable

import yaml
from sfdata_stream_parser.events import ParseEvent
from sfdata_stream_parser.parser.xml import parse

from liiatools.cin_census_pipeline import stream_filters as filters
from liiatools.cin_census_pipeline.spec import load_schema
from liiatools.cin_census_pipeline.spec.samples import CIN_2022
from liiatools.cin_census_pipeline.spec.samples import DIR as SAMPLES_DIR
from liiatools.datasets.cin_census.lds_cin_clean import validator
from liiatools.datasets.cin_census.lds_cin_clean.parse import dom_parse


def _xml_to_stream(root) -> Iterable[ParseEvent]:
    schema = load_schema(2022)

    input = BytesIO(ET.tostring(root, encoding="utf-8"))
    stream = dom_parse(input)
    stream = filters.strip_text(stream)
    stream = filters.add_context(stream)
    stream = filters.add_schema(stream, schema=schema)
    # stream = validator.validate_elements(
    #     stream, LAchildID_error=[], field_error=[]
    # )
    stream = filters.validate_elements(stream)

    return list(stream)


def test_validate_all_valid():
    with CIN_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    stream = _xml_to_stream(root)

    # Count nodes in stream that are not valid
    invalid_nodes = [e for e in stream if not getattr(e, "valid", True)]
    assert len(invalid_nodes) == 0


def test_validate_missing_child_id():
    with CIN_2022.open("rb") as f:
        root = ET.parse(f).getroot()

    parent = root.find(".//ChildIdentifiers")
    el = parent.find("LAchildID")
    parent.remove(el)

    stream = _xml_to_stream(root)

    # Count nodes in stream that are not valid
    invalid_nodes = [e for e in stream if not getattr(e, "valid", True)]
    assert len(invalid_nodes) > 0

    assert invalid_nodes[0].tag == "LAchildID"