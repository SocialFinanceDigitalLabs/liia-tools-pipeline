from liiatools.annex_a_pipeline.spec import load_schema


def test_load_schema():
    schema = load_schema()
    assert schema.table["list_1"]["Child Unique ID"]
