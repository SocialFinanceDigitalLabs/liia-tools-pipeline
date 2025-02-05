from liiatools.pnw_pipeline.spec import load_schema


def test_load_schema():
    schema = load_schema()
    assert schema.table["pnw"]["Looked after child?"]
