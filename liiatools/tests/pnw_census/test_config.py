from liiatools.pnw_census_pipeline.spec import load_schema


def test_load_schema():
    schema = load_schema()
    assert schema.table["pnw_census"]["Looked after child?"]
