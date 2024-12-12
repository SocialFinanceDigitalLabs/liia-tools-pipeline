from liiatools.annex_a_pipeline.build_schema import build_schema
import os
import pytest
import tempfile
import pandas as pd
from ruamel.yaml import YAML

yaml = YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=4, sequence=6, offset=2)


@pytest.fixture
def setup_files():
    schema_content = """
    column_map:
        annex_a_list:
            column_name: &tab-name
                category: 
                    -   code: test
                        name: test
    """
    excel_content = {
        'tab name': pd.DataFrame({
            'group': ['A', 'B', 'A'],
            'code': [1, 2, 3]
        })
    }

    # Create a temporary YAML schema file
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.yml') as schema_file:
        schema = yaml.load(schema_content)
        print(schema['column_map']['annex_a_list']['column_name'])
        for annex_a_list, columns in schema.get("column_map", {}).items():
            for column_name, column_coding in columns.items():
                anchor = column_coding.anchor.value
                print(f"Anchor: {anchor}") # this correctly prints the anchor value
        yaml.dump(schema, schema_file)
        schema_path = schema_file.name
        print(f"Schema file created at: {schema_path}")

    # Create a temporary Excel file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as excel_file:
        with pd.ExcelWriter(excel_file.name) as writer:
            for sheet_name, df in excel_content.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        excel_path = excel_file.name
        print(f"Excel file created at: {excel_path}")

    # Create a temporary output schema file
    with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.yml') as output_file:
        output_schema_path = output_file.name
        print(f"Output schema file created at: {output_schema_path}")

    # Provide paths to the test
    yield schema_path, excel_path, output_schema_path

    # Clean up temporary files
    os.remove(schema_path)
    os.remove(excel_path)
    os.remove(output_schema_path)

def test_build_schema(setup_files):
    schema_path, excel_path, output_schema_path = setup_files

    with open(schema_path, 'r') as f:
        schema = yaml.load(f)
        print(schema['column_map']['annex_a_list']['column_name'])
        for annex_a_list, columns in schema.get("column_map", {}).items():
            for column_name, column_coding in columns.items():
                anchor = column_coding.anchor.value
                print(f"Anchor: {anchor}")

    build_schema(schema_path, excel_path, output_schema_path)

    with open(output_schema_path, 'r') as file:
        updated_schema = yaml.load(file)
        print(updated_schema)

    assert 'annex_a_list' in updated_schema['column_map']
    assert 'column_name' in updated_schema['column_map']['annex_a_list']
    assert 'category' in updated_schema['column_map']['annex_a_list']['column_name']
    assert len(updated_schema['column_map']['annex_a_list']['column_name']['category']) == 1
    assert updated_schema['column_map']['annex_a_list']['column_name']['category'][0] == 'A'
    assert updated_schema['column_map']['annex_a_list']['column_name']['category']['name'] == ['1', '3']
    assert updated_schema['column_map']['annex_a_list']['column_name']['category']['code'] == 'B'
    assert updated_schema['column_map']['annex_a_list']['column_name']['category']['name'] == ['2']

def test_build_schema_no_matching_tab(setup_files):
    schema_path, excel_path, output_schema_path = setup_files

    # Modify the schema to have a different tab name
    with open(schema_path, 'r') as file:
        schema = yaml.load(file)
    schema['column_map']['annex_a_list']['column_name']['anchor'] = 'non_matching_tab'
    with open(schema_path, 'w') as file:
        yaml.dump(schema, file)

    build_schema(schema_path, excel_path, output_schema_path)

    with open(output_schema_path, 'r') as file:
        updated_schema = yaml.load(file)

    assert 'annex_a_list' in updated_schema['column_map']
    assert 'column_name' in updated_schema['column_map']['annex_a_list']
    assert 'category' in updated_schema['column_map']['annex_a_list']['column_name']
    assert len(updated_schema['column_map']['annex_a_list']['column_name']['category']) == 0

def test_build_schema_missing_columns(setup_files):
    schema_path, excel_path, output_schema_path = setup_files

    # Modify the Excel content to remove required columns
    with pd.ExcelWriter(excel_path) as writer:
        pd.DataFrame({'group': ['A', 'B', 'A']}).to_excel(writer, sheet_name='tab_name', index=False)

    build_schema(schema_path, excel_path, output_schema_path)

    with open(output_schema_path, 'r') as file:
        updated_schema = yaml.load(file)

    assert 'annex_a_list' in updated_schema['column_map']
    assert 'column_name' in updated_schema['column_map']['annex_a_list']
    assert 'category' in updated_schema['column_map']['annex_a_list']['column_name']
    assert len(updated_schema['column_map']['annex_a_list']['column_name']['category']) == 0