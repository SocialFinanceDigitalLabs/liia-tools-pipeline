import pandas as pd
from ruamel.yaml import YAML

yaml = YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=4, sequence=6, offset=2)


def build_schema(schema_path, excel_path, output_schema_path):   
    # Load the YAML schema
    with open(schema_path, 'r') as file:
        schema = yaml.load(file)

    # Load the Excel file with codes
    excel_data = pd.ExcelFile(excel_path)

    for tab in excel_data.sheet_names:
        # Read the current tab into a DataFrame
        df = excel_data.parse(tab)
        # Convert column names to lowercase
        df.columns = df.columns.str.lower()

        if 'code' not in df.columns or 'group' not in df.columns:
            print(f"Skipping tab '{tab}' as it does not have required columns")
            continue

        grouped = (
            df.groupby('group')['code']
            .apply(lambda x: list(map(str, x.unique())))
            .reset_index()
        )

        # Prepare category entries for the schema
        categories = [
            {
                "code": str(row['group']),
                "name": row['code'] 
            }
            for _, row in grouped.iterrows()
        ]

        # Start a count to know if any items in the schema have been updated
        updated_count = 0

        # Check if the tab matches a column name in the schema and only update category under that column
        for annex_a_list, columns in schema.get("column_map", {}).items():
            
            for column_name, column_coding in columns.items():
                tab_name = tab.replace(' ', '-').lower()
                anchor = column_coding.anchor.value
                
                if isinstance(column_coding, dict) and anchor is not None and column_coding.get("category") and tab_name == anchor:
                    print(tab_name)
                    print(anchor)
                    # Replace the old categories with the new ones
                    column_coding["category"] = categories
                    updated_count += 1

        if updated_count == 0:
            print(f"Skipping tab '{tab}' as it does not have a matching item in the schema")

    # Save the updated schema back to a YAML file
    with open(output_schema_path, 'w') as file:
        yaml.dump(schema, file)