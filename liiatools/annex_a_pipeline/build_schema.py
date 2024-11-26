import pandas as pd
import yaml


def build_schema(schema_path, excel_path, output_schema_path):
    
    # Load the YAML schema
    with open(schema_path, 'r') as file:
        schema = yaml.safe_load(file)

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

        # Check if the tab matches a column name in the schema and only update category under that column
        for annex_a_list, columns in schema.get("column_map", {}).items():
            for column_name, column_coding in columns.items():
                if isinstance(column_coding, dict) and column_coding.get("category") and tab in column_name.lower():
                    # Replace the old categories with the new ones
                    column_coding["category"] = categories

    # Save the updated schema back to a YAML file
    with open(output_schema_path, 'w') as file:
        yaml.dump(schema, file, default_flow_style=False, allow_unicode=True, indent=4)