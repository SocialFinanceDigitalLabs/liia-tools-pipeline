import unicodedata

import pandas as pd
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import SingleQuotedScalarString

yaml = YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=4, sequence=6, offset=2)

# Define a mapping of problematic characters to safe alternatives
CHARACTER_MAP = {
    "‘": "'",  # Left single quotation mark to straight single quote
    "’": "'",  # Right single quotation mark to straight single quote
    "“": '"',  # Left double quotation mark to straight double quote
    "”": '"',  # Right double quotation mark to straight double quote
    "–": "-",  # En dash to hyphen
    "—": "-",  # Em dash to hyphen
}


def replace_special_characters(value):
    """
    Replaces special characters using CHARACTER_MAP.
    """
    if isinstance(value, str):
        for special, replacement in CHARACTER_MAP.items():
            value = value.replace(special, replacement)
    return value


def ensure_quoted(value):
    """
    Ensures the value is a string and wraps it in single quotes if necessary.
    """
    value = replace_special_characters(str(value))  # Ensure it's a string
    if value.lower() in {"yes", "no", "true", "false", "y", "n"}:
        return SingleQuotedScalarString(value)  # Force single quotes
    return value


def build_schema(schema_path, excel_path, output_schema_path):
    # Load the YAML schema
    with open(schema_path, "r") as file:
        schema = yaml.load(file)

    # Load the Excel file with codes
    excel_data = pd.ExcelFile(excel_path)

    for tab in excel_data.sheet_names:
        # Read the current tab into a DataFrame
        df = excel_data.parse(tab)
        # Convert column names to lowercase
        df.columns = df.columns.str.lower()

        # Check if the tab has the required columns
        if "code" not in df.columns or "group" not in df.columns:
            print(f"Skipping tab '{tab}' as it does not have required columns")
            continue

        # Group the codes by the group column to ensure 1:many dict structure
        grouped = (
            df.groupby("group")["code"]
            .apply(lambda x: list(map(str, x.unique())))
            .reset_index()
        )

        # Prepare category entries for the schema
        categories = [
            {
                "code": str(row["group"]),
                "name": [ensure_quoted(code) for code in row["code"]]
                if isinstance(row["code"], list)
                else ensure_quoted(row["code"]),
            }
            for _, row in grouped.iterrows()
        ]

        # Start a count to know if any items in the schema have been updated
        updated_count = 0

        # Check if the tab matches a column name in the schema and only update category under that column
        for annex_a_list, columns in schema.get("column_map", {}).items():
            for column_name, column_coding in columns.items():
                tab_name = tab.replace(" ", "-").lower()
                anchor = column_coding.anchor.value
                column_coding.anchor.always_dump = True

                if (
                    isinstance(column_coding, dict)
                    and anchor is not None
                    and column_coding.get("category")
                    and tab_name == anchor
                ):
                    # Replace the old categories with the new ones
                    column_coding["category"] = categories
                    updated_count += 1

        # Print a message if no items in the schema have been updated
        if updated_count == 0:
            print(
                f"Skipping tab '{tab}' as it does not have a matching item in the schema"
            )

    # Save the updated schema back to a YAML file
    with open(output_schema_path, "w", encoding="utf-8") as file:
        yaml.dump(schema, file)
