import pandas as pd


def add_summary_sheet_columns(
    data: pd.DataFrame, mapping: dict, column_order: list
) -> pd.DataFrame:
    """
    Add summary sheet columns to a dataframe based on a mapping dictionary.
    :param data: The dataframe to add the columns to
    :param mapping: The mapping dictionary
    :return: The dataframe with the new columns added
    """
    for col in column_order:
        data[col] = ""

    for idx, row in data.iterrows():
        for field, value in row.items():
            if field in mapping and str(value) in mapping[field]:
                target_col = mapping[field][str(value)]
                current_val = data.at[idx, target_col]
                if current_val:
                    data.at[idx, target_col] = current_val + ";" + field
                else:
                    data.at[idx, target_col] = field
    return data


def map_summary_planning(row, mapping: dict) -> str:
    """
    Map a row to its summary planning value based on the mapping dictionary.
    :param row: The row of the dataframe
    :param mapping: The mapping dictionary
    :return: The summary planning value for the row
    """
    subcat = row["CANS subcategory"]
    score = row["Score"]

    if pd.isna(score):
        return ""

    score_str = str(score).strip()

    # If it's a numeric score (e.g. 2, '2', 2.0), convert to int string '2'
    try:
        score_val = float(score_str)
        if score_val.is_integer():
            score_str = str(int(score_val))
    except (ValueError, TypeError):
        # Keeps text like 'Yes', 'No', etc.
        pass

    # Lookup subcategory, then score, default to "" if not found
    return mapping.get(subcat, {}).get(score_str, "")
