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
