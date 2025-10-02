import pandas as pd

def dedup_cin(data: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicates CIN Census data at a CIN episode level
    :param data: Concatenated CIN Census flatfile dataframe
    :return: Deduplicated CIN Census dataframe
    """
    # Find the latest year for each set of child_id and CIN referral date sets
    latest_years = (
        data.groupby(["LAchildID", "CINreferralDate"])["Year"].transform("max")
    )

    # Filter to only include latest_years rows
    deduplicated_data = data[data["Year"] == latest_years]

    return deduplicated_data
