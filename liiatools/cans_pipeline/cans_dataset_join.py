import pandas as pd
from dagster import get_dagster_logger

log = get_dagster_logger(__name__)


def join_episodes_data(cans: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from 903 episodes dataframe onto CANS dataframe
    Returns CANS dataframe
    """

    # Make sure dates are datetime
    cans["Assessment Date"] = pd.to_datetime(cans["Assessment Date"], dayfirst=True).dt.date
    episodes[["DECOM", "DEC"]] = episodes[["DECOM", "DEC"]].apply(lambda x: pd.to_datetime(x, dayfirst=True).dt.date)

    # Track original cans rows so unmatched rows can be added back after filtering
    cans = cans.reset_index(drop=True)
    cans["_cans_row_id"] = cans.index

    cans_merged = cans.merge(
        episodes[["CHILD", "DECOM", "DEC", "EPISODE_ID"]],
        left_on="Child Unique ID",
        right_on="CHILD",
        how="left",
    )

    # Keep only the episode open on the assessment date
    # A blank DEC means the episode is still ongoing, so there is no upper bound
    in_episode = cans_merged["DECOM"].isna() | (
        (cans_merged["DECOM"] <= cans_merged["Assessment Date"])
        & (cans_merged["DEC"].isna() | (cans_merged["Assessment Date"] < cans_merged["DEC"]))
    )
    cans_merged = cans_merged[in_episode]

    # Episodes don't overlap, so each cans row matches at most one episode above;
    # add back any cans rows that didn't match, with a blank EPISODE_ID rather than dropping them
    unmatched = cans[~cans["_cans_row_id"].isin(cans_merged["_cans_row_id"])]
    cans_merged = pd.concat([cans_merged, unmatched]).sort_values("_cans_row_id").reset_index(drop=True)

    # Row number in cans should not have changed
    try:
        assert len(cans) == len(cans_merged)
    except AssertionError:
        log.error(
            f"Join with 903 episodes results in incorrect row count: {len(cans_merged)-len(cans)} additional rows."
        )

    # Log number of joins made
    joins = cans_merged.loc[cans_merged["EPISODE_ID"].notna(), "CHILD"].nunique()
    log.info(f"{joins} joins made from 903 episodes file")

    cans_merged = cans_merged.drop(columns=["CHILD", "DECOM", "DEC", "_cans_row_id"])

    return cans_merged