import pandas as pd
from dagster import get_dagster_logger

log = get_dagster_logger(__name__)


def join_episodes_data(episodes: pd.DataFrame, cans: pd.DataFrame, identifier_col: str) -> pd.DataFrame:
    """
    Merges data from 903 episodes dataframe onto CANS dataframe
    Returns CANS dataframe
    """

    # Make sure dates are datetime
    cans["Assessment Date"] = pd.to_datetime(cans["Assessment Date"])
    episodes[["DECOM", "DEC"]] = episodes[["DECOM", "DEC"]].apply(pd.to_datetime)


    cans_merged = cans.merge(
        episodes[["CHILD", "DECOM", "DEC", "EPISODE_ID"]],
        left_on=identifier_col,
        right_on="CHILD",
        how="left",
    )

    # Keep only the episode open on the assessment date (or unmatched cans rows)
    # A blank DEC means the episode is still ongoing, so there is no upper bound
    in_episode = cans_merged["DECOM"].isna() | (
        (cans_merged["DECOM"] <= cans_merged["Assessment Date"])
        & (cans_merged["DEC"].isna() | (cans_merged["Assessment Date"] <= cans_merged["DEC"]))
    )
    cans_merged = cans_merged[in_episode]

    # Row number in cans should not have changed
    try:
        assert len(cans) == len(cans_merged)
    except AssertionError:
        log.error(
            f"Join with 903 episodes results in incorrect row count: {len(cans_merged)-len(cans)} additional rows."
        )

    # Log number of joins made
    joins = cans_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 episodes file")

    cans_merged = cans_merged.drop(columns=["CHILD", "DECOM", "DEC"])

    return cans_merged