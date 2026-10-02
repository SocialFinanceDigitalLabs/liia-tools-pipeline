import pandas as pd
from dagster import get_dagster_logger

from liiatools.ssda903_pipeline.ssda903_dataset_join import _get_unmatched_rows

log = get_dagster_logger(__name__)

def join_episodes_data(episodes: pd.DataFrame, placements_standard: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from 903 episodes dataframe onto placements standard dataframe
    Returns placements standard dataframe and unmatched placements dataframe
    """
    unmatched_placements = _get_unmatched_rows(
            episodes, placements_standard, ["child_ID", "placement_start_date"], ["CHILD", "DECOM"], "episodes", "placements_standard"
        )
    
    placements_standard_merged = placements_standard.merge(
        episodes[["CHILD", "DECOM", "EPISODE_ID"]],
        left_on=["child_ID", "placement_start_date"],
        right_on=["CHILD", "DECOM"],
        how="left",
    )

    # Row number in episodes should not have changed
    try:
        assert len(placements_standard) == len(placements_standard_merged)
    except AssertionError:
        log.error(
            f"Join with 903 episodes results in incorrect row count: {len(placements_standard_merged)-len(placements_standard)} additional rows."
        )

    # Log number of joins made
    joins = placements_standard_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 episodes file")

    placements_standard_merged = placements_standard_merged.drop(columns=["CHILD", "DECOM"])

    return placements_standard_merged, unmatched_placements