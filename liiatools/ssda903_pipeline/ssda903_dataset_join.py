import numpy as np
import pandas as pd
from dagster import get_dagster_logger

from liiatools.pnw_census_pipeline.pnw_dataset_join import (
    _filter_to_open_on_snapshot_date,
)

log = get_dagster_logger(__name__)

def join_header_data(header: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from 903 header dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe
    """
    episodes_merged = episodes.merge(
        header[["CHILD", "SEX", "ETHNIC", "DOB"]],
        on="CHILD",
        how="left",
    )

    # Row number in episodes should not have changed
    try:
        assert len(episodes) == len(episodes_merged)
    except AssertionError:
        log.error(
            f"Join with 903 header results in incorrect row count: {len(episodes_merged)-len(episodes)} additional rows."
        )

    # Log number of joins made
    joins = episodes_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 header file")

    return episodes_merged


def join_uasc_data(uasc: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from 903 UASC dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe
    """
    episodes_merged = episodes.merge(
        uasc[["CHILD", "DUC"]], on="CHILD", how="left"
    )

    # Row number in episodes should not have changed
    try:
        assert len(episodes) == len(episodes_merged)
    except AssertionError:
        log.error(
            f"Join with 903 uasc results in incorrect row count: {len(episodes_merged)-len(episodes)} additional rows."
        )

    # Log number of joins made
    joins = episodes_merged["DUC"].count()
    log.info(f"{joins} joins made from 903 uasc file")

    #TODO check if we need to filter DUC for regarding episodes
    # # Create new column for when snapshot date is less than or equal to DUC
    # episodes_merged["UASC 903"] = np.where(
    #     episodes_merged["snapshot_date"] <= episodes_merged["DUC"], 1, 0
    # )

    # episodes_merged = episodes_merged.drop(columns=["CHILD", "DUC"])

    return episodes_merged


def join_pnw_data(pnw_census: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from pnw census dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe
    """
    # Join pnw_census onto episodes, keeping all children in episodes
    episodes_merged = episodes.merge(
        pnw_census[["Identifier", "snapshot_date", "Type of provision", "Primary Registration type"]],
        left_on="CHILD",
        right_on="Identifier",
        how="left",
    )

    # Filter to only keep episodes open on day of snapshot
    episodes_merged = _filter_to_open_on_snapshot_date(
        episodes_merged, "DEC", "snapshot_date", "DECOM"
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(episodes) == len(episodes_merged)
    except AssertionError:
        log.error(
            f"Join with PNW Census results in incorrect row count: {len(episodes_merged)-len(episodes)} additional rows."
        )

    # Log number of joins made
    joins = episodes_merged["snapshot_date"].count()
    log.info(f"{joins} joins made from PNW Census file")

    # Rename columns according to schema and drop unnecessary identifier
    episodes_merged = episodes_merged.drop(columns=["snapshot_date", "Identifier"])

    return episodes_merged