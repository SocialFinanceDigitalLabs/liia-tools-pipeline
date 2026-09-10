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


def join_latest_episodes_data(episodes: pd.DataFrame, header: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from the latest 903 episodes for each child onto the 903 header dataframe
    Returns 903 header dataframe
    """
    episodes["DECOM"] = pd.to_datetime(episodes["DECOM"])

    # Keep only the latest episode for each child
    latest_episodes = (
        episodes.sort_values(["CHILD", "DECOM"], ascending=[True, False])
        .drop_duplicates(subset="CHILD", keep="first")
    )

    header_merged = header.merge(
        latest_episodes[["CHILD", "CIN"]], on="CHILD", how="left",
    )

    # Row number in header should not have changed
    try:
        assert len(header) == len(header_merged)
    except AssertionError:
        log.error(
            f"Join with 903 episodes results in incorrect row count: {len(header_merged)-len(header)} additional rows."
        )

    # Log number of joins made
    joins = header_merged["CIN"].count()
    log.info(f"{joins} joins made from 903 episodes file")

    return header_merged


def join_latest_uasc_data(uasc: pd.DataFrame, header: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from the latest 903 UASC dataframe onto 903 header dataframe
    Returns 903 header dataframe
    """
    header_merged = header.merge(
        uasc[["CHILD", "DUC"]], on="CHILD", how="left"
    )

    # Row number in header should not have changed
    try:
        assert len(header) == len(header_merged)
    except AssertionError:
        log.error(
            f"Join with 903 uasc results in incorrect row count: {len(header_merged)-len(header)} additional rows."
        )

    # Log number of joins made
    joins = header_merged["DUC"].count()
    log.info(f"{joins} joins made from 903 uasc file")

    #TODO check if we need to filter DUC for regarding header
    # # Create new column for when snapshot date is less than or equal to DUC
    # header_merged["UASC 903"] = np.where(
    #     header_merged["snapshot_date"] <= header_merged["DUC"], 1, 0
    # )

    # header_merged = header_merged.drop(columns=["CHILD", "DUC"])

    return header_merged


def join_latest_oc2_data(oc2: pd.DataFrame, header: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from the latest 903 oc2 dataframe onto 903 header dataframe
    Returns 903 header dataframe
    """
    header_merged = header.merge(
        oc2[["CHILD", "SDQ_SCORE"]], on="CHILD", how="left"
    )

    # Row number in header should not have changed
    try:
        assert len(header) == len(header_merged)
    except AssertionError:
        log.error(
            f"Join with 903 oc2 results in incorrect row count: {len(header_merged)-len(header)} additional rows."
        )

    # Log number of joins made
    joins = header_merged["SDQ_SCORE"].count()
    log.info(f"{joins} joins made from 903 oc2 file")

    return header_merged


def join_pnw_data(pnw_census: pd.DataFrame, episodes: pd.DataFrame, pnw_join_columns: list) -> pd.DataFrame:
    """
    Merges data from pnw census dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe
    """
    # Join pnw_census onto episodes, keeping all children in episodes
    episodes_merged = episodes.merge(
        pnw_census[["Identifier", "snapshot_date"] + pnw_join_columns],
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

    # Drop unnecessary columns
    episodes_merged = episodes_merged.drop(columns=["snapshot_date", "Identifier"])

    return episodes_merged


def join_placement_standard_data(placement_standard: pd.DataFrame, episodes: pd.DataFrame, placement_standard_join_columns: list) -> pd.DataFrame:
    """
    Merges data from placement standard dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe
    """
    episodes_merged = episodes.merge(
        placement_standard[["child_ID",] + placement_standard_join_columns],
        left_on="CHILD",
        right_on="child_ID",
        how="left",
    )

    # Row number in episodes should not have changed
    try:
        assert len(episodes) == len(episodes_merged)
    except AssertionError:
        log.error(
            f"Join with Placement Standard results in incorrect row count: {len(episodes_merged)-len(episodes)} additional rows."
        )

    # Log number of joins made
    joins = episodes_merged["child_ID"].count()
    log.info(f"{joins} joins made from Placement Standard file")

    # Drop unnecessary columns
    episodes_merged = episodes_merged.drop(columns=["child_ID"])

    return episodes_merged


def join_latest_cans_data(
    cans: pd.DataFrame, header: pd.DataFrame, cans_columns: list
) -> pd.DataFrame:
    """
    Merges data from CANS dataframe onto 903 header dataframe
    Returns 903 header dataframe
    """

    # Make sure dates are datetime
    cans["Assessment Date"] = pd.to_datetime(cans["Assessment Date"])

    # Keep only the latest CANS assessment for each child
    latest_cans = (
        cans.sort_values(["Child Unique ID", "Assessment Date"], ascending=[True, False])
        .drop_duplicates(subset="Child Unique ID", keep="first")
    )

    # Merge with header
    header_merged = header.merge(
        latest_cans, left_on="CHILD", right_on="Child Unique ID", how="left"
    )

    # Row count in header should not have changed
    try:
        assert len(header) == len(header_merged)
    except AssertionError:
        log.error(
            f"Join with CANS assessments results in incorrect row count: {len(header_merged)-len(header)} additional rows."
        )

    # Log number of joins made
    joins = header_merged[
        header_merged["Child Unique ID"].notna()
        & (header_merged["Child Unique ID"] != "")
    ].shape[0]
    log.info(f"{joins} joins made from CANS on CHILD")

    header_merged = header_merged.drop(columns="Child Unique ID")

    return header_merged


def join_latest_placement_standard_data(
    placement_standard: pd.DataFrame, header: pd.DataFrame, placement_standard_join_columns: list
    ) -> pd.DataFrame:
    """
    Merges data from placement standard dataframe onto 903 header dataframe
    Returns 903 header dataframe
    """
    # Make sure dates are datetime
    placement_standard["placement_start_date"] = pd.to_datetime(placement_standard["placement_start_date"])

    # Keep only the latest Placement Standard assessment for each child
    latest_placement_standard = (
        placement_standard.sort_values(["child_ID", "placement_start_date"], ascending=[True, False])
        .drop_duplicates(subset="child_ID", keep="first")
    )

    header_merged = header.merge(
        latest_placement_standard[["child_ID",] + placement_standard_join_columns],
        left_on="CHILD",
        right_on="child_ID",
        how="left",
    )

    # Row number in header should not have changed
    try:
        assert len(header) == len(header_merged)
    except AssertionError:
        log.error(
            f"Join with Placement Standard results in incorrect row count: {len(header_merged)-len(header)} additional rows."
        )

    # Log number of joins made
    joins = header_merged["child_ID"].count()
    log.info(f"{joins} joins made from Placement Standard file")

    # Drop unnecessary columns
    header_merged = header_merged.drop(columns=["child_ID"])

    return header_merged