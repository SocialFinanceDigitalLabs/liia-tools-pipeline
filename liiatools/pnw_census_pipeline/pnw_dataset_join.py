import numpy as np
import pandas as pd
from dagster import get_dagster_logger

log = get_dagster_logger(__name__)


def _filter_to_open_in_last_12m(
    df: pd.DataFrame, episode_start: str, snapshot_date: str, episode_end: str
) -> pd.DataFrame:
    """
    Filters dataframe to episodes that are open within 12 months before the snapshot date
    """
    filtered_df = df[
        # Opened before or on the snapshot date
        (df[episode_start] <= df[snapshot_date])
        &
        # And
        (
            # Closed no earlier than 12 months prior to the shapshot
            (df[episode_end] >= (df[snapshot_date] - pd.DateOffset(months=12)))
            |
            # Or are still open
            (df[episode_end].isna())
        )
    ]

    return filtered_df


def _filter_to_open_on_snapshot_date(
    df: pd.DataFrame, episode_end: str, snapshot_date: str, episode_start: str
) -> pd.DataFrame:
    """
    Filters dataframe to episodes that are open on the snapshot date
    Removes episodes that opened on the snapshot date to
    avoid counting two episodes for the same child where:
    - One closed on the snapshot date
    - One opened on the snapshot date
    """
    filtered_df = df[
        ((df[episode_end].isna()) | (df[episode_end] >= df[snapshot_date]))
        & (df[episode_start] < df[snapshot_date])
    ]

    return filtered_df


def join_episode_data(episodes: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from 903 episodes dataframe onto pnw census dataframe
    Returns pnw census dataframe
    """
    # Join pnw_census onto episodes, keeping only children in episodes with a match in pnw
    episodes_merged = episodes.merge(
        pnw_census[["Identifier", "snapshot_date"]],
        left_on="CHILD",
        right_on="Identifier",
        how="inner",
    )

    # Filter to episodes open within 12 months before the snapshot date
    episodes_merged = _filter_to_open_in_last_12m(
        episodes_merged, "DECOM", "snapshot_date", "DEC"
    )

    # Count episodes per child
    episodes_merged["# placements in last 12 months"] = episodes_merged.groupby(
        ["CHILD", "snapshot_date"]
    )["CHILD"].transform("count")

    # Filter to only keep episodes open on day of snapshot
    episodes_merged = _filter_to_open_on_snapshot_date(
        episodes_merged, "DEC", "snapshot_date", "DECOM"
    )

    # Merge back with pnw_census
    pnw_census_merged = pnw_census.merge(
        episodes_merged[
            [
                "CHILD",
                "PLACE",
                "PLACE_PROVIDER",
                "HOME_POST",
                "PL_POST",
                "# placements in last 12 months",
                "snapshot_date",
            ]
        ],
        left_on=["Identifier", "snapshot_date"],
        right_on=["CHILD", "snapshot_date"],
        how="left",
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census) == len(pnw_census_merged)
    except AssertionError:
        log.error(
            f"Join with 903 episodes results in incorrect row count: {len(pnw_census_merged)-len(pnw_census)} additional rows."
        )

    # Log number of joins made
    joins = pnw_census_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 episodes file")

    # Rename columns according to schema and drop unnecessary identifier
    pnw_census_merged = pnw_census_merged.rename(
        columns={
            "PLACE": "903 placement type",
            "PLACE_PROVIDER": "903 provider type",
            "HOME_POST": "Home postcode",
            "PL_POST": "Placement postcode",
        }
    )
    pnw_census_merged = pnw_census_merged.drop(columns="CHILD")

    return pnw_census_merged


def join_header_data(header: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from 903 header dataframe onto pnw census dataframe
    Returns pnw census dataframe
    """
    pnw_census_merged = pnw_census.merge(
        header[["CHILD", "SEX", "ETHNIC"]],
        left_on="Identifier",
        right_on="CHILD",
        how="left",
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census) == len(pnw_census_merged)
    except AssertionError:
        log.error(
            f"Join with 903 header results in incorrect row count: {len(pnw_census_merged)-len(pnw_census)} additional rows."
        )

    # Log number of joins made
    joins = pnw_census_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 header file")

    # Rename columns according to schema and drop unnecessary identifier
    pnw_census_merged = pnw_census_merged.rename(
        columns={"SEX": "Gender 903", "ETHNIC": "Ethnicity 903"}
    )
    pnw_census_merged = pnw_census_merged.drop(columns="CHILD")

    return pnw_census_merged


def join_uasc_data(uasc: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from 903 UASC dataframe onto pnw census dataframe
    Returns pnw census dataframe
    """
    pnw_census_merged = pnw_census.merge(
        uasc[["CHILD", "DUC"]], left_on="Identifier", right_on="CHILD", how="left"
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census) == len(pnw_census_merged)
    except AssertionError:
        log.error(
            f"Join with 903 uasc results in incorrect row count: {len(pnw_census_merged)-len(pnw_census)} additional rows."
        )

    # Log number of joins made
    joins = pnw_census_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 uasc file")

    # Create new column for when snapshot date is less than or equal to DUC
    pnw_census_merged["UASC 903"] = np.where(
        pnw_census_merged["snapshot_date"] <= pnw_census_merged["DUC"], 1, 0
    )

    pnw_census_merged = pnw_census_merged.drop(columns=["CHILD", "DUC"])

    return pnw_census_merged


def join_oc2_data(oc2: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from 903 OC2 dataframe onto pnw census dataframe
    Returns pnw census dataframe
    """
    # Join pnw_census onto OC2, keeping only children in episodes with a OC2 in PNW
    oc2_merged = oc2.merge(
        pnw_census[["Identifier", "snapshot_date"]],
        left_on="CHILD",
        right_on="Identifier",
        how="inner",
    )

    # Filter to OC2 entries from the same FY as the snapshot date
    oc2_merged["start"] = pd.to_datetime(
        (oc2_merged["YEAR"] - 1).astype(str) + "-04-01"
    )
    oc2_merged["end"] = pd.to_datetime((oc2_merged["YEAR"]).astype(str) + "-03-31")
    oc2_merged = oc2_merged[
        (oc2_merged["start"] <= oc2_merged["snapshot_date"])
        & (oc2_merged["end"] >= oc2_merged["snapshot_date"])
    ]

    # Ensure only one result per child, keeping entries with the most complete data if multiple
    oc2_merged["non_blank_count"] = oc2_merged.notna().sum(axis=1)
    oc2_merged = oc2_merged.sort_values(by="non_blank_count", ascending=False)
    oc2_merged = oc2_merged.drop_duplicates("Identifier", keep="first")

    # Merge back with pnw_census
    pnw_census_merged = pnw_census.merge(
        oc2_merged[
            [
                "CHILD",
                "CONVICTED",
                "SUBSTANCE_MISUSE",
                "INTERVENTION_RECEIVED",
                "INTERVENTION_OFFERED",
            ]
        ],
        left_on="Identifier",
        right_on="CHILD",
        how="left",
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census) == len(pnw_census_merged)
    except AssertionError:
        log.error(
            f"Join with 903 oc2 results in incorrect row count: {len(pnw_census_merged)-len(pnw_census)} additional rows."
        )

    # Log number of joins made
    joins = pnw_census_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 oc2 file")

    # Rename columns according to schema and drop unnecessary identifier
    pnw_census_merged = pnw_census_merged.rename(
        columns={
            "CONVICTED": "Child convicted during the year",
            "SUBSTANCE_MISUSE": "Child identified as having a substance misuse problem",
            "INTERVENTION_RECEIVED": "Child received intervention for substance misuse problem",
            "INTERVENTION_OFFERED": "Child offered intervention for substance misuse problem",
        }
    )
    pnw_census_merged = pnw_census_merged.drop(columns="CHILD")

    return pnw_census_merged


def join_missing_data(missing: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from 903 missing dataframe onto pnw census dataframe
    Returns pnw census dataframe
    """
    # Join pnw_census onto missing, keeping only children in episodes with a match in pnw
    missing_merged = missing.merge(
        pnw_census[["Identifier", "snapshot_date"]],
        left_on="CHILD",
        right_on="Identifier",
        how="inner",
    )

    # Filter to episodes open within 12 months before the snapshot date
    missing_merged = _filter_to_open_in_last_12m(
        missing_merged, "MIS_START", "snapshot_date", "MIS_END"
    )

    # Count episodes per child
    missing_merged = (
        missing_merged.groupby("CHILD")["CHILD"]
        .count()
        .to_frame(name="# missing episodes in last 12 months")
    )

    # Merge back with pnw_census
    pnw_census_merged = pnw_census.merge(
        missing_merged, left_on="Identifier", right_index=True, how="left"
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census) == len(pnw_census_merged)
    except AssertionError:
        log.error(
            f"Join with 903 missing results in incorrect row count: {len(pnw_census_merged)-len(pnw_census)} additional rows."
        )

    # Log number of joins made
    joins = pnw_census_merged["# missing episodes in last 12 months"].count()
    log.info(f"{joins} joins made from 903 missing file")

    return pnw_census_merged


def join_onspd_data(postcode: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from ONSPD dataframe onto pnw census dataframe
    Returns pnw census dataframe
    """
    # Format postcode column to:
    # - have only one space between first and second halves
    # - remove trailing and leading spaces
    postcode["pcd2"] = postcode["pcd2"].str.replace(r"\s+", " ", regex=True).str.strip()

    # Merge postcode table on pnw_census using home postcode
    pnw_census_merged = pnw_census.merge(
        postcode[["pcd2", "oseast1m", "osnrth1m"]],
        left_on="Home postcode",
        right_on="pcd2",
        how="left",
    )
    pnw_census_merged = pnw_census_merged.rename(
        columns={
            "oseast1m": "Home eastings",
            "osnrth1m": "Home northings",
        }
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census) == len(pnw_census_merged)
    except AssertionError:
        log.error(
            f"Join with ONSPD home postcode results in incorrect row count: {len(pnw_census_merged)-len(pnw_census)} additional rows."
        )

    # Log number of joins made
    joins = pnw_census_merged["pcd2"].notna().count()
    log.info(f"{joins} joins made from ONSPD on home postcode")

    pnw_census_merged = pnw_census_merged.drop(columns="pcd2")

    # Merge postcode table on pnw_census using placment postcode
    pnw_census_merged = pnw_census_merged.merge(
        postcode[["pcd2", "oseast1m", "osnrth1m", "oslaua"]],
        left_on="Placement postcode",
        right_on="pcd2",
        how="left",
    )
    pnw_census_merged = pnw_census_merged.rename(
        columns={
            "oseast1m": "Placement eastings",
            "osnrth1m": "Placement northings",
            "oslaua": "Placement LA code",
        }
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census) == len(pnw_census_merged)
    except AssertionError:
        log.error(
            f"Join with ONSPD placement postcode results in incorrect row count: {len(pnw_census_merged)-len(pnw_census)} additional rows."
        )

    # Log number of joins made
    joins = pnw_census_merged["pcd2"].notna().count()
    log.info(f"{joins} joins made from ONSPD on placement postcode")

    pnw_census_merged = pnw_census_merged.drop(columns="pcd2")

    return pnw_census_merged


def add_missing_cans_columns(data: pd.DataFrame, cans_columns: list) -> pd.DataFrame:
    """
    Ensures all all 4 columns exist for each variable - add missing ones as blank
    """
    for i in range(1, 5):
        for var in cans_columns:
            col_name = f"{var} {i}"
            if col_name not in data.columns:
                data[col_name] = pd.NA
    return data


def join_cans_data(
    cans: pd.DataFrame, pnw_census: pd.DataFrame, cans_columns: list
) -> pd.DataFrame:
    """
    Merges data from CANS dataframe onto PNW census dataframe
    Returns PNW census dataframe
    """

    # Make sure dates are datetime
    cans["Assessment Date"] = pd.to_datetime(cans["Assessment Date"])

    # Remove any CANS assessments after the snapshot date
    cans = cans[cans["Assessment Date"] <= pnw_census["snapshot_date"].max()]

    # Sort so latest are first
    cans = cans.sort_values(
        ["Child Unique ID", "Assessment Date"], ascending=[True, False]
    )

    # Rank assessments for each child
    cans["rank"] = cans.groupby("Child Unique ID")["Assessment Date"].rank(
        method="first", ascending=False
    )

    # Keep only 4 most recent assessments
    cans_top4 = cans[cans["rank"] <= 4]

    # Pivot everything except Child Unique ID
    wide_cans = cans_top4.melt(id_vars=["Child Unique ID", "rank"]).pivot(
        index="Child Unique ID", columns=["rank", "variable"], values="value"
    )

    # Flatten multi-level columns
    wide_cans.columns = [f"{var} {int(rank)}" for rank, var in wide_cans.columns]
    wide_cans = wide_cans.reset_index()

    wide_cans = add_missing_cans_columns(wide_cans, cans_columns)

    # Reorder columns
    cols_order = ["Child Unique ID"] + [
        f"{var} {i}" for i in range(1, 5) for var in cans_columns
    ]
    wide_cans = wide_cans[cols_order]

    # Merge with pnw_census
    pnw_census_merged = pnw_census.merge(
        wide_cans, left_on="Identifier", right_on="Child Unique ID", how="left"
    )

    # Row count in pnw_census should not have changed
    try:
        assert len(pnw_census) == len(pnw_census_merged)
    except AssertionError:
        log.error(
            f"Join with CANS assessments results in incorrect row count: {len(pnw_census_merged)-len(pnw_census)} additional rows."
        )

    # Log number of joins made
    joins = pnw_census_merged[
        pnw_census_merged["Child Unique ID"].notna()
        & (pnw_census_merged["Child Unique ID"] != "")
    ].shape[0]
    log.info(f"{joins} joins made from CANS on Identifier")

    pnw_census_merged = pnw_census_merged.drop(columns="Child Unique ID")

    return pnw_census_merged
