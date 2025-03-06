import pandas as pd
import numpy as np
from dagster import get_dagster_logger

log = get_dagster_logger(__name__)

def join_episode_data(episodes: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    '''
    Merges data from 903 episodes dataframe onto pnw census dataframe
    Returns pnw census dataframe
    '''
    # Join pnw_census onto episodes, keeping only children in episodes with a match in pnw
    episodes_merged = episodes.merge(
        pnw_census[[
            "Identifier",
            "snapshot_date"
        ]],
        left_on="CHILD",
        right_on="Identifier",
        how="inner"
    )

    # Filter to episodes open within 12 months before the snapshot date
    episodes_merged = episodes_merged[
        # Opened before or on the snapshot date
        (episodes_merged["DECOM"] <= episodes_merged["snapshot_date"]) &
        # And
        (
            # Closed no earlier than 12 months prior to the shapshot
            (episodes_merged["DEC"] >= (episodes_merged["snapshot_date"] - pd.DateOffset(months=12))) |
            # Or are still open
            (episodes_merged["DEC"].isna())
        )
    ]

    # Count episodes per child
    episodes_merged["# placements in last 12 months"] = episodes_merged.groupby("CHILD")["CHILD"].transform("count")

    # Keep only the most recent episode per child
    episodes_merged = episodes_merged.loc[episodes_merged.groupby("CHILD")["DECOM"].idxmax()]

    # Filter to only keep episodes open on day of snapshot
    episodes_merged = episodes_merged[
        (episodes_merged["DEC"].isna()) |
        (episodes_merged["DEC"] >= episodes_merged["snapshot_date"])
    ]

    # Merge back with pnw_census
    pnw_census_merged = pnw_census.merge(
        episodes_merged[[
            "CHILD",
            "PLACE",
            "PLACE_PROVIDER",
            "HOME_POST",
            "PL_POST"
        ]],
        left_on="Identifier",
        right_on="CHILD",
        how="left"
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census)==len(pnw_census_merged)
    except AssertionError as e:
        log.error(f"Join with 903 episodes results in too many rows: {e}")

    # Log number of joins made
    joins = pnw_census_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 episodes file")

    # Rename columns according to schema and drop unnecessary identifier 
    pnw_census_merged = pnw_census_merged.rename(columns={
        "PLACE":"903 placement type",
        "PLACE_PROVIDER":"903 provider type",
        "HOME_POST":"Home postcode",
        "PL_POST":"Placement postcode"
        })
    pnw_census_merged = pnw_census_merged.drop(columns="CHILD")

    return pnw_census_merged


def join_header_data(header: pd.DataFrame, pnw_census:pd.DataFrame) -> pd.DataFrame:
    '''
    Merges data from 903 header dataframe onto pnw census dataframe
    Returns pnw census dataframe
    '''
    pnw_census_merged = pnw_census.merge(header[[
        "CHILD",
        "SEX",
        "ETHNIC"
    ]],
    left_on="Identifier",
    right_on="CHILD",
    how="left")

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census)==len(pnw_census_merged)
    except AssertionError as e:
        log.error(f"Join with 903 header results in too many rows: {e}")

    # Log number of joins made
    joins = pnw_census_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 header file")

    # Rename columns according to schema and drop unnecessary identifier 
    pnw_census_merged = pnw_census_merged.rename(columns={
        "SEX":"Gender 903",
        "ETHNIC":"Ethnicity 903"
        })
    pnw_census_merged = pnw_census_merged.drop(columns="CHILD")

    return pnw_census_merged


def join_uasc_data(uasc: pd.DataFrame, pnw_census:pd.DataFrame) -> pd.DataFrame:
    '''
    Merges data from 903 UASC dataframe onto pnw census dataframe
    Returns pnw census dataframe
    '''
    pnw_census_merged = pnw_census.merge(uasc[[
        "CHILD",
        "DUC"
    ]],
    left_on="Identifier",
    right_on="CHILD",
    how="left")

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census)==len(pnw_census_merged)
    except AssertionError as e:
        log.error(f"Join with 903 uasc results in too many rows: {e}")

    # Log number of joins made
    joins = pnw_census_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 uasc file")

    # Create new column for when snapshot date is less than or equal to DUC
    pnw_census_merged["UASC 903"] = np.where(
        pnw_census_merged["DUC"].notna() &
        (pnw_census_merged["snapshot_date"] <= pnw_census_merged["DUC"]),
        1, 0
    )

    pnw_census_merged = pnw_census_merged.drop(columns=["CHILD", "DUC"])

    return pnw_census_merged


def join_oc2_data(oc2: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    '''
    Merges data from 903 OC2 dataframe onto pnw census dataframe
    Returns pnw census dataframe
    '''
    # Join pnw_census onto OC2, keeping only children in episodes with a OC2 in PNW
    oc2_merged = oc2.merge(
        pnw_census[[
            "Identifier",
            "snapshot_date"
        ]],
        left_on="CHILD",
        right_on="Identifier",
        how="inner"
    )

    # Filter to OC2 entries from the same FY as the snapshot date
    oc2_merged["start"] = pd.to_datetime((oc2_merged["YEAR"] - 1).astype(str) + "-04-01")
    oc2_merged["end"] = pd.to_datetime((oc2_merged["YEAR"]).astype(str) + "-03-31")
    oc2_merged = oc2_merged[
        (oc2_merged["start"] <= oc2_merged["snapshot_date"]) &
        (oc2_merged["end"] >= oc2_merged["snapshot_date"])
    ]

    # Ensure only one result per child, keeping entries with the most complete data if multiple
    oc2_merged["non_blank_count"] = oc2_merged.notna().sum(axis=1)
    oc2_merged = oc2_merged.sort_values(by="non_blank_count", ascending=False)
    oc2_merged = oc2_merged.drop_duplicates("Identifier", keep="first")

    # Merge back with pnw_census
    pnw_census_merged = pnw_census.merge(
        oc2_merged[[
            "CHILD",
            "CONVICTED",
            "SUBSTANCE_MISUSE",
            "INTERVENTION_RECEIVED",
            "INTERVENTION_OFFERED"
        ]],
        left_on="Identifier",
        right_on="CHILD",
        how="left"
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census)==len(pnw_census_merged)
    except AssertionError as e:
        log.error(f"Join with 903 oc2 results in too many rows: {e}")

    # Log number of joins made
    joins = pnw_census_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 oc2 file")

    # Rename columns according to schema and drop unnecessary identifier
    pnw_census_merged = pnw_census_merged.rename(columns={
        "CONVICTED":"Child convicted during the year",
        "SUBSTANCE_MISUSE":"Child identified as having a substance misuse problem",
        "INTERVENTION_RECEIVED":"Child received intervention for substance misuse problem",
        "INTERVENTION_OFFERED":"Child offered intervention for substance misuse problem"
        })
    pnw_census_merged = pnw_census_merged.drop(columns="CHILD")

    return pnw_census_merged


def join_missing_data(missing: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    '''
    Merges data from 903 missing dataframe onto pnw census dataframe
    Returns pnw census dataframe
    '''
    # Join pnw_census onto missing, keeping only children in episodes with a match in pnw
    missing_merged = missing.merge(
        pnw_census[[
            "Identifier",
            "snapshot_date"
        ]],
        left_on="CHILD",
        right_on="Identifier",
        how="inner"
    )

    # Filter to episodes open within 12 months before the snapshot date
    missing_merged = missing_merged[
        # Opened before or on the snapshot date
        (missing_merged["MIS_START"] <= missing_merged["snapshot_date"]) &
        # And
        (
            # Closed no earlier than 12 months prior to the shapshot
            (missing_merged["MIS_END"] >= (missing_merged["snapshot_date"] - pd.DateOffset(months=12))) |
            # Or are still open
            (missing_merged["MIS_END"].isna())
        )
    ]

    # Count episodes per child
    missing_merged["# missing episodes in last 12 months"] = missing_merged.groupby("CHILD")["CHILD"].transform("count")

    # Keep only the most recent episode per child
    missing_merged = missing_merged.loc[missing_merged.groupby("CHILD")["MIS_START"].idxmax()]

    # Merge back with pnw_census
    pnw_census_merged = pnw_census.merge(
        missing_merged[[
            "CHILD",
            "# missing episodes in last 12 months"
        ]],
        left_on="Identifier",
        right_on="CHILD",
        how="left"
    )

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census)==len(pnw_census_merged)
    except AssertionError as e:
        log.error(f"Join with 903 missing results in too many rows: {e}")

    # Log number of joins made
    joins = pnw_census_merged["CHILD"].nunique()
    log.info(f"{joins} joins made from 903 missing file")

    # Drop unnecessary identifier
    pnw_census_merged = pnw_census_merged.drop(columns="CHILD")

    return pnw_census_merged


def join_onspd_data(postcode: pd.DataFrame, pnw_census: pd.DataFrame) -> pd.DataFrame:
    '''
    Merges data from ONSPD dataframe onto pnw census dataframe
    Returns pnw census dataframe
    '''
    # Merge postcode table on pnw_census using home postcode
    pnw_census_merged = pnw_census.merge(
        postcode[[
            "pcd2",
            "oseast1m",
            "osnrth1m"
        ]],
        left_on="Home postcode",
        right_on="pcd2",
        how="left"
    )
    pnw_census_merged = pnw_census_merged.rename(columns={
        "oseast1m":"Home eastings",
        "osnrth1m":"Home northings",
        })

    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census)==len(pnw_census_merged)
    except AssertionError:
        log.error(f"Join with ONSPD home postcode results in too many rows: {len(pnw_census_merged)} rows now with {len(pnw_census)} before")

    # Log number of joins made
    joins = pnw_census_merged["pcd2"].notna().count()
    log.info(f"{joins} joins made from ONSPD on home postcode")

    pnw_census_merged = pnw_census_merged.drop(columns="pcd2")

    # Merge postcode table on pnw_census using placment postcode
    pnw_census_merged = pnw_census_merged.merge(
        postcode[[
            "pcd2",
            "oseast1m",
            "osnrth1m",
            "oslaua"
        ]],
        left_on="Placement postcode",
        right_on="pcd2",
        how="left"
    )
    pnw_census_merged = pnw_census_merged.rename(columns={
        "oseast1m":"Placement eastings",
        "osnrth1m":"Placement northings",
        "oslaua":"Placement LA code"
        })
    
    # Row number in pnw_census should not have changed
    try:
        assert len(pnw_census)==len(pnw_census_merged)
    except AssertionError:
        log.error(f"Join with ONSPD placement postcode results in too many rows: {len(pnw_census_merged)} rows now with {len(pnw_census)} before")

    # Log number of joins made
    joins = pnw_census_merged["pcd2"].notna().count()
    log.info(f"{joins} joins made from ONSPD on placement postcode")

    pnw_census_merged = pnw_census_merged.drop(columns="pcd2")

    return pnw_census_merged