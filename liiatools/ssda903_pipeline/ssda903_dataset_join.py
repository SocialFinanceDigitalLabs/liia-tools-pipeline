import numpy as np
import pandas as pd
from dagster import get_dagster_logger

from liiatools.pnw_census_pipeline.pnw_dataset_join import (
    _filter_to_open_on_snapshot_date,
)

log = get_dagster_logger(__name__)


def _normalise_column_name(name: str) -> str:
    """
    Normalises a column name for comparison, ignoring case, spaces and underscores
    """
    return name.lower().replace(" ", "").replace("_", "")


def _find_column(df: pd.DataFrame, name: str) -> str | None:
    """
    Finds the column in `df` matching `name`, tolerating different case/spacing
    (e.g. Year/YEAR, row_number/Row Number). Returns None if no such column exists.
    """
    target = _normalise_column_name(name)
    for col in df.columns:
        if _normalise_column_name(col) == target:
            return col
    return None


<<<<<<< Updated upstream
def _describe_matching_criteria(source_keys: list[str], target_keys: list[str], source_dataset_name: str, target_dataset_name: str) -> str:
=======
def _describe_matching_criteria(source_keys: list[str], target_keys: list[str]) -> str:
>>>>>>> Stashed changes
    """
    Describes which column(s) were used to match, e.g. "CHILD + DECOM" or "child_ID = CHILD"
    """
    parts = [
<<<<<<< Updated upstream
        f"{source_dataset_name} {source_col} = {target_dataset_name} {target_col}"
        for source_col, target_col in zip(source_keys, target_keys)
    ]
    return ", ".join(parts)
=======
        source_col if source_col == target_col else f"{source_col} = {target_col}"
        for source_col, target_col in zip(source_keys, target_keys)
    ]
    return " + ".join(parts)
>>>>>>> Stashed changes


def _get_unmatched_rows(
    source: pd.DataFrame,
    target: pd.DataFrame,
    source_keys: str | list[str],
    target_keys: str | list[str],
<<<<<<< Updated upstream
    source_dataset_name: str,
    target_dataset_name: str,
=======
    dataset_name: str,
>>>>>>> Stashed changes
) -> pd.DataFrame:
    """
    Identifies rows in `source` whose join key(s) have no match in `target`
    Returns Row Number/Year/Month/LA for those rows, tagged with dataset_name and the columns used to match.
    Keys can be a single column name or a list of columns for composite joins (e.g. CHILD + DECOM, CHILD + CIN).
    Info columns are matched ignoring case/spacing (e.g. Year/YEAR) and left blank if `source` doesn't have them.
    """
    source_keys = [source_keys] if isinstance(source_keys, str) else list(source_keys)
    target_keys = [target_keys] if isinstance(target_keys, str) else list(target_keys)

<<<<<<< Updated upstream
    unmatched = source.merge(
        target[target_keys],
        left_on=source_keys,
        right_on=target_keys,
        how="left_anti",
        indicator=True,
    )

    info_columns = ["Row Number", "Year", "Month", "LA"]
    result = pd.DataFrame()
    for col in info_columns:
        actual_col = _find_column(unmatched, col)
        result[col] = unmatched[actual_col] if actual_col else pd.NA

    result["Dataset"] = source_dataset_name
    result["Matching Criteria"] = _describe_matching_criteria(source_keys, target_keys, source_dataset_name, target_dataset_name)

    return result[["Row Number", "Dataset", "Year", "Month", "LA", "Matching Criteria"]]
=======
    source = source.reset_index(drop=True)
    match_status = source.merge(
        target[target_keys].drop_duplicates(),
        left_on=source_keys,
        right_on=target_keys,
        how="left",
        indicator=True,
    )["_merge"]
    unmatched = source[match_status.values == "left_only"]

    info_columns = {"Row Number": "row_number", "Year": "Year", "Month": "Month", "LA": "LA"}
    result = pd.DataFrame(index=unmatched.index)
    for canonical, alias in info_columns.items():
        actual_col = _find_column(unmatched, alias)
        result[canonical] = unmatched[actual_col] if actual_col else pd.NA

    result["Dataset"] = dataset_name
    result["Matching Criteria"] = _describe_matching_criteria(source_keys, target_keys)

    return result.reset_index(drop=True)
>>>>>>> Stashed changes


def join_header_data(header: pd.DataFrame, episodes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from 903 header dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe and a dataframe of unmatched header rows
    """
    unmatched_header = _get_unmatched_rows(
<<<<<<< Updated upstream
        header, episodes, "CHILD", "CHILD", "header", "episodes"
    )
=======
        header, episodes, "CHILD", "CHILD", "header"
    )
    if len(unmatched_header):
        log.warning(
            f"{len(unmatched_header)} rows in 903 header file did not match any 903 episodes record"
        )
>>>>>>> Stashed changes

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

    return episodes_merged, unmatched_header


def join_uasc_data(uasc: pd.DataFrame, episodes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from 903 UASC dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe and a dataframe of unmatched uasc rows
    """
<<<<<<< Updated upstream
    unmatched_uasc = _get_unmatched_rows(uasc, episodes, "CHILD", "CHILD", "uasc", "episodes")
=======
    unmatched_uasc = _get_unmatched_rows(uasc, episodes, "CHILD", "CHILD", "uasc")
    if len(unmatched_uasc):
        log.warning(
            f"{len(unmatched_uasc)} rows in 903 uasc file did not match any 903 episodes record"
        )
>>>>>>> Stashed changes

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

<<<<<<< Updated upstream
=======
    #TODO check if we need to filter DUC for regarding episodes
    # # Create new column for when snapshot date is less than or equal to DUC
    # episodes_merged["UASC 903"] = np.where(
    #     episodes_merged["snapshot_date"] <= episodes_merged["DUC"], 1, 0
    # )

    # episodes_merged = episodes_merged.drop(columns=["CHILD", "DUC"])

>>>>>>> Stashed changes
    return episodes_merged, unmatched_uasc


def join_latest_episodes_data(episodes: pd.DataFrame, header: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from the latest 903 episodes for each child onto the 903 header dataframe
    Returns 903 header dataframe and a dataframe of unmatched episodes rows
    """
    episodes["DECOM"] = pd.to_datetime(episodes["DECOM"])

    # Keep only the latest episode for each child
    latest_episodes = (
        episodes.sort_values(["CHILD", "DECOM"], ascending=[True, False])
        .drop_duplicates(subset="CHILD", keep="first")
    )

<<<<<<< Updated upstream
    unmatched_episodes = _get_unmatched_rows(latest_episodes, header, "CHILD", "CHILD", "episodes", "header")
=======
    unmatched_episodes = _get_unmatched_rows(latest_episodes, header, "CHILD", "CHILD", "episodes")
    if len(unmatched_episodes):
        log.warning(
            f"{len(unmatched_episodes)} rows in 903 episodes file did not match any 903 header record"
        )
>>>>>>> Stashed changes

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

    return header_merged, unmatched_episodes


def join_latest_uasc_data(uasc: pd.DataFrame, header: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from the latest 903 UASC dataframe onto 903 header dataframe
    Returns 903 header dataframe and a dataframe of unmatched uasc rows
    """
<<<<<<< Updated upstream
    unmatched_uasc = _get_unmatched_rows(uasc, header, "CHILD", "CHILD", "uasc", "header")
=======
    unmatched_uasc = _get_unmatched_rows(uasc, header, "CHILD", "CHILD", "uasc")
    if len(unmatched_uasc):
        log.warning(
            f"{len(unmatched_uasc)} rows in 903 uasc file did not match any 903 header record"
        )
>>>>>>> Stashed changes

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

<<<<<<< Updated upstream
=======
    #TODO check if we need to filter DUC for regarding header
    # # Create new column for when snapshot date is less than or equal to DUC
    # header_merged["UASC 903"] = np.where(
    #     header_merged["snapshot_date"] <= header_merged["DUC"], 1, 0
    # )

    # header_merged = header_merged.drop(columns=["CHILD", "DUC"])

>>>>>>> Stashed changes
    return header_merged, unmatched_uasc


def join_latest_oc2_data(oc2: pd.DataFrame, header: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from the latest 903 oc2 dataframe onto 903 header dataframe
    Returns 903 header dataframe and a dataframe of unmatched oc2 rows
    """
<<<<<<< Updated upstream
    unmatched_oc2 = _get_unmatched_rows(oc2, header, "CHILD", "CHILD", "oc2", "header")
=======
    unmatched_oc2 = _get_unmatched_rows(oc2, header, "CHILD", "CHILD", "oc2")
    if len(unmatched_oc2):
        log.warning(
            f"{len(unmatched_oc2)} rows in 903 oc2 file did not match any 903 header record"
        )
>>>>>>> Stashed changes

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

    return header_merged, unmatched_oc2


def join_pnw_data(pnw_census: pd.DataFrame, episodes: pd.DataFrame, pnw_join_columns: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from pnw census dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe and a dataframe of unmatched pnw census rows
    """
<<<<<<< Updated upstream
    unmatched_pnw = _get_unmatched_rows(pnw_census, episodes, "Identifier", "CHILD", "pnw_census", "episodes")
=======
    unmatched_pnw = _get_unmatched_rows(pnw_census, episodes, "Identifier", "CHILD", "pnw_census")
    if len(unmatched_pnw):
        log.warning(
            f"{len(unmatched_pnw)} rows in PNW Census file did not match any 903 episodes record"
        )
>>>>>>> Stashed changes

    # Join pnw_census onto episodes, keeping all children in episodes
    episodes_merged = episodes.merge(
        pnw_census[["Identifier", "snapshot_date", "row_number", "Year", "Month", "LA"] + pnw_join_columns],
        left_on="CHILD",
        right_on="Identifier",
        how="left",
    )

    # Filter to only keep episodes open on day of snapshot
    episodes_merged_filtered = _filter_to_open_on_snapshot_date(
        episodes_merged, "DEC", "snapshot_date", "DECOM"
    )

    filtered_pnw = _get_unmatched_rows(episodes_merged, episodes_merged_filtered, ["CHILD", "DECOM"], ["CHILD", "DECOM"], "episodes_merged", "episodes_merged_filtered")
    filtered_pnw["Dataset"] = "pnw_census"
    filtered_pnw["Matching Criteria"] = "episodes DECOM <= pnw_census snapshot_date <= episodes DEC"
    unmatched_pnw = pd.concat([unmatched_pnw, filtered_pnw], ignore_index=True)

    # Row number in episodes should not have changed
    try:
        assert len(episodes) == len(episodes_merged_filtered)
    except AssertionError:
        log.error(
            f"Join with PNW Census results in incorrect row count: {len(episodes_merged_filtered)-len(episodes)} additional rows."
        )

    # Log number of joins made
    joins = episodes_merged_filtered["snapshot_date"].count()
    log.info(f"{joins} joins made from PNW Census file")

    # Drop unnecessary columns
    episodes_merged_filtered = episodes_merged_filtered.drop(columns=["snapshot_date", "Identifier", "row_number", "Year", "Month", "LA"])

<<<<<<< Updated upstream
    return episodes_merged_filtered, unmatched_pnw
=======
    return episodes_merged, unmatched_pnw
>>>>>>> Stashed changes


def join_placements_standard_data(placements_standard: pd.DataFrame, episodes: pd.DataFrame, placements_standard_join_columns: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from placement standard dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe and a dataframe of unmatched placements standard rows
    """
    unmatched_placements_standard = _get_unmatched_rows(
<<<<<<< Updated upstream
        placements_standard, episodes, ["child_ID", "placement_start_date"], ["CHILD", "DECOM"], "placements_standard", "episodes"
    )
=======
        placements_standard, episodes, "child_ID", "CHILD", "placements_standard"
    )
    if len(unmatched_placements_standard):
        log.warning(
            f"{len(unmatched_placements_standard)} rows in Placements Standard file did not match any 903 episodes record"
        )
>>>>>>> Stashed changes

    episodes_merged = episodes.merge(
        placements_standard[["child_ID", "placement_start_date"] + placements_standard_join_columns],
        left_on=["CHILD", "DECOM"],
        right_on=["child_ID", "placement_start_date"],
        how="left",
    )

    # Row number in episodes should not have changed
    try:
        assert len(episodes) == len(episodes_merged)
    except AssertionError:
        log.error(
            f"Join with Placements Standard results in incorrect row count: {len(episodes_merged)-len(episodes)} additional rows."
        )

    # Log number of joins made
    joins = episodes_merged["child_ID"].count()
    log.info(f"{joins} joins made from Placements Standard file")

    # Drop unnecessary columns
    episodes_merged = episodes_merged.drop(columns=["child_ID", "placement_start_date"])

    return episodes_merged, unmatched_placements_standard


def join_latest_cans_data(
    cans: pd.DataFrame, header: pd.DataFrame, cans_columns: list
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from CANS dataframe onto 903 header dataframe
    Returns 903 header dataframe and a dataframe of unmatched cans rows
    """

    # Make sure dates are datetime
    cans["Assessment Date"] = pd.to_datetime(cans["Assessment Date"])

    # Keep only the latest CANS assessment for each child
    latest_cans = (
        cans.sort_values(["Child Unique ID", "Assessment Date"], ascending=[True, False])
        .drop_duplicates(subset="Child Unique ID", keep="first")
    )

    unmatched_cans = _get_unmatched_rows(latest_cans, header, "Child Unique ID", "CHILD", "cans")
    if len(unmatched_cans):
        log.warning(
            f"{len(unmatched_cans)} rows in CANS file did not match any 903 header record"
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

    return header_merged, unmatched_cans


def join_latest_placements_standard_data(
    placements_standard: pd.DataFrame, header: pd.DataFrame, placements_standard_join_columns: list
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merges data from placements standard dataframe onto 903 header dataframe
    Returns 903 header dataframe and a dataframe of unmatched placements standard rows
    """
    # Make sure dates are datetime
    placements_standard["placement_start_date"] = pd.to_datetime(placements_standard["placement_start_date"])

    # Keep only the latest Placements Standard assessment for each child
    latest_placements_standard = (
        placements_standard.sort_values(["child_ID", "placement_start_date"], ascending=[True, False])
        .drop_duplicates(subset="child_ID", keep="first")
    )

    unmatched_placements_standard = _get_unmatched_rows(
<<<<<<< Updated upstream
        latest_placements_standard, header, "child_ID", "CHILD", "placements_standard", "header"
    )
=======
        latest_placements_standard, header, "child_ID", "CHILD", "placements_standard"
    )
    if len(unmatched_placements_standard):
        log.warning(
            f"{len(unmatched_placements_standard)} rows in Placements Standard file did not match any 903 header record"
        )
>>>>>>> Stashed changes

    header_merged = header.merge(
        latest_placements_standard[["child_ID"] + placements_standard_join_columns],
        left_on="CHILD",
        right_on="child_ID",
        how="left",
    )

    # Row number in header should not have changed
    try:
        assert len(header) == len(header_merged)
    except AssertionError:
        log.error(
            f"Join with Placements Standard results in incorrect row count: {len(header_merged)-len(header)} additional rows."
        )

    # Log number of joins made
    joins = header_merged["child_ID"].count()
    log.info(f"{joins} joins made from Placements Standard file")

    # Drop unnecessary columns
    header_merged = header_merged.drop(columns=["child_ID"])

    return header_merged, unmatched_placements_standard