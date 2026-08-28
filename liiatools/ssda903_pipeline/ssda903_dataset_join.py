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

    # Drop unnecessary columns
    episodes_merged = episodes_merged.drop(columns=["snapshot_date", "Identifier"])

    return episodes_merged


def join_placement_standard_data(placement_standard: pd.DataFrame, episodes: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data from placement standard dataframe onto 903 episodes dataframe
    Returns 903 episodes dataframe
    """
    episodes_merged = episodes.merge(
        placement_standard[[
            "child_ID",
            "when_placement_is_needed_by",
            "number_of_siblings_to_place_with",
            "preferred_location_for_home_search",
            "communication_language_learning_needs",
            "specific_communication_and_language_requirements",
            "adaptation_to_the_home",
            "cultural_needs",
            "who_can_the_child_be_cared_for_alongside",
            "can_child_live_with_pets",
            "additional_support",
            "mental_health_diagnosis",
            "open_to_CAMHS",
            "dol",
            "needs_assesment",
            "foster_care_suitability",
            "residential_care_suitability",
            "supported_accommodation",
            "risk_to_child_self_harm",
            "risk_to_child_criminal_exploitation",
            "risk_to_child_drug_and_alcohol_use",
            "risk_to_child_eating_disorder",
            "risk_to_child_going_missing",
            "risk_to_others_physical_harm",
            "risk_to_others_sexual_harm",
            "risk_to_others_fire_setting",
            "risk_to_others_harm_to_animals",
            "risk_to_others_criminal_exploitation",
            "placement_search_foster_total_number",
            "placement_search_foster_U4",
            "placement_search_foster_U5",
            "placement_search_foster_U6",
            "placement_search_supported_accommodation_total_number",
            "placement_search_supported_accommodation_solo",
            "placement_search_supported_accommodation_shared_lac",
            "placement_search_supported_accommodation_shared_other",
            "placement_search_supported_accommodation_shared_lodgings",
            "placement_search_residential_total_number",
            "placement_search_residential_ebd",
            "placement_search_residential_mhd",
            "placement_search_residential_si",
            "placement_search_residential_alc",
            "placement_search_residential_drug",
            "placement_search_residential_ld",
            "placement_search_residential_pd",
            "placement_type_offers",
            "placement_sourced",
            "preferability_of_placement_location",
            "placement_location_non_preferable_reason",
            "education_continuity",
            "how_many_siblings_were_placed_together",
            ]],
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