import numpy as np
import pandas as pd

from liiatools.ssda903_pipeline.ssda903_dataset_join import (
    _describe_matching_criteria,
    _find_column,
    _get_unmatched_rows,
    _normalise_column_name,
    join_header_data,
    join_latest_cans_data,
    join_latest_episodes_data,
    join_latest_oc2_data,
    join_latest_placements_standard_data,
    join_latest_uasc_data,
    join_placements_standard_data,
    join_pnw_data,
    join_uasc_data,
)


def test_normalise_column_name():
    assert _normalise_column_name("Year") == "year"
    assert _normalise_column_name("YEAR") == "year"
    assert _normalise_column_name("row_number") == "rownumber"
    assert _normalise_column_name("Row Number") == "rownumber"


def test_find_column():
    df = pd.DataFrame(columns=["YEAR", "Row Number", "LA"])

    assert _find_column(df, "Year") == "YEAR"
    assert _find_column(df, "row_number") == "Row Number"
    assert _find_column(df, "Month") is None


def test_describe_matching_criteria():
    assert _describe_matching_criteria(["CHILD"], ["CHILD"], "header", "episodes") == "header CHILD = episodes CHILD"
    assert _describe_matching_criteria(
        ["child_ID"], ["CHILD"], "placements_standard", "episodes"
    ) == "placements_standard child_ID = episodes CHILD"
    assert (
        _describe_matching_criteria(["child_ID", "placement_start_date"], ["CHILD", "DECOM"], "placements_standard", "episodes")
        == "placements_standard child_ID = episodes CHILD, placements_standard placement_start_date = episodes DECOM"
    )


def test_get_unmatched_rows_single_key():
    source = pd.DataFrame(
        {
            "CHILD": ["1", "2", "3"],
            "row_number": [2, 3, 4],
            "Year": [2024, 2024, 2024],
            "Month": ["Jan", "Jan", "Jan"],
            "LA": ["999", "999", "999"],
        }
    )
    target = pd.DataFrame({"CHILD": ["1", "2"]})

    result = _get_unmatched_rows(source, target, "CHILD", "CHILD", "test_source_dataset", "test_target_dataset")

    assert list(result.columns) == [
        "Row Number",
        "Dataset",
        "Year",
        "Month",
        "LA",
        "Matching Criteria",
    ]
    assert len(result) == 1
    assert result["Row Number"].iloc[0] == 4
    assert result["Dataset"].iloc[0] == "test_source_dataset"
    assert result["Matching Criteria"].iloc[0] == "test_source_dataset CHILD = test_target_dataset CHILD"


def test_get_unmatched_rows_composite_key():
    source = pd.DataFrame(
        {
            "CHILD": ["1", "1", "2"],
            "DECOM": pd.to_datetime(["2024-01-01", "2024-06-01", "2024-01-01"]),
            "row_number": [2, 3, 4],
            "Year": [2024, 2024, 2024],
            "Month": ["Jan", "Jun", "Jan"],
            "LA": ["999", "999", "999"],
        }
    )
    target = pd.DataFrame(
        {
            "child_ID": ["1", "2"],
            "placement_start_date": pd.to_datetime(["2024-01-01", "2024-06-01"]),
        }
    )

    result = _get_unmatched_rows(
        source, target, ["CHILD", "DECOM"], ["child_ID", "placement_start_date"], "episodes", "placements_standard"
    )

    assert len(result) == 2
    assert set(result["Row Number"]) == {3, 4}
    assert (result["Matching Criteria"] == "episodes CHILD = placements_standard child_ID, episodes DECOM = placements_standard placement_start_date").all()


def test_get_unmatched_rows_handles_aliases_and_missing_columns():
    source = pd.DataFrame(
        {
            "CHILD": ["1", "2"],
            "ROW_NUMBER": [2, 3],
            "YEAR": [2024, 2024],
            "LA": ["999", "999"],
            # No Month column present at all
        }
    )
    target = pd.DataFrame({"CHILD": ["1"]})

    result = _get_unmatched_rows(source, target, "CHILD", "CHILD", "annual_dataset", "test_target_dataset")

    assert len(result) == 1
    assert result["Row Number"].iloc[0] == 3
    assert result["Year"].iloc[0] == 2024
    assert pd.isna(result["Month"].iloc[0])


def test_join_header_data():
    header_df = pd.DataFrame(
        {
            "CHILD": ["1", "2", "3"],
            "SEX": ["M", "F", "M"],
            "ETHNIC": ["WBRI", "WBRI", "MWBC"],
            "DOB": ["2010-01-01", "2011-01-01", "2012-01-01"],
            "row_number": [2, 3, 4],
            "Year": [2024, 2024, 2024],
            "Month": ["Jan", "Jan", "Jan"],
            "LA": ["999", "999", "999"],
        }
    )
    episodes_df = pd.DataFrame(
        {
            "CHILD": ["1", "1", "2"],
            "DECOM": ["2023-01-01", "2023-06-01", "2023-01-01"],
        }
    )

    episodes_merged, unmatched_header = join_header_data(header_df, episodes_df)

    assert len(episodes_merged) == 3
    assert episodes_merged["SEX"].tolist() == ["M", "M", "F"]
    assert len(unmatched_header) == 1
    assert unmatched_header["Row Number"].iloc[0] == 4
    assert unmatched_header["Dataset"].iloc[0] == "header"
    assert unmatched_header["Matching Criteria"].iloc[0] == "header CHILD = episodes CHILD"


def test_join_uasc_data():
    uasc_df = pd.DataFrame(
        {
            "CHILD": ["1", "3"],
            "DUC": ["2025-01-01", "2025-02-01"],
            "row_number": [2, 3],
            "Year": [2024, 2024],
            "Month": ["Jan", "Jan"],
            "LA": ["999", "999"],
        }
    )
    episodes_df = pd.DataFrame({"CHILD": ["1", "1", "2"]})

    episodes_merged, unmatched_uasc = join_uasc_data(uasc_df, episodes_df)

    assert len(episodes_merged) == 3
    assert episodes_merged["DUC"].tolist()[:2] == ["2025-01-01", "2025-01-01"]
    assert pd.isna(episodes_merged["DUC"].iloc[2])
    assert len(unmatched_uasc) == 1
    assert unmatched_uasc["Row Number"].iloc[0] == 3


def test_join_latest_episodes_data():
    episodes_df = pd.DataFrame(
        {
            "CHILD": ["1", "1", "2", "4"],
            "DECOM": ["2023-01-01", "2023-06-01", "2023-01-01", "2023-01-01"],
            "CIN": ["N1", "N2", "N3", "N4"],
            "row_number": [2, 3, 4, 5],
            "Year": [2024, 2024, 2024, 2024],
            "Month": ["Jan", "Jan", "Jan", "Jan"],
            "LA": ["999", "999", "999", "999"],
        }
    )
    header_df = pd.DataFrame({"CHILD": ["1", "2", "3"]})

    header_merged, unmatched_episodes = join_latest_episodes_data(
        episodes_df, header_df
    )

    assert len(header_merged) == 3
    assert header_merged.loc[header_merged["CHILD"] == "1", "CIN"].iloc[0] == "N2"
    assert header_merged.loc[header_merged["CHILD"] == "2", "CIN"].iloc[0] == "N3"
    assert pd.isna(header_merged.loc[header_merged["CHILD"] == "3", "CIN"].iloc[0])
    assert len(unmatched_episodes) == 1
    assert unmatched_episodes["Row Number"].iloc[0] == 5


def test_join_latest_uasc_data():
    uasc_df = pd.DataFrame(
        {
            "CHILD": ["1", "4"],
            "DUC": ["2025-01-01", "2025-02-01"],
            "row_number": [2, 3],
            "Year": [2024, 2024],
            "Month": ["Jan", "Jan"],
            "LA": ["999", "999"],
        }
    )
    header_df = pd.DataFrame({"CHILD": ["1", "2"]})

    header_merged, unmatched_uasc = join_latest_uasc_data(uasc_df, header_df)

    assert len(header_merged) == 2
    assert header_merged.loc[header_merged["CHILD"] == "1", "DUC"].iloc[0] == (
        "2025-01-01"
    )
    assert pd.isna(header_merged.loc[header_merged["CHILD"] == "2", "DUC"].iloc[0])
    assert len(unmatched_uasc) == 1
    assert unmatched_uasc["Row Number"].iloc[0] == 3


def test_join_latest_oc2_data():
    oc2_df = pd.DataFrame(
        {
            "CHILD": ["1", "4"],
            "SDQ_SCORE": [10, 20],
            "row_number": [2, 3],
            "Year": [2024, 2024],
            "Month": ["Jan", "Jan"],
            "LA": ["999", "999"],
        }
    )
    header_df = pd.DataFrame({"CHILD": ["1", "2"]})

    header_merged, unmatched_oc2 = join_latest_oc2_data(oc2_df, header_df)

    assert len(header_merged) == 2
    assert header_merged.loc[header_merged["CHILD"] == "1", "SDQ_SCORE"].iloc[0] == 10
    assert pd.isna(header_merged.loc[header_merged["CHILD"] == "2", "SDQ_SCORE"].iloc[0])
    assert len(unmatched_oc2) == 1
    assert unmatched_oc2["Row Number"].iloc[0] == 3


def test_join_pnw_data():
    pnw_df = pd.DataFrame(
        {
            "Identifier": ["1", "2", "5", "6"],
            "snapshot_date": pd.to_datetime(
                ["2024-12-31", "2024-12-31", "2024-12-31", "2024-12-31"]
            ),
            "Placement type": ["Foster", "Resi", "Foster", "Foster"],
            "row_number": [2, 3, 4, 5],
            "Year": [2024, 2024, 2024, 2024],
            "Month": [12, 12, 12, 12],
            "LA": ["999", "999", "999", "999"],
        }
    )
    episodes_df = pd.DataFrame(
        {
            "CHILD": ["1", "2", "6"],
            "DECOM": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
            "DEC": pd.to_datetime([None, None, "2024-11-30"]),
        }
    )

    episodes_merged, unmatched_pnw = join_pnw_data(
        pnw_df, episodes_df, ["Placement type"]
    )

    assert len(episodes_merged) == 2
    assert episodes_merged["Placement type"].tolist() == ["Foster", "Resi"]
    assert len(unmatched_pnw) == 1
    assert unmatched_pnw["Row Number"].iloc[0] == 4
    assert unmatched_pnw["Dataset"].iloc[0] == "pnw_census"
    assert unmatched_pnw["Matching Criteria"].iloc[0] == "pnw_census Identifier = episodes CHILD"


def test_join_placements_standard_data():
    placements_standard_df = pd.DataFrame(
        {
            "child_ID": ["1", "6"],
            "placement_start_date": pd.to_datetime(["2024-01-01", "2024-04-06"]),
            "placement_type_offers": ["Foster", "Resi"],
            "row_number": [2, 3],
            "Year": [2024, 2024],
            "Month": [1, 1],
            "LA": ["999", "999"],
        }
    )
    episodes_df = pd.DataFrame({"CHILD": ["1", "2"], "DECOM": pd.to_datetime(["2024-01-01", "2024-04-06"])})

    episodes_merged, unmatched_placements_standard = join_placements_standard_data(
        placements_standard_df, episodes_df, ["placement_type_offers"]
    )

    assert len(episodes_merged) == 2
    assert (
        episodes_merged.loc[episodes_merged["CHILD"] == "1", "DECOM"].iloc[0]
        == pd.to_datetime("2024-01-01")
    )
    assert len(unmatched_placements_standard) == 1
    assert unmatched_placements_standard["Row Number"].iloc[0] == 3
    assert unmatched_placements_standard["Matching Criteria"].iloc[0] == "placements_standard child_ID = episodes CHILD, placements_standard placement_start_date = episodes DECOM"


def test_join_latest_cans_data():
    cans_df = pd.DataFrame(
        {
            "Child Unique ID": ["1", "1", "7"],
            "Assessment Date": ["2024-01-01", "2024-06-01", "2024-01-01"],
            "Assessment type": ["A", "B", "C"],
            "row_number": [2, 3, 4],
            "Year": [2024, 2024, 2024],
            "Month": [1, 6, 1],
            "LA": ["999", "999", "999"],
        }
    )
    header_df = pd.DataFrame({"CHILD": ["1", "2"]})

    header_merged = join_latest_cans_data(
        cans_df, header_df, ["Assessment type"]
    )

    assert len(header_merged) == 2
    assert (
        header_merged.loc[header_merged["CHILD"] == "1", "Assessment type"].iloc[0]
        == "B"
    )


def test_join_latest_placements_standard_data():
    placements_standard_df = pd.DataFrame(
        {
            "child_ID": ["1", "1", "8"],
            "placement_start_date": pd.to_datetime(["2024-01-01", "2024-06-01", "2024-01-01"]),
            "placement_type_offers": ["Foster", "Resi", "Solo"],
            "row_number": [2, 3, 4],
            "Year": [2024, 2024, 2024],
            "Month": [1, 6, 1],
            "LA": ["999", "999", "999"],
        }
    )
    header_df = pd.DataFrame({"CHILD": ["1", "2"]})

    header_merged, unmatched_placements_standard = join_latest_placements_standard_data(
        placements_standard_df, header_df, ["placement_type_offers"]
    )

    assert len(header_merged) == 2
    assert (
        header_merged.loc[header_merged["CHILD"] == "1", "placement_type_offers"].iloc[0]
        == "Resi"
    )
    assert len(unmatched_placements_standard) == 1
    assert unmatched_placements_standard["Row Number"].iloc[0] == 4
