import numpy as np
import pandas as pd

from liiatools.ssda903_pipeline.ssda903_dataset_join import (
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

    episodes_merged = join_header_data(header_df, episodes_df)

    assert len(episodes_merged) == 3
    assert episodes_merged["SEX"].tolist() == ["M", "M", "F"]


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

    episodes_merged = join_uasc_data(uasc_df, episodes_df)

    assert len(episodes_merged) == 3
    assert episodes_merged["DUC"].tolist()[:2] == ["2025-01-01", "2025-01-01"]
    assert pd.isna(episodes_merged["DUC"].iloc[2])


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

    header_merged = join_latest_episodes_data(
        episodes_df, header_df
    )

    assert len(header_merged) == 3
    assert header_merged.loc[header_merged["CHILD"] == "1", "CIN"].iloc[0] == "N2"
    assert header_merged.loc[header_merged["CHILD"] == "2", "CIN"].iloc[0] == "N3"
    assert pd.isna(header_merged.loc[header_merged["CHILD"] == "3", "CIN"].iloc[0])


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

    header_merged = join_latest_uasc_data(uasc_df, header_df)

    assert len(header_merged) == 2
    assert header_merged.loc[header_merged["CHILD"] == "1", "DUC"].iloc[0] == (
        "2025-01-01"
    )
    assert pd.isna(header_merged.loc[header_merged["CHILD"] == "2", "DUC"].iloc[0])


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

    header_merged = join_latest_oc2_data(oc2_df, header_df)

    assert len(header_merged) == 2
    assert header_merged.loc[header_merged["CHILD"] == "1", "SDQ_SCORE"].iloc[0] == 10
    assert pd.isna(
        header_merged.loc[header_merged["CHILD"] == "2", "SDQ_SCORE"].iloc[0]
    )


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
            "EPISODE_ID": ["1", "2", "3"],
            "DECOM": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
            "DEC": pd.to_datetime([None, None, "2024-11-30"]),
        }
    )

    episodes_merged = join_pnw_data(
        pnw_df, episodes_df, ["Placement type"]
    )

    assert len(episodes_merged) == 3
    assert episodes_merged["Placement type"].tolist() == ["Foster", "Resi", np.nan]


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
    episodes_df = pd.DataFrame(
        {"CHILD": ["1", "2"], "DECOM": pd.to_datetime(["2024-01-01", "2024-04-06"])}
    )

    episodes_merged = join_placements_standard_data(
        placements_standard_df, episodes_df, ["placement_type_offers"]
    )

    assert len(episodes_merged) == 2
    assert episodes_merged.loc[episodes_merged["CHILD"] == "1", "DECOM"].iloc[
        0
    ] == pd.to_datetime("2024-01-01")


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

    header_merged = join_latest_cans_data(cans_df, header_df, ["Assessment type"])

    assert len(header_merged) == 2
    assert (
        header_merged.loc[header_merged["CHILD"] == "1", "Assessment type"].iloc[0]
        == "B"
    )


def test_join_latest_placements_standard_data():
    placements_standard_df = pd.DataFrame(
        {
            "child_ID": ["1", "1", "8"],
            "placement_start_date": pd.to_datetime(
                ["2024-01-01", "2024-06-01", "2024-01-01"]
            ),
            "placement_type_offers": ["Foster", "Resi", "Solo"],
            "row_number": [2, 3, 4],
            "Year": [2024, 2024, 2024],
            "Month": [1, 6, 1],
            "LA": ["999", "999", "999"],
        }
    )
    header_df = pd.DataFrame({"CHILD": ["1", "2"]})

    header_merged = join_latest_placements_standard_data(
        placements_standard_df, header_df, ["placement_type_offers"]
    )

    assert len(header_merged) == 2
    assert (
        header_merged.loc[header_merged["CHILD"] == "1", "placement_type_offers"].iloc[
            0
        ]
        == "Resi"
    )