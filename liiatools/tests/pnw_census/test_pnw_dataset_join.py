import numpy as np
import pandas as pd

from liiatools.pnw_census_pipeline.pnw_dataset_join import (
    _filter_to_open_in_last_12m,
    _filter_to_open_on_snapshot_date,
    join_episode_data,
    join_missing_data,
    join_oc2_data,
    join_uasc_data,
)


def test_filter_to_open_in_last_12m():
    data = pd.DataFrame(
        {
            "DECOM": pd.to_datetime(
                [
                    "2025-01-01",
                    "2023-01-01",
                    "2024-01-01",
                    "2024-01-01",
                    "2024-01-01",
                    None,
                ]
            ),
            "snapshot_date": pd.to_datetime(
                [
                    "2024-12-31",
                    "2024-12-31",
                    "2024-12-31",
                    "2024-12-31",
                    "2024-12-31",
                    "2024-12-31",
                ]
            ),
            "DEC": pd.to_datetime(
                [None, "2023-12-30", "2024-12-01", "2025-01-01", None, None]
            ),
        }
    )

    data = _filter_to_open_in_last_12m(data, "DECOM", "snapshot_date", "DEC")

    try:
        assert len(data) == 3
        assert data["DECOM"].iloc[0].strftime("%Y-%m-%d") == "2024-01-01"
        assert data["DEC"].iloc[0].strftime("%Y-%m-%d") == "2024-12-01"
        assert data["DEC"].iloc[1].strftime("%Y-%m-%d") == "2025-01-01"
        assert pd.isna(data["DEC"].iloc[2])
        print("Test passed")
    except AssertionError:
        print("Test failed")


def test_filter_to_open_on_snapshot_date():
    data = pd.DataFrame(
        {
            "snapshot_date": pd.to_datetime(
                ["2024-12-31", "2024-12-31", "2024-12-31", "2024-12-31"]
            ),
            "DEC": pd.to_datetime([None, "2023-12-30", "2024-12-31", "2025-01-01"]),
        }
    )

    data = _filter_to_open_on_snapshot_date(data, "DEC", "snapshot_date")

    try:
        assert len(data) == 3
        assert pd.isna(data["DEC"].iloc[0])
        assert data["DEC"].iloc[1].strftime("%Y-%m-%d") == "2024-12-31"
        assert data["DEC"].iloc[2].strftime("%Y-%m-%d") == "2025-01-01"
        print("Test passed")
    except AssertionError:
        print("Test failed")


def test_join_episode_data():
    episodes_df = pd.DataFrame(
        {
            "CHILD": [
                "185686",
                "185686",
                "173918",
                "173918",
                "173918",
                "173918",
                "173918",
                "900323",
                "900323",
                "900323",
                "900327",
                "900327",
                "900327",
                "900327",
                "173118",
                "173118",
                "154228",
            ],
            "DECOM": pd.to_datetime(
                [
                    "2024-02-17",
                    "2024-12-11",
                    "2024-03-14",
                    "2024-07-16",
                    "2024-10-09",
                    "2024-10-19",
                    "2025-03-05",
                    "2023-02-13",
                    "2024-08-03",
                    "2025-01-30",
                    "2024-03-30",
                    "2024-04-10",
                    "2024-06-24",
                    "2024-12-27",
                    "2023-12-07",
                    "2024-06-25",
                    "2024-06-12",
                ]
            ),
            "DEC": pd.to_datetime(
                [
                    "2024-11-12",
                    None,
                    "2024-07-16",
                    "2024-10-09",
                    "2024-10-19",
                    "2025-03-05",
                    None,
                    "2024-08-03",
                    "2025-01-30",
                    None,
                    "2024-04-10",
                    "2024-06-24",
                    "2024-12-27",
                    None,
                    "2024-06-25",
                    "2024-10-10",
                    None,
                ]
            ),
            "PLACE": [
                "U4",
                "U3",
                "U5",
                "A4",
                "P4",
                "U4",
                "U4",
                "U5",
                "U5",
                "U1",
                "U4",
                "U4",
                "K2",
                "U4",
                "U4",
                "U5",
                "U5",
            ],
            "PLACE_PROVIDER": [
                "PR1",
                "PR1",
                "PR4",
                "PR1",
                "PR0",
                "PR4",
                "PR4",
                "PR1",
                "PR1",
                "PR1",
                "PR1",
                "PR1",
                "PR4",
                "PR4",
                "PR1",
                "PR4",
                "PR4",
            ],
            "HOME_POST": [
                "NE2 1",
                "NE2 1",
                "IG3 9",
                "IG3 9",
                "IG3 9",
                "IG3 9",
                "IG3 9",
                "L8 0",
                "L8 0",
                "L8 0",
                "NP11 6",
                "NP11 6",
                "NP11 6",
                "NP11 6",
                "M27 8",
                "M27 8",
                "SS0 5",
            ],
            "PL_POST": [
                "NE2 1",
                "NE2 1",
                "IG10 4",
                "IG10 4",
                "IG10 4",
                "IG10 4",
                "IG10 4",
                "L8 9",
                "L8 9",
                "L8 9",
                "NP13 2",
                "NP13 2",
                "NP13 2",
                "NP13 2",
                "M26 3",
                "M26 3",
                "SS2 1",
            ],
        }
    )
    pnw_df = pd.DataFrame(
        {
            "Identifier": ["185686", "173918", "900323", "900327", "173118"],
            "snapshot_date": pd.to_datetime(
                ["2024-12-31", "2024-12-31", "2024-12-31", "2024-12-31", "2024-12-31"]
            ),
        }
    )

    data = join_episode_data(episodes_df, pnw_df)

    try:
        assert len(data) == 5
        assert data["Home postcode"].nunique() == 4
        np.testing.assert_equal(
            data["# placements in last 12 months"].tolist(),
            [2.0, 4.0, 2.0, 4.0, np.nan],
        )
        print("Test passed")
    except AssertionError:
        print("Test failed")


def test_join_uasc_data():
    uasc_df = pd.DataFrame(
        {"CHILD": ["900327", "173118"], "DUC": ["2025-05-06", "2022-01-30"]}
    )
    pnw_df = pd.DataFrame(
        {
            "Identifier": ["185686", "900327", "173118"],
            "snapshot_date": pd.to_datetime(["2024-12-31", "2024-12-31", "2024-12-31"]),
        }
    )

    data = join_uasc_data(uasc_df, pnw_df)

    try:
        assert len(data) == 3
        assert data["UASC 903"].iloc[0] == 0
        assert data["UASC 903"].iloc[1] == 1
        assert data["UASC 903"].iloc[2] == 0
        print("Test passed")
    except AssertionError:
        print("Test failed")


def test_join_oc2_data():
    oc2_df = pd.DataFrame(
        {
            "CHILD": ["185686", "185686", "173918", "900323", "900323"],
            "YEAR": [2024, 2025, 2026, 2025, 2025],
            "CONVICTED": [0, 1, 1, None, 1],
            "SUBSTANCE_MISUSE": [1, 0, 1, 1, 0],
            "INTERVENTION_RECEIVED": [0, 1, 1, 1, 0],
            "INTERVENTION_OFFERED": [1, 0, 1, 1, 0],
        }
    )
    pnw_df = pd.DataFrame(
        {
            "Identifier": ["185686", "173918", "900323", "900327", "173118"],
            "snapshot_date": pd.to_datetime(
                ["2024-12-31", "2024-12-31", "2024-12-31", "2024-12-31", "2024-12-31"]
            ),
        }
    )

    data = join_oc2_data(oc2_df, pnw_df)

    try:
        assert len(data) == 5
        np.testing.assert_equal(
            data["Child convicted during the year"].tolist(),
            [1.0, np.nan, 1.0, np.nan, np.nan],
        )
        print("Test passed")
    except AssertionError:
        print("Test failed")


def test_join_missing_data():
    missing_df = pd.DataFrame(
        {
            "CHILD": [
                "185686",
                "185686",
                "900327",
                "900327",
                "173118",
                "173118",
                "173918",
            ],
            "MIS_START": pd.to_datetime(
                [
                    "2021-07-02",
                    "2022-04-03",
                    "2021-11-12",
                    "2024-05-27",
                    "2023-12-18",
                    "2024-08-04",
                    "2025-02-01",
                ]
            ),
            "MIS_END": pd.to_datetime(
                [
                    "2021-11-07",
                    "2023-04-12",
                    "2021-12-09",
                    "2024-06-16",
                    "2024-02-05",
                    "2024-09-01",
                    "2025-02-03",
                ]
            ),
        }
    )
    pnw_df = pd.DataFrame(
        {
            "Identifier": ["185686", "173918", "900323", "900327", "173118"],
            "snapshot_date": pd.to_datetime(
                ["2024-12-31", "2024-12-31", "2024-12-31", "2024-12-31", "2024-12-31"]
            ),
        }
    )

    data = join_missing_data(missing_df, pnw_df)

    try:
        assert len(data) == 5
        np.testing.assert_equal(
            data["# missing episodes in last 12 months"].tolist(),
            [np.nan, np.nan, np.nan, 1.0, 2.0],
        )
        print("Test passed")
    except AssertionError:
        print("Test failed")
