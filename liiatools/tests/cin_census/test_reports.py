import unittest
from datetime import date

import numpy as np
import pandas as pd

from liiatools.cin_census_pipeline.reports.reports import (
    _filter_events,
    _time_between_date_series,
    expanded_assessment_factors,
    referral_outcomes,
    s47_journeys,
)


def test_assessment_factors():
    df = pd.DataFrame(
        [
            ["CHILD1", "A,B,C", "AssessmentAuthorisationDate", date(2022, 1, 1), "TT1"],
            ["CHILD1", None, "AssessmentAuthorisationDate", date(2022, 2, 12), "TT1"],
            ["CHILD1", "", "AssessmentAuthorisationDate", date(2022, 3, 15), "TT1"],
            ["CHILD2", "A", "AssessmentAuthorisationDate", date(2022, 1, 1), "TT1"],
            ["CHILD3", "D,A,D", "AssessmentAuthorisationDate", date(2022, 1, 1), "TT1"],
        ],
        columns=["LAchildID", "Factors", "Type", "AssessmentAuthorisationDate", "LA"],
    )

    df = expanded_assessment_factors(df)

    assert df.A.tolist() == [1, 0, 0, 1, 1]
    assert df.B.tolist() == [1, 0, 0, 0, 0]
    assert df.C.tolist() == [1, 0, 0, 0, 0]
    assert df.D.tolist() == [0, 0, 0, 0, 1]


def test_time_between_date_series():
    test_df_1 = pd.DataFrame(
        [
            [date(2022, 1, 1), date(2021, 1, 1)],
            [date(2022, 1, 1), date(2020, 1, 1)],
        ],
        columns=["date_series_1", "date_series_2"],
    )

    output_series_1 = _time_between_date_series(
        test_df_1["date_series_1"], test_df_1["date_series_2"], years=True
    )
    assert list(output_series_1) == [1, 2]

    output_series_2 = _time_between_date_series(
        test_df_1["date_series_1"], test_df_1["date_series_2"], days=True
    )
    assert list(output_series_2) == [365, 731]


def test_filter_events():
    test_df_1 = pd.DataFrame(
        [
            1,
            -1,
            30,
        ],
        columns=["day_series"],
    )

    output_1 = _filter_events(test_df_1, "day_series", 25)
    output_2 = _filter_events(test_df_1, "day_series", 30)
    assert output_1.shape == (1, 1)
    assert output_2.shape == (2, 1)


def test_referral_outcomes():
    df = pd.DataFrame(
        [
            [
                "CHILD1",
                "AssessmentActualStartDate",
                date(1965, 6, 15),
                date(1970, 10, 6),
                date(1970, 6, 3),
                pd.NA,
                "TT1",
            ],
            [
                "CHILD1",
                "S47ActualStartDate",
                date(1965, 6, 15),
                date(1970, 10, 6),
                pd.NA,
                date(1970, 6, 2),
                "TT1",
            ],
            [
                "CHILD1",
                "CINreferralDate",
                date(1965, 6, 15),
                date(1970, 10, 6),
                pd.NA,
                pd.NA,
                "TT1",
            ],
            [
                "CHILD2",
                "AssessmentActualStartDate",
                date(1992, 1, 2),
                date(2001, 11, 7),
                date(2001, 10, 25),
                pd.NA,
                "TT1",
            ],
            [
                "CHILD2",
                "S47ActualStartDate",
                date(1992, 1, 2),
                date(2001, 11, 7),
                pd.NA,
                date(2001, 10, 20),
                "TT1",
            ],
            [
                "CHILD2",
                "CINreferralDate",
                date(1992, 1, 2),
                date(2001, 11, 7),
                pd.NA,
                pd.NA,
                "TT1",
            ],
            [
                "CHILD3",
                "AssessmentActualStartDate",
                date(1995, 7, 21),
                date(2003, 9, 5),
                date(2003, 8, 28),
                pd.NA,
                "TT1",
            ],
            [
                "CHILD3",
                "S47ActualStartDate",
                date(1995, 7, 21),
                date(2003, 9, 5),
                pd.NA,
                date(2003, 8, 26),
                "TT1",
            ],
            [
                "CHILD3",
                "CINreferralDate",
                date(1995, 7, 21),
                date(2003, 9, 5),
                pd.NA,
                pd.NA,
                "TT1",
            ],
        ],
        columns=[
            "LAchildID",
            "Type",
            "PersonBirthDate",
            "CINreferralDate",
            "AssessmentActualStartDate",
            "S47ActualStartDate",
            "LA"
        ],
    )

    df = referral_outcomes(df)

    assert list(df["AssessmentActualStartDate"]) == [
        np.nan,
        date(2001, 10, 25),
        date(2003, 8, 28),
    ]
    assert list(df["days_to_s17"]) == [pd.NA, 13, 8]
    assert list(df["S47ActualStartDate"]) == [
        np.nan,
        date(2001, 10, 20),
        date(2003, 8, 26),
    ]
    assert list(df["days_to_s47"]) == [pd.NA, 18, 10]
    assert list(df["referral_outcome"]) == ["NFA", "BOTH", "BOTH"]
    assert list(df["Age at referral"]) == [5, 9, 8]


def test_s47_journeys():
    df = pd.DataFrame(
        [
            [
                "CHILD1",
                "CPPstartDate",
                date(1970, 5, 25),
                date(1965, 6, 15),
                date(1970, 10, 6),
                pd.NA,
                pd.NA,
                date(1970, 5, 25),
                2022,
                "TT1",
            ],
            [
                "CHILD1",
                "S47ActualStartDate",
                date(1970, 3, 15),
                date(1965, 6, 15),
                date(1970, 10, 6),
                date(1970, 5, 12),
                date(1970, 3, 15),
                pd.NA,
                2022,
                "TT1",
            ],
            [
                "CHILD2",
                "S47ActualStartDate",
                date(2001, 10, 12),
                date(1992, 1, 2),
                date(2001, 11, 7),
                pd.NA,
                date(2001, 10, 12),
                pd.NA,
                2022,
                "TT1",
            ],
            [
                "CHILD2",
                "CPPstartDate",
                date(2001, 10, 31),
                date(1992, 1, 2),
                date(2001, 11, 7),
                pd.NA,
                pd.NA,
                date(2001, 10, 31),
                2022,
                "TT1",
            ],
            [
                "CHILD3",
                "S47ActualStartDate",
                date(2022, 1, 31),
                date(2015, 7, 21),
                date(2022, 9, 5),
                pd.NA,
                date(2022, 1, 31),
                pd.NA,
                2022,
                "TT1",
            ],
            [
                "CHILD4",
                "S47ActualStartDate",
                date(2006, 8, 16),
                date(1997, 7, 21),
                date(2006, 9, 5),
                pd.NA,
                date(2006, 8, 16),
                pd.NA,
                2022,
                "TT1",
            ],
            [
                "CHILD5",
                "S47ActualStartDate",
                date(2001, 7, 22),
                date(1993, 4, 22),
                date(2001, 9, 2),
                date(2001, 7, 15),
                date(2001, 7, 22),
                pd.NA,
                2022,
                "TT1",
            ],
            [
                "CHILD6",
                "S47ActualStartDate",
                date(2022, 2, 22),
                date(2011, 4, 22),
                date(2021, 9, 2),
                date(2022, 2, 15),
                date(2022, 2, 22),
                pd.NA,
                2022,
                "TT1",
            ],
        ],
        columns=[
            "LAchildID",
            "Type",
            "Date",
            "PersonBirthDate",
            "CINreferralDate",
            "DateOfInitialCPC",
            "S47ActualStartDate",
            "CPPstartDate",
            "Year",
            "LA",
        ],
    )

    df = s47_journeys(df)

    assert list(df["icpc_to_cpp"]) == [
        13,
        pd.NA,
        pd.NA,
        pd.NA,
        pd.NA,
        pd.NA,
        13,
        pd.NA,
        pd.NA,
    ]
    assert list(df["s47_to_cpp"]) == [
        71,
        19,
        pd.NA,
        pd.NA,
        pd.NA,
        pd.NA,
        71,
        pd.NA,
        pd.NA,
    ]
    assert list(df["cin_census_close"]) == [
        date(2022, 3, 31),
        date(2022, 3, 31),
        date(2022, 3, 31),
        date(2022, 3, 31),
        date(2022, 3, 31),
        date(2022, 3, 31),
        date(2022, 3, 31),
        date(2022, 3, 31),
        date(2022, 3, 31),
    ]
    assert list(df["s47_max_date"]) == [
        date(2022, 1, 30),
        date(2022, 1, 30),
        date(2022, 1, 30),
        date(2022, 1, 30),
        date(2022, 1, 30),
        date(2022, 1, 30),
        date(2022, 1, 30),
        date(2022, 1, 30),
        date(2022, 1, 30),
    ]
    assert list(df["icpc_max_date"]) == [
        date(2022, 2, 14),
        date(2022, 2, 14),
        date(2022, 2, 14),
        date(2022, 2, 14),
        date(2022, 2, 14),
        date(2022, 2, 14),
        date(2022, 2, 14),
        date(2022, 2, 14),
        date(2022, 2, 14),
    ]
    assert list(df["Source"]) == [
        "S47 strategy discussion",
        "S47 strategy discussion",
        "S47 strategy discussion",
        "S47 strategy discussion",
        "S47 strategy discussion",
        "S47 strategy discussion",
        "ICPC",
        "ICPC",
        "ICPC",
    ]
    assert list(df["Destination"]) == [
        "ICPC",
        "CPP Start",
        "TBD - S47 too recent",
        "No ICPC or CPP",
        "ICPC",
        "ICPC",
        "CPP Start",
        "No CPP",
        "TBD - ICPC too recent",
    ]
    assert list(df["Age at S47"]) == [4, 9, 6, 9, 8, 10, 4, 8, 10]
