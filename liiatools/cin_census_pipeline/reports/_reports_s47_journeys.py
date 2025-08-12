from datetime import date

import numpy as np
import pandas as pd

from liiatools.cin_census_pipeline.reports import _time_between_date_series
from liiatools.cin_census_pipeline.spec import load_reports


def s47_journeys(data: pd.DataFrame) -> pd.DataFrame:
    """
    Creates an output that can generate a Sankey diagram of outcomes from S47 events

    :param data: The data to calculate S47 event outcomes.
    :return: The data with S47 outcomes attached.
    """
    reports_config = load_reports()

    s47_dates = (
        data[data["S47ActualStartDate"].notna()]
        .drop_duplicates(subset=["LAchildID", "S47ActualStartDate", "LA"])
        .dropna(axis=1, how="all")
    )

    cpp_dates = (
        data[data["CPPstartDate"].notna()][["LAchildID", "CPPstartDate", "LA"]]
        .drop_duplicates()
        .dropna(axis=1, how="all")
    )

    merged = s47_dates.merge(cpp_dates, how="left", on=["LAchildID", "LA"])

    merged["icpc_to_cpp"] = _time_between_date_series(
        merged["CPPstartDate"], merged["DateOfInitialCPC"], days=True
    )

    merged["s47_to_cpp"] = _time_between_date_series(
        merged["CPPstartDate"], merged["S47ActualStartDate"], days=True
    )

    # Only keep logically consistent events (as defined in config variables)
    merged = merged[
        (
            (merged["icpc_to_cpp"] >= 0)
            & (merged["icpc_to_cpp"] <= reports_config["icpc_cpp_days"])
        )
        | (
            (merged["s47_to_cpp"] >= 0)
            & (merged["s47_to_cpp"] <= reports_config["s47_cpp_days"])
        )
    ]

    # Merge events back to S47 view
    s47_outcomes = s47_dates.merge(
        merged[["Date", "LAchildID", "CPPstartDate", "icpc_to_cpp", "s47_to_cpp"]],
        how="left",
        on=["Date", "LAchildID"],
    )

    # Dates used to define window for S47 events where outcome may not be known because CIN Census is too recent
    for y in s47_outcomes["Year"]:
        s47_outcomes["cin_census_close"] = date(int(y), 3, 31)

    s47_outcomes["s47_max_date"] = s47_outcomes["cin_census_close"] - pd.Timedelta(
        reports_config["s47_day_limit"]
    )
    s47_outcomes["icpc_max_date"] = s47_outcomes["cin_census_close"] - pd.Timedelta(
        reports_config["icpc_day_limit"]
    )

    s47_outcomes["Source"] = "S47 strategy discussion"

    icpc = s47_outcomes["DateOfInitialCPC"].notna()

    cpp_start = (
        s47_outcomes["DateOfInitialCPC"].isna() & s47_outcomes["CPPstartDate"].notna()
    )

    tbd = (
        pd.to_datetime(s47_outcomes["S47ActualStartDate"], dayfirst=True)
        >= s47_outcomes["s47_max_date"]
    )

    s47_outcomes["Destination"] = np.select(
        [icpc, cpp_start, tbd],
        ["ICPC", "CPP Start", "TBD - S47 too recent"],
        default="No ICPC or CPP",
    )

    icpc_destination = s47_outcomes[s47_outcomes["Destination"] == "ICPC"]
    icpc_destination["Source"] = "ICPC"

    cpp_start_2 = icpc_destination["CPPstartDate"].notna()

    tbd_2 = (
        pd.to_datetime(icpc_destination["DateOfInitialCPC"], dayfirst=True)
        >= icpc_destination["icpc_max_date"]
    )

    icpc_destination["Destination"] = np.select(
        [cpp_start_2, tbd_2],
        ["CPP Start", "TBD - ICPC too recent"],
        default="No CPP",
    )

    s47_journey = pd.concat([s47_outcomes, icpc_destination])

    s47_journey["Age at S47"] = _time_between_date_series(
        s47_journey["S47ActualStartDate"], s47_journey["PersonBirthDate"], years=True
    )

    return s47_journey
