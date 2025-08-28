import numpy as np
import pandas as pd

from liiatools.cin_census_pipeline.reports import (
    _filter_events,
    _time_between_date_series,
)
from liiatools.cin_census_pipeline.spec import load_reports


def referral_outcomes(data: pd.DataFrame) -> pd.DataFrame:
    """
    Add referral outcomes to the data based on assessment and S47 dates. These can be;
    NFA, S17, S47 or BOTH

    :param data: The data calculate referral outcomes.
    :returns: The data with referral outcomes attached.
    """
    reports_config = load_reports()

    s17_dates = (
        data[data["Type"] == "AssessmentActualStartDate"][["LAchildID", "CINreferralDate", "AssessmentActualStartDate", "LA"]]
        .drop_duplicates()
        .dropna(axis=1, how="all")
    )

    s17_dates["days_to_s17"] = _time_between_date_series(
        s17_dates["AssessmentActualStartDate"], s17_dates["CINreferralDate"], days=True
    )

    # Only assessments within config-specified period following referral are valid
    s17_dates = _filter_events(
        s17_dates, "days_to_s17", max_days=reports_config["ref_assessment"]
    )

    s47_dates = (
        data[data["Type"] == "S47ActualStartDate"][["LAchildID", "CINreferralDate", "S47ActualStartDate", "LA"]]
        .drop_duplicates()
        .dropna(axis=1, how="all")
    )

    s47_dates["days_to_s47"] = _time_between_date_series(
        s47_dates["S47ActualStartDate"], s47_dates["CINreferralDate"], days=True
    )

    # Only S47s within config-specified period following referral are valid
    s47_dates = _filter_events(
        s47_dates, "days_to_s47", max_days=reports_config["ref_assessment"]
    )

    referral = (
        data[data["Type"] == "CINreferralDate"]
        .drop_duplicates()
        .dropna(axis=1, how="all")
    )

    merged = referral.merge(s17_dates, how="left", on=["LAchildID", "CINreferralDate", "LA"])
    merged = merged.merge(s47_dates, how="left", on=["LAchildID", "CINreferralDate", "LA"])

    neither = (
        merged["AssessmentActualStartDate"].isna() & merged["S47ActualStartDate"].isna()
    )
    s17_set = (
        merged["AssessmentActualStartDate"].notna()
        & merged["S47ActualStartDate"].isna()
    )
    s47_set = (
        merged["AssessmentActualStartDate"].isna()
        & merged["S47ActualStartDate"].notna()
    )
    both_set = (
        merged["AssessmentActualStartDate"].notna()
        & merged["S47ActualStartDate"].notna()
    )

    merged["referral_outcome"] = np.select(
        [neither, s17_set, s47_set, both_set],
        ["NFA", "S17", "S47", "BOTH"],
        default=None,
    )

    merged["Age at referral"] = _time_between_date_series(
        merged["CINreferralDate"], merged["PersonBirthDate"], years=True
    )

    return merged
