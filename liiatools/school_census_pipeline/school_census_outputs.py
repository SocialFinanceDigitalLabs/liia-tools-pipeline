import pandas as pd


def create_demographics_output(
    pupil: pd.DataFrame,
    identifier: str,
    addresses: pd.DataFrame,
    fsm: pd.DataFrame | None,
    sen: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Makes a demographic output using a pupil, addresses, fsm and, optionally, a sen file

    :param pupil: A dataframe corresponding to the pupilonroll table from the School Census
    :type pupil: pd.DataFrame
    :param identifier: A string corresponding to the name of the primary key to the pupil table
    :type identifier: string
    :param addresses: A dataframe corresponding to either addresses, addressesonroll or a concatenation of both tables from the School Census
    :type addresses: pd.DataFrame
    :param fsm: A dataframe corresponding to either fsmperiod, fsmperiodsonroll or a concatenation of both tables from the School Census (only for onroll)
    :type fsm: pd.DataFrame | None
    :param sen: A dataframe corresponding to the senneeds table from the School Census (only for onroll in spring)
    :type sen: pd.DataFrame | None
    :return: A dataframe for the demographics output for the School Census
    :rtype: DataFrame
    """
    # Join addresses to pupil
    pupil = pupil.copy().merge(
        right=addresses.copy(),
        on=[identifier, "NativeId", "Year", "Term", "LA", "Acad/LA"],
        how="left",
    )

    # Join fsm (if it exists) after filtering to only open periods
    if fsm is not None:
        fsm = fsm[fsm["fsmenddate"].isna()].copy()
        pupil = pupil.merge(
            right=fsm,
            on=[identifier, "NativeId", "Year", "Term", "LA", "Acad/LA"],
            how="left",
        )

    # Join sen if it exists
    if sen is not None:
        sen = sen[sen["sentyperank"].isin([1, 2])].copy()
        sen = sen[
            [
                identifier,
                "NativeId",
                "Year",
                "Term",
                "LA",
                "Acad/LA",
                "sentyperank",
                "sentype",
            ]
        ]
        sen = (
            sen.pivot(
                index=[identifier, "NativeId", "Year", "Term", "LA", "Acad/LA"],
                columns="sentyperank",
                values="sentype",
            )
            .rename(columns={1: "sentype1", 2: "sentype2"})
            .reset_index()
        )

        pupil = pupil.merge(
            right=sen,
            on=[identifier, "NativeId", "Year", "Term", "LA", "Acad/LA"],
            how="left",
        )

    return pupil


def create_sessions_output(
    pupil: pd.DataFrame,
    identifier: str,
    session_identifier: str,
    sessions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Makes a sessions output using a pupil and sessions file

    :param pupil: A dataframe corresponding to a pupil level dataset, including relevant possible sessions
    :type pupil: pd.DataFrame
    :param identifier: A string corresponding to the name of the primary key to the pupil table
    :type identifier: string
    :param sessions: A dataframe corresponding to either termlysessiondetailsoffroll, termlysessiondetailsonroll, summerhalfterm2sessiondetailsoffroll, or summerhalfterm2sessiondetailsonroll
    :type sessions: pd.DataFrame
    :return: a sessions output with pupil details and pivoted attendance values
    :rtype: DataFrame
    """

    # Pivot sessions table
    sessions = sessions.copy()
    sessions = sessions.pivot(
        index=[identifier, "NativeId", "Year", "Term", "LA", "Acad/LA"],
        columns="attendancereason",
        values="sessions",
    )
    sessions = sessions.rename(
        columns=lambda x: "attendancereason BLANK"
        if pd.isna(x) or x == ""
        else f"attendancereason {x}"
    ).reset_index()

    # Join to pupil table
    pupil = pupil.merge(
        right=sessions,
        on=[identifier, "NativeId", "Year", "Term", "LA", "Acad/LA"],
        how="left",
    )

    return pupil
