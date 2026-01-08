import re
from typing import List

import pandas as pd
from dagster import In, Out, get_dagster_logger, op
from fs import errors
from fs.base import FS

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesSCCross, SessionNamesSCRegion
from liiatools.common.data import DataContainer
from liiatools.common.pipeline import open_file
from liiatools.school_census_pipeline.school_census_outputs import (
    create_demographics_output,
    create_sessions_output,
)
from liiatools_pipeline.assets.common import shared_folder, workspace_folder
from liiatools_pipeline.assets.external_dataset import external_data_folder

log = get_dagster_logger(__name__)


def open_and_concat_patterns(
    files: List[str], patterns: List[re.Pattern[str]], session_folder: FS
) -> pd.DataFrame:
    """
    Function to receive a list of one or more files. If one, the function opens the file and returns this. If multiple, the function opens each and returns the concatenated output

    :param files: List of filenames as strings
    :type files: List[str]
    :param patterns: Regex pattern/s to find relevant files
    :type patterns: List[re.Pattern[str]]
    :param session_folder: session folder with all files in
    :type session_folder: FS
    :return: a single dataframe for the relevant file/s
    :rtype: DataFrame
    """
    matches = [f for f in files if any(pattern.search(f) for pattern in patterns)]
    if len(matches) == 1:
        filename = matches[0]
        return open_file(session_folder, filename)
    elif len(matches) > 1:
        dfs = [open_file(session_folder, f) for f in matches]
        return pd.concat(dfs, ignore_index=True)

    raise ValueError("No files matched the patterns provided")


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_cross_session_folder() -> FS:
    log.info("Creating session folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesSCCross
    )

    log.info("Opening incoming folder...")
    session_folder = session_folder.opendir(SessionNamesSCCross.INCOMING_FOLDER)

    sc_reports_folder = workspace_folder().opendir("current/school_census/CROSS")
    pl.move_files_for_sharing(sc_reports_folder, session_folder)

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def school_census_cross_outputs(
    session_folder: FS,
):
    log.info("Removing previous CROSS outputs from shared folder...")

    output_folder = shared_folder()
    existing_files = output_folder.listdir("/")
    cross_files_regex = f"^CROSS_"
    pl.remove_files(cross_files_regex, existing_files, output_folder)

    log.info("Checking necessary files are present...")

    address_pattern = re.compile(r"addresses")
    fsm_pattern = re.compile(r"fsm")
    pupil_pattern = re.compile(r"pupil")
    exclusions_pattern = re.compile(r"exclusions")
    session_pattern = re.compile(r"termlysession")

    files = session_folder.listdir("/")

    # Check if required files are present
    school_patterns = [
        address_pattern,
        fsm_pattern,
        pupil_pattern,
        exclusions_pattern,
        session_pattern,
    ]

    files_present = all(
        any(pattern.search(f) for f in files) for pattern in school_patterns
    )
    if not files_present:
        log.error(
            "Not all of the required outputs are present to create the CROSS outputs"
        )
        return
    log.info(
        "All CROSS school census input files present in session folder; proceeding to data transformation"
    )

    # Check for GIAS lookup file
    ext_folder = external_data_folder()
    try:
        GIAS_lookup = open_file(ext_folder, "gias_lad25cd_lookup.csv")
    except errors.ResourceNotFound as err:
        log.error(f"No GIAS lookup file to open: {err}")
        log.info("Exiting run as external dataset resources not available")
        return

    # Create empty dict to store outputs
    outputs = {}

    # Make demographics output
    pupil = open_file(session_folder, "school_census_pupilonroll.csv")

    # Open addresses files, where there may be one or two files to open
    addresses_pattern = re.compile(r"school_census_addresses\.csv$")
    addresses_onroll_pattern = re.compile(r"school_census_addressesonroll\.csv$")
    addresses_patterns = [addresses_pattern, addresses_onroll_pattern]
    addresses = open_and_concat_patterns(files, addresses_patterns, session_folder)
    addresses = addresses.drop(
        columns=["addressesorderseqcolumn", "addressesonrollorderseqcolumn"],
        errors="ignore",
    )

    # Join GIAS code to addresses
    addresses = addresses.merge(
        right=GIAS_lookup[["GIAS code", "lad25cd"]],
        left_on="child_home_la",
        right_on="lad25cd",
    )
    addresses = addresses.rename(columns={"GIAS code": "child_home_GIAS"})

    # Open fsm files, where there may be one or two files to open
    fsm_pattern = re.compile(r"school_census_fsmperiod\.csv$")
    fsm_onroll_pattern = re.compile(r"school_census_fsmperiodsonroll\.csv$")
    fsm_patterns = [fsm_pattern, fsm_onroll_pattern]
    fsm = open_and_concat_patterns(files, fsm_patterns, session_folder)

    # Open sen file if exists (only in spring)
    sen = None
    if "school_census_senneeds.csv" in files:
        sen = open_file(session_folder, "school_census_senneeds.csv")

    # Create demographics output
    demographics = create_demographics_output(
        pupil, "pupilonrolltableid", addresses, fsm, sen
    )
    outputs["children"] = demographics

    # Create sessions outputs
    # Filter pupil down to necessary columns
    pupil_termly_sessions = demographics[
        [
            "pupilonrolltableid",
            "NativeId",
            "child_home_GIAS",
            "termlysessionspossible",
            "LA",
            "Year",
            "Term",
            "Acad/LA",
        ]
    ].copy()
    termly_sessions = open_file(
        session_folder, "school_census_termlysessiondetailsonroll.csv"
    )
    termly_sessions = create_sessions_output(
        pupil=pupil_termly_sessions,
        identifier="pupilonrolltableid",
        session_identifier="termlysessionspossible",
        sessions=termly_sessions,
    )
    outputs["termly_attendance"] = termly_sessions

    if "school_census_summerhalfterm2sessiondetailsonroll.csv" in files:
        pupil_summer_sessions = demographics[
            [
                "pupilonrolltableid",
                "NativeId",
                "child_home_GIAS",
                "summerhalfterm2sessionspossible",
                "LA",
                "Year",
                "Term",
                "Acad/LA",
            ]
        ].copy()
        summer_sessions = open_file(
            session_folder, "school_census_summerhalfterm2sessiondetailsonroll.csv"
        )
        summer_sessions = create_sessions_output(
            pupil=pupil_summer_sessions,
            identifier="pupilonrolltableid",
            session_identifier="summerhalfterm2sessionspossible",
            sessions=summer_sessions,
        )
        outputs["summer_attendance"] = summer_sessions

    # Create exclusions output
    exclusions = open_file(session_folder, "school_census_termlyexclusionsonroll.csv")
    exclusions = exclusions.merge(
        right=demographics[
            [
                "pupilonrolltableid",
                "NativeId",
                "child_home_GIAS",
                "LA",
                "Year",
                "Term",
                "Acad/LA",
            ]
        ].copy(),
        on=["pupilonrolltableid", "NativeId", "LA", "Year", "Term", "Acad/LA"],
    )
    outputs["exclusions"] = exclusions

    # Filter each output table into a separate table for each child_home_GIAS, only keeping rows where child_home_GIAS <> first three digits of NativeID
    final_output_dict = {}

    for key, df in outputs.items():
        # Filter the dfs to rows with different GIAS codes
        df = df[df["child_home_GIAS"].notna()]
        df = df[df["NativeId"].notna()]
        df["child_home_GIAS"] = df["child_home_GIAS"].astype(int).astype(str)
        before_count = len(df)
        filtered = df[
            df["NativeId"].astype(str).str[:3] != df["child_home_GIAS"].astype(str)
        ]
        after_count = len(filtered)
        log.info(f"{key} rows removed: {before_count - after_count}")

        # Group into separate outputs for each child_home_GIAS
        groups = list(filtered.groupby("child_home_GIAS"))
        num_groups = len(groups)
        log.info(f"{key} number of outputs created: {num_groups}")

        for group_value, output in groups:
            name = f"CROSS_{group_value}_{key}"
            final_output_dict[name] = output.reset_index(drop=True)

    # Export outputs
    cross_dc = DataContainer(final_output_dict)
    log.info("Writing School Census CROSS outputs to shared folder")
    cross_dc.export(output_folder, "", "csv")


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_region_session_folder() -> FS:
    log.info("Creating session folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesSCRegion
    )

    log.info("Opening incoming folder...")
    session_folder = session_folder.opendir(SessionNamesSCRegion.INCOMING_FOLDER)

    sc_reports_folder = workspace_folder().opendir("current/school_census/REGION")
    pl.move_files_for_sharing(sc_reports_folder, session_folder)

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def school_census_region_outputs(
    session_folder: FS,
):
    log.info("Removing previous REGION outputs from shared folder...")

    output_folder = shared_folder()
    existing_files = output_folder.listdir("/")
    region_files_regex = f"^REGION_"
    pl.remove_files(region_files_regex, existing_files, output_folder)

    log.info("Checking necessary files are present...")
    address_pattern = re.compile(r"addresses")
    fsm_pattern = re.compile(r"fsm")
    pupil_pattern = re.compile(r"pupilonroll")
    exclusions_pattern = re.compile(r"exclusions")
    session_pattern = re.compile(r"termlysession")

    files = session_folder.listdir("/")

    # Check if required files are present
    school_patterns = [
        address_pattern,
        fsm_pattern,
        pupil_pattern,
        exclusions_pattern,
        session_pattern,
    ]

    files_present = all(
        any(pattern.search(f) for f in files) for pattern in school_patterns
    )
    if not files_present:
        log.error(
            "Not all of the required outputs are present to create the REGION outputs"
        )
        return
    log.info(
        "All REGION school census input files present in session folder; proceeding to data transformation"
    )

    # Create empty dict to store outputs
    outputs = {}

    # On roll tables
    # Make demographics output
    pupil_on = open_file(session_folder, "school_census_pupilonroll.csv")

    # Open addresses files, where there may be one or two files to open
    addresses_pattern = re.compile(r"school_census_addresses\.csv$")
    addresses_onroll_pattern = re.compile(r"school_census_addressesonroll\.csv$")
    addresses_patterns = [addresses_pattern, addresses_onroll_pattern]
    addresses_on = open_and_concat_patterns(files, addresses_patterns, session_folder)
    addresses_on = addresses_on.drop(
        columns=["addressesorderseqcolumn", "addressesonrollorderseqcolumn"],
        errors="ignore",
    )

    # Open fsm files, where there may be one or two files to open
    fsm_pattern = re.compile(r"school_census_fsmperiod\.csv$")
    fsm_onroll_pattern = re.compile(r"school_census_fsmperiodsonroll\.csv$")
    fsm_patterns = [fsm_pattern, fsm_onroll_pattern]
    fsm_on = open_and_concat_patterns(files, fsm_patterns, session_folder)

    # Open sen file if exists (only in spring)
    sen_on = None
    if "school_census_senneeds.csv" in files:
        sen_on = open_file(session_folder, "school_census_senneeds.csv")

    # Create demographics output
    demographics_on = create_demographics_output(
        pupil_on, "pupilonrolltableid", addresses_on, fsm_on, sen_on
    )
    outputs["REGION_onroll_children"] = demographics_on

    # Create sessions outputs
    pupil_on_termly_sessions = pupil_on[
        [
            "pupilonrolltableid",
            "NativeId",
            "termlysessionspossible",
            "LA",
            "Year",
            "Term",
            "Acad/LA",
        ]
    ].copy()
    termly_sessions_on = open_file(
        session_folder, "school_census_termlysessiondetailsonroll.csv"
    )
    termly_sessions_on = create_sessions_output(
        pupil=pupil_on_termly_sessions,
        identifier="pupilonrolltableid",
        session_identifier="termlysessionspossible",
        sessions=termly_sessions_on,
    )
    outputs["REGION_onroll_termly_attendance"] = termly_sessions_on

    if "school_census_summerhalfterm2sessiondetailsonroll.csv" in files:
        pupil_on_summer_sessions = pupil_on[
            [
                "pupilonrolltableid",
                "NativeId",
                "summerhalfterm2sessionspossible",
                "LA",
                "Year",
                "Term",
                "Acad/LA",
            ]
        ].copy()
        summer_sessions_on = open_file(
            session_folder, "school_census_summerhalfterm2sessiondetailsonroll.csv"
        )
        summer_sessions_on = create_sessions_output(
            pupil=pupil_on_summer_sessions,
            identifier="pupilonrolltableid",
            session_identifier="summerhalfterm2sessionspossible",
            sessions=summer_sessions_on,
        )
        outputs["REGION_onroll_summer_attendance"] = summer_sessions_on

    # Create exclusions output
    exclusions_on = open_file(
        session_folder, "school_census_termlyexclusionsonroll.csv"
    )
    outputs["REGION_onroll_exclusions"] = exclusions_on

    # Off roll tables
    # Make demographics output
    if "school_census_pupilnolongeronroll.csv" in files:
        pupil_off = open_file(session_folder, "school_census_pupilnolongeronroll.csv")
        addresses_off = None
        if "school_census_addressesoffroll.csv" in files:
            addresses_off = open_file(
                session_folder, "school_census_addressesoffroll.csv"
            )

        fsm_off = None
        sen_off = None

        # Create demographics output if addresses exists
        if addresses_off is not None:
            demographics_off = create_demographics_output(
                pupil_off, "pupilnolongeronrolltableid", addresses_off, fsm_off, sen_off
            )
            outputs["REGION_offroll_children"] = demographics_off
        else:
            outputs["REGION_offroll_children"] = pupil_off

        # Create sessions outputs
        pupil_off_termly_sessions = pupil_off[
            [
                "pupilnolongeronrolltableid",
                "NativeId",
                "termlysessionspossible",
                "LA",
                "Year",
                "Term",
                "Acad/LA",
            ]
        ].copy()
        termly_sessions_off = open_file(
            session_folder, "school_census_termlysessiondetailsoffroll.csv"
        )
        termly_sessions_off = create_sessions_output(
            pupil=pupil_off_termly_sessions,
            identifier="pupilnolongeronrolltableid",
            session_identifier="termlysessionspossible",
            sessions=termly_sessions_off,
        )
        outputs["REGION_offroll_termly_attendance"] = termly_sessions_off

        if "school_census_summerhalfterm2sessiondetailsoffroll.csv" in files:
            pupil_off_summer_sessions = pupil_off[
                [
                    "pupilnolongeronrolltableid",
                    "NativeId",
                    "summerhalfterm2sessionspossible",
                    "LA",
                    "Year",
                    "Term",
                    "Acad/LA",
                ]
            ].copy()
            summer_sessions_off = open_file(
                session_folder, "school_census_summerhalfterm2sessiondetailsoffroll.csv"
            )
            summer_sessions_off = create_sessions_output(
                pupil=pupil_off_summer_sessions,
                identifier="pupilnolongeronrolltableid",
                session_identifier="summerhalfterm2sessionspossible",
                sessions=summer_sessions_off,
            )
            outputs["REGION_offroll_summer_attendance"] = summer_sessions_off

        # Create exclusions output
        exclusions_off = open_file(
            session_folder, "school_census_termlyexclusionsoffroll.csv"
        )
        outputs["REGION_offroll_exclusions"] = exclusions_off

    else:
        log.info("No pupilnolongeronroll file found; skipping off roll outputs")

    # Export outputs
    region_dc = DataContainer(outputs)
    log.info("Writing School Census REGION outputs to shared folder")
    region_dc.export(output_folder, "", "csv")
