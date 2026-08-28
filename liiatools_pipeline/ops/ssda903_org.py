import re

import pandas as pd
from dagster import In, Out, get_dagster_logger, op
from decouple import config as env_config
from fs import errors
from fs.base import FS
from pandas.tseries.offsets import MonthEnd

from liiatools.common import pipeline as pl
from liiatools.common.constants import (
    SessionNamesPanSufficiencyJoins,
    SessionNamesSufficiency,
)
from liiatools.common.data import DataContainer
from liiatools.ssda903_pipeline.ssda903_dataset_join import (
    join_header_data,
    join_placement_standard_data,
    join_pnw_data,
    join_uasc_data,
)
from liiatools.ssda903_pipeline.sufficiency_transform import (
    dict_to_dfs,
    ofsted_transform,
    ons_transform,
    open_file,
    postcode_transform,
    ss903_transform,
)
from liiatools_pipeline.assets.common import shared_folder, workspace_folder
from liiatools_pipeline.assets.external_dataset import external_data_folder

log = get_dagster_logger()

@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_sufficiency_session_folder() -> FS:
    log.info("Creating session folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesSufficiency
    )
    session_folder = session_folder.opendir(SessionNamesSufficiency.INCOMING_FOLDER)

    reports_folder = workspace_folder().opendir("current/ssda903/SUFFICIENCY")
    log.info("Moving 903 input files for sufficiency to session folder")
    pl.move_files_for_sharing(
        reports_folder, session_folder, required_table_id=["episodes", "header", "uasc"]
    )

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def create_dim_fact_tables(
    session_folder: FS,
):
    # Check that the files necessary for the job are in the folder
    log.info("Checking required input files are all present")
    episodes = re.compile(r"episodes")
    header = re.compile(r"header")
    uasc = re.compile(r"uasc")
    pattern_list = [episodes, header, uasc]

    files = session_folder.listdir("/")

    all_files_present = all(
        any(pattern.search(filename) for filename in files) for pattern in pattern_list
    )

    # Run the data transformation if all necessary files are present
    if all_files_present:
        log.info("All 903 input files present in session folder; proceeding to data transformation")
        ext_folder = external_data_folder()
        output_folder = shared_folder()

        # Create dictionary to store tables, starting with basic dim tables
        dim_tables = dict_to_dfs()

        # Create dimONSArea table
        # Open external file
        try:
            ONSArea = open_file(ext_folder, "ONS_Area.csv")
        except errors.ResourceNotFound as err:
            log.error(f"No ONS_Area file to open: {err}")
            log.info("Exiting run as external dataset resources not available")
            return
        
        # Transform ONSArea table
        ONSArea = ons_transform(ONSArea)
        log.info("Creating dimONSArea table")
        dim_tables["dimONSArea"] = ONSArea

        # Create dimPostcode table
        # Open external file
        try:
            Postcode = open_file(ext_folder, "ONSPD_reduced_to_postcode_sector.csv")
        except errors.ResourceNotFound as err:
            log.error(f"No ONSPD postcode file to open: {err}")
            log.info("Exiting run as external dataset resources not available")
            return
                    
        # Transform Postcode table
        Postcode = postcode_transform(Postcode)
        log.info("Creating dimPostcode table")
        dim_tables["dimPostcode"] = Postcode

        # Create dimOfstedProvider and factOfstedInspection tables
        # Open and transform files
        OfstedProvider, factOfstedInspection = ofsted_transform(ext_folder, ONSArea, log)
        if OfstedProvider is None and factOfstedInspection is None:
            log.info("Terminating process")
            return
        
        log.info("Creating dimOfstedProvider table")
        dim_tables["dimOfstedProvider"] = OfstedProvider
        log.info("Creating factOfstedInspection table")
        dim_tables["factOfstedInspection"] = factOfstedInspection

        # Create dimLookedAfterChild and factEpisode table
        # Open ssda903 files
        LookedAfterChild = open_file(session_folder, "ssda903_header.csv")
        UASC = open_file(session_folder, "ssda903_uasc.csv")
        Episode = open_file(session_folder, "ssda903_episodes.csv")

        # Transform tables
        LookedAfterChild, factEpisode = ss903_transform(
            LookedAfterChild, UASC, ONSArea, Episode, Postcode, OfstedProvider
        )
        log.info("Creating dimLookedAfterChild table")
        dim_tables["dimLookedAfterChild"] = LookedAfterChild
        log.info("Creating factEpisode table")
        dim_tables["factEpisode"] = factEpisode

        # Export tables
        dim_tables = DataContainer(dim_tables)

        reports_folder = workspace_folder().opendir("current/ssda903/SUFFICIENCY")
        log.info("Exporting output tables to org current folder")
        dim_tables.export(reports_folder, "", "csv")

        log.info("Exporting output tables to org shared folder")
        dim_tables.export(output_folder, "", "csv")

    # If three 903 input files not present, terminate process with log
    else:
        log.info("903 input files for sufficiency not all present: terminating process")


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_pan_sufficiency_join_session_folder() -> FS:
    log.info("Creating session folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesPanSufficiencyJoins
    )

    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")

    log.info("Opening incoming folder...")
    session_folder = session_folder.opendir(SessionNamesPanSufficiencyJoins.INCOMING_FOLDER)

    ssda903_reports_folder = workspace_folder().opendir("current/ssda903/PAN")
    pl.move_files_for_sharing(
        ssda903_reports_folder,
        session_folder,
        required_table_id=["episodes", "header", "uasc"],
        )

    if "pnw_census" in allowed_datasets:
        pnw_census_reports_folder = workspace_folder().opendir("current/pnw_census/PAN")
        pl.move_files_for_sharing(pnw_census_reports_folder, session_folder)

    if "placement_standards" in allowed_datasets:
        placement_standards_reports_folder = workspace_folder().opendir("current/placement_standards/PAN")
        pl.move_files_for_sharing(placement_standards_reports_folder, session_folder)

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def joins_pan_sufficiency(
    session_folder: FS,
):
    log.info("Checking necessary files are present...")
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")

    # SSDA903 file patterns
    episodes_pattern = re.compile(r"episodes")
    header_pattern = re.compile(r"header")
    uasc_pattern = re.compile(r"uasc")

    # PNW file pattern
    pnw_pattern = re.compile(r"pnw")

    # Placement standard file patterns
    placement_standard_pattern = re.compile(r"placement_standard")

    files = session_folder.listdir("/")

    try:
        episodes_file = next((f for f in files if episodes_pattern.search(f)), None)
    except errors.ResourceNotFound as err:
        log.error(f"No SSDA903 episodes file to open: {err}")
        log.info("Exiting run as SSDA903 episodes file not available")
        return

    ssda903_patterns = [
        header_pattern,
        uasc_pattern,
    ]

    # If no SSDA903, PNW or Placement Standard files, terminate process
    if not any(
        any(pattern.search(f) for f in files) for pattern in ssda903_patterns
    ) and not any(pattern.search(f) for f in files for pattern in [placement_standard_pattern]
    ) and not any(pattern.search(f) for f in files for pattern in [pnw_pattern]):
        log.error("No SSDA903, PNW or Placement Standard files found: terminating process.")
        return

    # Open the SSDA903 episodes file
    episodes = open_file(session_folder, episodes_file)

    # Check and process each SSDA903 file type
    if any(header_pattern.search(f) for f in files):
        log.info("Joining SSDA903 header data with SSDA903 episodes data")
        header_file = next(f for f in files if header_pattern.search(f))
        header = open_file(session_folder, header_file)
        episodes = join_header_data(header, episodes)
    else:
        log.error("No 903 header data to join")
        empty_header_cols = ["SEX", "ETHNIC", "DOB"]
        for col in empty_header_cols:
            episodes[col] = None

    if any(uasc_pattern.search(f) for f in files):
        log.info("Joining SSDA903 UASC data with SSDA903 episodes data")
        uasc_file = next(f for f in files if uasc_pattern.search(f))
        uasc = open_file(session_folder, uasc_file)
        episodes = join_uasc_data(uasc, episodes)
    else:
        log.error("No 903 uasc data to join")
        empty_uasc_cols = ["DUC"]
        for col in empty_uasc_cols:
            episodes[col] = None

    if "pnw_census" in allowed_datasets:
        pnw_join_columns = ["Type of provision", "Primary Registration type"]
        # Check and process the PNW Census file
        if any(pnw_pattern.search(f) for f in files):
            log.info("Joining PNW Census data with SSDA903 episodes data")
            pnw_census_file = next(f for f in files if pnw_pattern.search(f))
            pnw_census = open_file(session_folder, pnw_census_file)

            # Derive 'snapshot' date used in every table join equal to the last day of the snapshot month
            pnw_census["snapshot_date"] = pd.to_datetime(
                pnw_census[["Year", "Month"]].assign(day=1)
            ) + MonthEnd(0)

            episodes = join_pnw_data(pnw_census, episodes, pnw_join_columns)
        else:
            log.error("No PNW Census data to join")
            for col in pnw_join_columns:
                episodes[col] = None

    if "placement_standards" in allowed_datasets:
        placement_standard_join_columns = [
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
                ]
        # Check and process the Placement Standard file
        if any(placement_standard_pattern.search(f) for f in files):
            log.info("Joining Placement Standard data with SSDA903 episodes data")
            placement_standard_file = next(f for f in files if placement_standard_pattern.search(f))
            placement_standard = open_file(session_folder, placement_standard_file)

            episodes = join_placement_standard_data(placement_standard, episodes, placement_standard_join_columns)
        else:
            log.error("No Placement Standard data to join")
            for col in placement_standard_join_columns:
                episodes[col] = None

    # Export Episodes file
    episodes_dc = DataContainer({"PAN_SUFFICIENCY": episodes})
    log.info("Writing joined episodes output to shared folder")
    output_folder = shared_folder()
    episodes_dc.export(output_folder, "", "csv")