import re

from dagster import In, Out, get_dagster_logger, op
from decouple import config as env_config
from fs.base import FS
from ruamel.yaml import YAML

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesPanPlacementsStandardJoins
from liiatools.common.data import DataContainer
from liiatools.common.pipeline import open_file
from liiatools.placements_standard_pipeline.placements_standard_dataset_join import (
    join_episodes_data,
)
from liiatools_pipeline.assets.common import (
    shared_folder,
    workspace_folder,
)
from liiatools_pipeline.ops.common_config import CdmConfig
from liiatools_pipeline.util.utility import opendir_location

yaml = YAML()

log = get_dagster_logger()


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_cdm_placements_standard_session_folder(config: CdmConfig) -> FS:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesPanPlacementsStandardJoins
    )
    """
    Create Placements Standard session folder and move required files for mapping into it
    """
    log.info("Creating Placements Standard session folder")
    session_folder = session_folder.opendir(SessionNamesPanPlacementsStandardJoins.INCOMING_FOLDER)

    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")

    if "placements_standard" in config.datasets:
        placements_standard_reports_folder = opendir_location(workspace_folder(), "current/placements_standard/CDM")
        pl.move_files_for_sharing(
            placements_standard_reports_folder,
            session_folder,
        )

    if "ssda903" in allowed_datasets and "ssda903" in config.datasets:
        ssda903_reports_folder = opendir_location(workspace_folder(), "current/ssda903/CDM")
        pl.move_files_for_sharing(
            ssda903_reports_folder,
            session_folder,
            required_table_id=["episodes"],
            )

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def joins_cdm_placements_standard(
    session_folder: FS,
    config: CdmConfig
):
    log.info("Checking necessary files are present...")
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")

    # Placements standard file patterns
    placements_standard_pattern = re.compile(r"placements_standard")

    # SSDA903 file patterns
    episodes_pattern = re.compile(r"episodes")

    files = session_folder.listdir("/")

    log.info("Removing existing CDM_PLACEMENTS_STANDARD file from the shared folder before processing a new one")
    existing_cdm_placements_standard_files = shared_folder().listdir("/")
    pl.remove_files("CDM_PLACEMENTS_STANDARD", existing_cdm_placements_standard_files, shared_folder())

    if "placements_standard" not in config.datasets:
        log.error("Placements Standard dataset not included in CDM config")
        return

    placements_standard_file = next((f for f in files if placements_standard_pattern.search(f)), None)
    if not placements_standard_file:
        log.error("No placements standard file to open. Exiting run as placements standard file not available")
        return

    # If no Placements Standard or SSDA903 files, terminate process
    if not any(
        any(pattern.search(f) for f in files) for pattern in [episodes_pattern]
    ):
        log.error("No SSDA903 files found: terminating process.")
        return

    # Open the placements standard file
    placements_standard = open_file(session_folder, placements_standard_file)
    placements_standard_columns = [
        "child_ID",
        "when_placement_is_needed_by",
        "number_of_siblings_to_place_with",
        "communication_language_learning_needs",
        "specific_communication_and_language_requirements",
        "adaptation_to_the_home",
        "cultural_needs",
        "who_can_the_child_be_cared_for_alongside",
        "can_child_live_with_pets",
        "additional_support",
        "mental_health_diagnosis",
        "neurodevelopmental_diagnosis",
        "open_to_CAMHS",
        "needs_assessment",
        "foster_care_suitability",
        "residential_care_suitability",
        "supported_accommodation_suitability",
        "risk_to_child_self_harm",
        "risk_to_child_criminal_exploitation",
        "risk_to_child_drug_and_alcohol_use",
        "risk_to_child_eating_disorder",
        "risk_to_child_going_missing",
        "risk_to_child_sexual_exploitation",
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
        "placement_start_date",
        "total_weekly_cost",
        "preferability_of_placement_location",
        "placement_location_non_preferable_reason",
        "education_continuity",
        "how_many_siblings_were_placed_together",
        "placement_type_offers",
        "placement_sourced",
        "LA",
        "Month",
        "Year"
    ]
    placements_standard = placements_standard[placements_standard_columns]

    # Check and process SSDA903 episodes file type
    if "ssda903" in allowed_datasets and "ssda903" in config.datasets:
        if any(episodes_pattern.search(f) for f in files):
            log.info("Joining SSDA903 episodes data with placements standard data")
            episodes_file = next(f for f in files if episodes_pattern.search(f))
            episodes = open_file(session_folder, episodes_file)
            placements_standard = join_episodes_data(episodes, placements_standard)
        else:
            log.error("No SSDA903 episodes data to join with placements standard data")
            empty_episodes_cols = ["EPISODE_ID"]
            for col in empty_episodes_cols:
                placements_standard[col] = None

    # Export header file
    placements_standard_dc = DataContainer({"CDM_PLACEMENTS_STANDARD": placements_standard})
    log.info("Writing joined placements standard output to shared folder")
    placements_standard_dc.export(shared_folder(), "", "csv")