from typing import List, Tuple
from dagster import In, Out, op
from fs.base import FS
from fs import open_fs

from liiatools.common import pipeline as pl
from liiatools.common.archive import DataframeArchive
from liiatools.common.checks import check_year_within_range
from liiatools.common.constants import SessionNames
from liiatools.common.data import FileLocator, ErrorContainer
from liiatools.common.reference import authorities
from liiatools.common.stream_errors import StreamError
from liiatools.common.transform import degrade_data, enrich_data, prepare_export

from liiatools_pipeline.assets.common import (
    workspace_folder,
    shared_folder,
    pipeline_config,
)
from liiatools_pipeline.ops.common_config import CleanConfig

from dagster import get_dagster_logger

log = get_dagster_logger(__name__)


@op(
    out={
        "session_folder": Out(FS),
        "session_id": Out(str),
        "incoming_files": Out(List[FileLocator]),
    }
)
def create_session_folder(config: CleanConfig) -> Tuple[FS, str, List[FileLocator]]:
    log.info("Creating Session Folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNames
    )
    log.debug(f"Session folder id: {session_id}")
    incoming_files = pl.move_files_for_processing(
        open_fs(config.dataset_folder), session_folder
    )

    return session_folder, session_id, incoming_files


@op(
    out={"current": Out(DataframeArchive)},
)
def open_current(config: CleanConfig) -> DataframeArchive:
    log.info("Opening Current folder...")
    try:
        current_folder = workspace_folder().makedirs("current", recreate=True)
    except Exception as err:
        log.error(f"Getting/Creating current folder from workspace failed: {err}")
    current = DataframeArchive(current_folder, pipeline_config(config), config.dataset)
    return current


@op(
    ins={
        "session_folder": In(FS),
        "incoming_files": In(List[FileLocator]),
        "current": In(DataframeArchive),
        "session_id": In(str),
    },
)
def process_files(
    session_folder: FS,
    incoming_files: List[FileLocator],
    current: DataframeArchive,
    session_id: str,
    config: CleanConfig,
):
    error_report = ErrorContainer()

    if current.fs.isdir(f"{config.input_la_code}/{config.dataset}"):
        log.info(
            f"Removing existing LA data for {config.input_la_code}/{config.dataset}..."
        )
        current.fs.removetree(f"{config.input_la_code}/{config.dataset}")

    for file_locator in incoming_files:
        log.info(f"Processing file {file_locator.name}")
        uuid = file_locator.meta["uuid"]
        year = pl.discover_year(file_locator)
        log.debug(f"Discovered year: {year} from {file_locator.name}")
        if year is None:
            error_report.append(
                dict(
                    type="MissingYear",
                    message="Could not find a year in the filename or path",
                    filename=file_locator.name,
                    uuid=uuid,
                )
            )
            continue

        if (
            check_year_within_range(
                year, max(pipeline_config(config).retention_period.values())
            )
            is False
        ):
            error_report.append(
                dict(
                    type="RetentionPeriod",
                    message="This file is not within the year ranges of data retention policy",
                    filename=file_locator.name,
                    uuid=uuid,
                )
            )
            continue

        if config.input_la_code is None:
            error_report.append(
                dict(
                    type="MissingLA",
                    message="Could not find a local authority in the filename or path",
                    filename=file_locator.name,
                    uuid=uuid,
                )
            )
            continue

        try:
            schema = globals()[f"load_schema_{config.dataset}"](year)
        except KeyError:
            log.warning(f"Unable to load schema: {config.dataset} from year {year}")
            # ISSUE: It will likely be an issue that schema is undefined past this point, right?
            continue

        metadata = dict(year=year, schema=schema, la_code=config.input_la_code)

        try:
            cleanfile_result = globals()[f"task_cleanfile_{config.dataset}"](
                file_locator, schema
            )
        except StreamError as e:
            error_report.append(
                dict(
                    type="StreamError",
                    message=str(e),
                    filename=file_locator.name,
                    uuid=uuid,
                )
            )
            continue

        cleanfile_result.data = prepare_export(
            cleanfile_result.data, pipeline_config(config), profile="PAN"
        )

        log.info(f"Exporting cleanfile result... {file_locator.name}")
        cleanfile_result.data.export(
            session_folder.opendir(SessionNames.CLEANED_FOLDER),
            file_locator.meta["uuid"] + "_",
            "parquet",
        )
        error_report.extend(cleanfile_result.errors)

        log.info(f"Enriching result... {file_locator.name}")
        enrich_result = enrich_data(
            cleanfile_result.data, pipeline_config(config), metadata
        )
        enrich_result.data.export(
            session_folder.opendir(SessionNames.ENRICHED_FOLDER),
            file_locator.meta["uuid"] + "_",
            "parquet",
        )
        error_report.extend(enrich_result.errors)

        log.info(f"Degrading result... {file_locator.name}")
        degraded_result = degrade_data(
            enrich_result.data, pipeline_config(config), metadata
        )
        degraded_result.data.export(
            session_folder.opendir(SessionNames.DEGRADED_FOLDER),
            file_locator.meta["uuid"] + "_",
            "parquet",
        )
        error_report.extend(degraded_result.errors)
        current.add(degraded_result.data, config.input_la_code, year)

        log.debug(f"Setting properties to {file_locator.name}: UUID: {uuid}")
        error_report.set_property("filename", file_locator.name)
        error_report.set_property("uuid", uuid)

    error_report.set_property("session_id", session_id)
    error_report_name = (
        f"{config.input_la_code}_{config.dataset}_{session_id}_error_report.csv"
        if config.input_la_code is not None
        else f"{config.dataset}_{session_id}_error_report.csv"
    )

    log.debug(f"Creating logs directory in shared space for LA {config.la_folder}...")
    destination_logs = shared_folder().makedirs("logs", recreate=True)
    incoming_logs = open_fs(config.la_folder).makedirs("logs", recreate=True)
    log_locations = [session_folder, destination_logs, incoming_logs]

    for location in log_locations:
        log.debug(f"Writing {error_report_name} logs to {location}...")
        with location.open(error_report_name, "w") as FILE:
            error_report.to_dataframe().to_csv(FILE, index=False)


@op()
def move_current_view_la():
    log.info(f"Opening workspace current folder...")
    try:
        current_folder = workspace_folder().opendir("current")
    except Exception as err:
        log.error(f"Could not open Workspace current folder: {err}")

    log.info(f"Opening shared current folder...")
    try:
        destination_folder = shared_folder().makedirs("current", recreate=True)
    except Exception as err:
        log.error(f"Could not open current shared workspace folder: {err}")

    log.info("Removing old files in the shared folder...")
    try:
        destination_folder.removetree("/")
    except Exception as err:
        log.error(f"Could not remove files in shared folder: {err}")

    log.info("Moving current files from workspace to the shared area...")
    pl.move_files_for_sharing(current_folder, destination_folder)


@op(
    ins={"current": In(DataframeArchive)},
)
def create_concatenated_view(current: DataframeArchive, config: CleanConfig):
    log.info(f"Creating concatenated folder for dataset {config.dataset}")
    try:
        concat_folder = shared_folder().makedirs(
            f"concatenated/{config.dataset}", recreate=True
        )
    except Exception as err:
        log.error(f"Could not make concatenated folder in the shared space...")

    log.info(f"Listing contents of concat folder in shared space...")
    existing_files = concat_folder.listdir("/")

    for la_code in authorities.codes:
        la_files_regex = f"{la_code}_{config.dataset}_"
        log.info(f"Removing old concat files for LA: {la_code}_{config.dataset}_")

        pl.remove_files(la_files_regex, existing_files, concat_folder)

        concat_data = current.current(la_code)

        if concat_data:
            try:
                concat_data.export(concat_folder, f"{la_code}_{config.dataset}_", "csv")
            except Exception as err:
                log.error(
                    f"Failed to export concat data for {la_code}_{config.dataset}: {err}"
                )
