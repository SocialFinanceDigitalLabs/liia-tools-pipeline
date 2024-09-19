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
from liiatools.cin_census_pipeline.spec import load_schema as load_schema_cin
from liiatools.cin_census_pipeline.stream_pipeline import (
    task_cleanfile as task_cleanfile_cin,
)
from liiatools.ssda903_pipeline.spec import load_schema as load_schema_ssda903
from liiatools.ssda903_pipeline.stream_pipeline import (
    task_cleanfile as task_cleanfile_ssda903,
)

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
    log.info(f"Session folder id: {session_id}")
    incoming_files = pl.move_files_for_processing(
        open_fs(config.dataset_folder), session_folder
    )

    return session_folder, session_id, incoming_files


@op(
    out={"current": Out(DataframeArchive)},
)
def open_current(config: CleanConfig) -> DataframeArchive:
    log.info("Opening Current folder...")
    current_folder = workspace_folder().makedirs("current", recreate=True)
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
    log.info("Procesing Files...")
    error_report = ErrorContainer()
    current.fs.removetree(f"{config.input_la_code}/{config.dataset}")

    for file_locator in incoming_files:
        log.info(f"Processing file {file_locator.name}")
        uuid = file_locator.meta["uuid"]
        year = pl.discover_year(file_locator)
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

        cleanfile_result.data.export(
            session_folder.opendir(SessionNames.CLEANED_FOLDER),
            file_locator.meta["uuid"] + "_",
            "parquet",
        )
        error_report.extend(cleanfile_result.errors)

        enrich_result = enrich_data(
            cleanfile_result.data, pipeline_config(config), metadata
        )
        enrich_result.data.export(
            session_folder.opendir(SessionNames.ENRICHED_FOLDER),
            file_locator.meta["uuid"] + "_",
            "parquet",
        )
        error_report.extend(enrich_result.errors)

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

        error_report.set_property("filename", file_locator.name)
        error_report.set_property("uuid", uuid)

    error_report.set_property("session_id", session_id)
    error_report_name = (
        f"{config.input_la_code}_{config.dataset}_{session_id}_error_report.csv"
        if config.input_la_code is not None
        else f"{config.dataset}_{session_id}_error_report.csv"
    )

    destination_logs = shared_folder().makedirs("logs", recreate=True)
    incoming_logs = open_fs(config.la_folder).makedirs("logs", recreate=True)
    log_locations = [session_folder, destination_logs, incoming_logs]

    for location in log_locations:
        with location.open(error_report_name, "w") as FILE:
            error_report.to_dataframe().to_csv(FILE, index=False)


@op()
def move_current_view():
    current_folder = workspace_folder().opendir("current")
    destination_folder = shared_folder().makedirs("current", recreate=True)
    destination_folder.removetree("/")
    pl.move_files_for_sharing(current_folder, destination_folder)


@op(
    ins={"current": In(DataframeArchive)},
)
def create_concatenated_view(current: DataframeArchive, config: CleanConfig):
    concat_folder = shared_folder().makedirs(
        f"concatenated/{config.dataset}", recreate=True
    )
    existing_files = concat_folder.listdir("/")

    for la_code in authorities.codes:
        la_files_regex = f"{la_code}_{config.dataset}_"
        pl.remove_files(la_files_regex, existing_files, concat_folder)

        concat_data = current.current(la_code)

        if concat_data:
            concat_data.export(concat_folder, f"{la_code}_{config.dataset}_", "csv")
