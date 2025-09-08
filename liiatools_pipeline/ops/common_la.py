from os.path import basename
from typing import List, Tuple

import fs.errors
from dagster import In, Out, get_dagster_logger, op
from fs import open_fs
from fs.base import FS

from liiatools.annex_a_pipeline.spec import load_schema as load_schema_annex_a
from liiatools.annex_a_pipeline.stream_pipeline import (
    task_cleanfile as task_cleanfile_annex_a,
)
from liiatools.cin_census_pipeline.spec import load_schema as load_schema_cin
from liiatools.cin_census_pipeline.stream_pipeline import (
    task_cleanfile as task_cleanfile_cin,
)
from liiatools.common import pipeline as pl
from liiatools.common.archive import DataframeArchive
from liiatools.common.checks import check_year_within_range
from liiatools.common.constants import SessionNames
from liiatools.common.data import ErrorContainer, FileLocator
from liiatools.common.reference import authorities
from liiatools.common.stream_errors import StreamError
from liiatools.common.transform import degrade_data, enrich_data, prepare_export
from liiatools.pnw_census_pipeline.spec import load_schema as load_schema_pnw_census
from liiatools.pnw_census_pipeline.stream_pipeline import (
    task_cleanfile as task_cleanfile_pnw_census,
)
from liiatools.ssda903_pipeline.spec import load_schema as load_schema_ssda903
from liiatools.ssda903_pipeline.stream_pipeline import (
    task_cleanfile as task_cleanfile_ssda903,
)
from liiatools_pipeline.assets.common import (
    pipeline_config,
    shared_folder,
    workspace_folder,
)
from liiatools_pipeline.ops.common_config import CleanConfig
from liiatools_pipeline.util.utility import opendir_location

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
    la_name = authorities.get_by_code(config.input_la_code)
    log.info(f"Processing {config.dataset} {la_name} files...")

    error_report = ErrorContainer()
    current_path = f"{config.input_la_code}/{config.dataset}"
    if current.fs.isdir(current_path):
        log.info("Removing existing LA data...")
        current_files = current.fs.listdir(current_path)
        for file in current_files:
            current.fs.remove(f"{current_path}/{file}")

    output_config = pipeline_config(config)
    la_signed = output_config.la_signed[la_name]["PAN"]
    if la_signed == "No":
        log.info(
            f"{la_name} is not signed up for PAN {config.dataset} data processing."
        )
        error_report.append(
            dict(
                type="NotSigned",
                message=f"{la_name} is not signed up for data processing.",
            )
        )
    else:
        log.info(f"{la_name} is signed for PAN {config.dataset} data processing.")

        for file_locator in incoming_files:
            log.info(f"Processing file {basename(str(file_locator.name))}")
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
            log.info(f"Discovered year in {basename(str(file_locator.name))}")

            if (
                check_year_within_range(
                    year, max(output_config.retention_period.values())
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
            log.info(
                f"Year in {basename(str(file_locator.name))} is within retention period"
            )

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
            log.info(
                f"Local authority code found in {basename(str(file_locator.name))}"
            )

            month = None
            if config.dataset in ["annex_a", "pnw_census"]:
                month = pl.discover_month(file_locator)
                if month is None:
                    error_report.append(
                        dict(
                            type="MissingMonth",
                            message="Could not find a month in the filename or path",
                            filename=file_locator.name,
                            uuid=uuid,
                        )
                    )
                    continue
                log.info(f"Month found in {basename(str(file_locator.name))}")

            try:
                schema = (
                    globals()[f"load_schema_{config.dataset}"]()
                    if config.dataset in ["annex_a", "pnw_census"]
                    else globals()[f"load_schema_{config.dataset}"](year)
                )
            except KeyError:
                continue
            log.info(
                f"{config.dataset} schema loaded for {basename(str(file_locator.name))}"
            )

            metadata = dict(
                year=year, month=month, schema=schema, la_code=config.input_la_code
            )

            try:
                cleanfile_result = (
                    globals()[f"task_cleanfile_{config.dataset}"](
                        file_locator, schema, output_config, logger=log
                    )
                    if config.dataset == "cin"
                    else globals()[f"task_cleanfile_{config.dataset}"](
                        file_locator, schema, logger=log
                    )
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
            log.info(f"Cleanfile task completed for {basename(str(file_locator.name))}")

            cleanfile_result.data = prepare_export(
                cleanfile_result.data, output_config, profile="PAN"
            )

            cleanfile_result.data.export(
                session_folder.opendir(SessionNames.CLEANED_FOLDER),
                file_locator.meta["uuid"] + "_",
                "parquet",
            )
            error_report.extend(cleanfile_result.errors)

            log.info(f"Cleanfile exported for {basename(file_locator.name)}")

            enrich_result = enrich_data(cleanfile_result.data, output_config, metadata)
            enrich_result.data.export(
                session_folder.opendir(SessionNames.ENRICHED_FOLDER),
                file_locator.meta["uuid"] + "_",
                "parquet",
            )
            error_report.extend(enrich_result.errors)

            log.info(f"Enrichfile exported for {basename(file_locator.name)}")

            degraded_result = degrade_data(enrich_result.data, output_config, metadata)
            degraded_result.data.export(
                session_folder.opendir(SessionNames.DEGRADED_FOLDER),
                file_locator.meta["uuid"] + "_",
                "parquet",
            )
            error_report.extend(degraded_result.errors)
            current.add(degraded_result.data, config.input_la_code, year, month)

            log.info(f"Degraded file exported for {basename(file_locator.name)}")

            error_report.extend(current.deduplicate(cleanfile_result.data).errors)

            error_report.set_property("filename", file_locator.name)
            error_report.set_property("uuid", uuid)
            log.info(f"Finished processing {basename(file_locator.name)} errors")

    log.info(f"Writing error report for {config.input_la_code} {config.dataset}")
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

    log.info(f"Error report for {config.input_la_code} written for {config.dataset}")


@op()
def move_current_view_la():
    current_folder = opendir_location(workspace_folder(), "current")
    if current_folder is not None:
        destination_folder = shared_folder().makedirs("current", recreate=True)
        destination_folder.removetree("/")
        pl.move_files_for_sharing(current_folder, destination_folder)
    else:
        raise fs.errors.ResourceNotFound(
            f"Current folder not found in {workspace_folder()}"
        )


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
        log.info(f"Removing files with regex: {la_files_regex}")
        pl.remove_files(la_files_regex, existing_files, concat_folder)
        log.info(f"Successfully removed files")

        if config.dataset == "annex_a":
            concat_data = current.current(la_code, deduplicate_mode="N")
        else:
            concat_data = current.current(la_code)

        if concat_data:
            log.info("Exporting concatenated data")
            concat_data.export(concat_folder, f"{la_code}_{config.dataset}_", "csv")
