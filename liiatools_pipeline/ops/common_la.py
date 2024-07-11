from dagster import Config
from typing import List, Tuple
from dagster import In, Out, op
from fs.base import FS
from liiatools.common import pipeline as pl
from liiatools.common.archive import DataframeArchive
from liiatools.common.constants import SessionNames
from liiatools.common.data import FileLocator, ErrorContainer
from liiatools.common.reference import authorities
from liiatools.common.stream_errors import StreamError
from liiatools.common.transform import degrade_data, enrich_data

from liiatools.cin_census_pipeline.spec import load_schema as load_schema_cin, load_schema_path as load_schema_path_cin
from liiatools.ssda903_pipeline.spec import load_schema as load_schema_ssda903

from liiatools.cin_census_pipeline.stream_pipeline import task_cleanfile as task_cleanfile_cin
from liiatools.ssda903_pipeline.stream_pipeline import task_cleanfile as task_cleanfile_ssda903

from dagster import get_dagster_logger

from liiatools_pipeline.assets.common import (
    incoming_folder,
    workspace_folder,
    shared_folder,
    pipeline_config,
    la_code as input_la_code,
    dataset,
)

log = get_dagster_logger()


class FileConfig(Config):
    filename: str
    name: str


@op(
    out={
        "session_folder": Out(FS),
        "session_id": Out(str),
        "incoming_files": Out(List[FileLocator]),
    }
)
def create_session_folder() -> Tuple[FS, str, List[FileLocator]]:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNames
    )
    incoming_files = pl.move_files_for_processing(incoming_folder(), session_folder)

    return session_folder, session_id, incoming_files


@op(
    out={"current": Out(DataframeArchive)},
)
def open_current() -> DataframeArchive:
    current_folder = workspace_folder().makedirs("current", recreate=True)
    current = DataframeArchive(current_folder, pipeline_config(), f"{dataset()}")
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
):
    log.info(f"Processing files for dataset type: {dataset}")

    error_report = ErrorContainer()
    for file_locator in incoming_files:
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

        la_code = (
            input_la_code()
            if input_la_code() is not None
            else pl.discover_la(file_locator)
        )
        if la_code is None:
            error_report.append(
                dict(
                    type="MissingLA",
                    message="Could not find a local authority in the filename or path",
                    filename=file_locator.name,
                    uuid=uuid,
                )
            )
            continue

        # Determine the schema and schema path based on the dataset type
        if dataset == 'cin':
            schema = load_schema_cin(year)
            schema_path = load_schema_path_cin(year=year)
            task_cleanfile = task_cleanfile_cin
        elif dataset == 'ssda903':
            schema = load_schema_ssda903(year)
            task_cleanfile = task_cleanfile_ssda903
        else:
            error_report.append(
                dict(
                    type="InvalidDatasetType",
                    message="Invalid dataset type specified",
                    filename=file_locator.name,
                    uuid=uuid,
                )
            )
            continue

        metadata = dict(year=year, schema=schema, la_code=la_code)

        try:
            cleanfile_result = task_cleanfile(file_locator, schema, schema_path)
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

        cleanfile_result.data.export(
            session_folder.opendir(SessionNames.CLEANED_FOLDER),
            file_locator.meta["uuid"] + "_",
            "parquet",
        )
        error_report.extend(cleanfile_result.errors)

        enrich_result = enrich_data(cleanfile_result.data, pipeline_config(), metadata)
        enrich_result.data.export(
            session_folder.opendir(SessionNames.ENRICHED_FOLDER),
            file_locator.meta["uuid"] + "_",
            "parquet",
        )
        error_report.extend(enrich_result.errors)

        degraded_result = degrade_data(enrich_result.data, pipeline_config(), metadata)
        degraded_result.data.export(
            session_folder.opendir(SessionNames.DEGRADED_FOLDER),
            file_locator.meta["uuid"] + "_",
            "parquet",
        )
        error_report.extend(degraded_result.errors)
        current.add(degraded_result.data, la_code, year)

        error_report.set_property("filename", file_locator.name)
        error_report.set_property("uuid", uuid)

    error_report.set_property("session_id", session_id)
    error_report_name = (
        f"{input_la_code()}_{dataset()}_{session_id}_error_report.csv"
        if input_la_code() is not None
        else f"{dataset()}_{session_id}_error_report.csv"
    )
    with session_folder.open(error_report_name, "w") as FILE:
        error_report.to_dataframe().to_csv(FILE, index=False)


@op()
def move_current_view():
    current_folder = workspace_folder().opendir("current")
    destination_folder = shared_folder().makedirs("current", recreate=True)
    pl.move_files_for_sharing(current_folder, destination_folder)


@op(
    ins={"current": In(DataframeArchive)},
)
def create_concatenated_view(current: DataframeArchive):
    concat_folder = shared_folder().makedirs("concatenated", recreate=True)
    for la_code in authorities.codes:
        concat_data = current.current(la_code)

        if concat_data:
            concat_data.export(concat_folder, f"{la_code}_{dataset()}", "csv")

