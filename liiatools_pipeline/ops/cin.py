# from typing import List, Tuple
#
# from dagster import In, Nothing, Out, op
# from fs.base import FS
# from liiatools.common import pipeline as pl
# from liiatools.common.archive import DataframeArchive
# from liiatools.common.constants import SessionNames
# from liiatools.common.data import DataContainer, FileLocator, ErrorContainer
# from liiatools.common.transform import degrade_data, enrich_data, prepare_export
# from liiatools.cin_census_pipeline.spec import load_schema, load_schema_path
# from liiatools.cin_census_pipeline.stream_pipeline import task_cleanfile
#
# from liiatools_pipeline.assets.cin import (
#     incoming_folder_cin,
#     pipeline_config_cin,
#     process_folder_cin,
# )
#
#
# @op(
#     out={
#         "session_folder": Out(FS),
#         "session_id": Out(str),
#         "incoming_files": Out(List[FileLocator]),
#     }
# )
#
# # Changed names to be cin specific so it doesnt throw error of duplication with ssda903 when run on dagster
#
# def create_session_folder_cin() -> Tuple[FS, str, List[FileLocator]]:
#     session_folder, session_id = pl.create_session_folder(process_folder_cin())
#     incoming_files = pl.move_files_for_processing(incoming_folder_cin(), session_folder)
#
#     return session_folder, session_id, incoming_files
#
#
# @op(
#     out={"archive": Out(DataframeArchive)},
# )
# def open_archive_cin(session_id) -> DataframeArchive:
#     archive_folder = process_folder_cin().makedirs("archive", recreate=True)
#     archive = DataframeArchive(archive_folder, pipeline_config_cin(), session_id)
#     return archive
#
#
# # TODO: Add optional la_code argument
# @op(
#     ins={
#         "session_folder": In(FS),
#         "incoming_files": In(List[FileLocator]),
#         "archive": In(DataframeArchive),
#         "session_id": In(str),
#     },
# )
# def process_files_cin(
#     session_folder: FS,
#     incoming_files: List[FileLocator],
#     archive: DataframeArchive,
#     session_id: str,
# ):
#     error_report = ErrorContainer()
#     for file_locator in incoming_files:
#         uuid = file_locator.meta["uuid"]
#         year = pl.discover_year(file_locator)
#         if year is None:
#             error_report.append(
#                 dict(
#                     type="MissingYear",
#                     message="Could not find a year in the filename or path",
#                     filename=file_locator.name,
#                     uuid=uuid,
#                 )
#             )
#             continue
#
#         la_code = pl.discover_la(file_locator)
#         if la_code is None:
#             error_report.append(
#                 dict(
#                     type="MissingLA",
#                     message="Could not find a local authority in the filename or path",
#                     filename=file_locator.name,
#                     uuid=uuid,
#                 )
#             )
#             continue
#
#         schema = load_schema(year)
#         schema_path = load_schema_path(year=year)
#         metadata = dict(year=year, schema=schema, la_code=la_code)
#
#
#         try:
#             cleanfile_result = task_cleanfile(file_locator, schema, schema_path)
#         except Exception as e:
#             error_report.append(
#                 dict(
#                     type="StreamError",
#                     message="Failed to clean file. Check log files for technical errors.",
#                     filename=file_locator.name,
#                     uuid=uuid,
#                 )
#             )
#             continue
#
#
#         # Clean result
#         cleanfile_result.data.export(
#             session_folder.opendir(SessionNames.CLEANED_FOLDER), file_locator.meta["uuid"] + "_", "parquet"
#         )
#         error_report.extend(cleanfile_result.errors)
#
#         # Enrich result
#         enrich_result = enrich_data(cleanfile_result.data, pipeline_config_cin(), metadata)
#         enrich_result.data.export(
#             session_folder.opendir(SessionNames.ENRICHED_FOLDER), file_locator.meta["uuid"] + "_", "parquet"
#         )
#         error_report.extend(enrich_result.errors)
#
#         # Degrade result
#         degraded_result = degrade_data(enrich_result.data, pipeline_config_cin(), metadata)
#         degraded_result.data.export(
#             session_folder.opendir(SessionNames.DEGRADED_FOLDER), file_locator.meta["uuid"] + "_", "parquet"
#         )
#         error_report.extend(degraded_result.errors)
#         archive.add(degraded_result.data)
#
#
#         error_report.set_property("filename", file_locator.name)
#         error_report.set_property("uuid", uuid)
#
#     error_report.set_property("session_id", session_id)
#     with session_folder.open("error_report.csv", "w") as FILE:
#         error_report.to_dataframe().to_csv(FILE, index=False)
#
#
# @op(
#     ins={"archive": In(DataframeArchive), "start": In(Nothing)},
#     out={"current_data": Out(DataContainer)},
# )
# def create_current_view_cin(archive: DataframeArchive):
#     archive.rollup()
#     current_folder = process_folder_cin().makedirs("current", recreate=True)
#     current_data = archive.current()
#
#     # Write archive
#     current_data.export(current_folder, "cin_test_", "csv")
#
#     return current_data
#
#
# @op(ins={"current_data": In(DataContainer)})
# def create_reports_cin(current_data: DataContainer):
#     export_folder = process_folder_cin().makedirs("export", recreate=True)
#
#     for report in ["PAN"]:
#         report_folder = export_folder.makedirs(report, recreate=True)
#         report = prepare_export(current_data, pipeline_config_cin())
#         report.data.export(report_folder, "cin_test_", "csv")
