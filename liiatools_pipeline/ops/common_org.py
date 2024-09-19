from dagster import op, In, Out, get_dagster_logger
from fs.base import FS

from liiatools.common.aggregate import DataframeAggregator
from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesOrg
from liiatools.common.reference import authorities
from liiatools.common.transform import prepare_export, apply_retention
from liiatools_pipeline.ops.common_config import CleanConfig

from liiatools_pipeline.assets.common import (
    pipeline_config,
    incoming_folder,
    workspace_folder,
    shared_folder,
)

log = get_dagster_logger()

@op()
def move_error_report():
    source_folder = incoming_folder().opendir("logs")
    destination_folder = shared_folder().makedirs("logs", recreate=True)
    log.info("Moving Error Reports to destination...")
    pl.move_error_report(source_folder, destination_folder)


@op()
def move_current_and_concat_view(config: CleanConfig):
    current_folder = incoming_folder().opendir("current")
    destination_folder = shared_folder()
    
    existing_files = destination_folder.listdir("/")

    authority_regex = "|".join(authorities.codes)
    current_files_regex = f"({authority_regex})_\d{{4}}"
    pl.remove_files(current_files_regex, existing_files, destination_folder)

    log.info("Moving current files to destination...")
    pl.move_files_for_sharing(current_folder, destination_folder)

    log.info("Moving concat files to destination...")
    concat_folder = incoming_folder().opendir(f"concatenated/{config.dataset}")

    concat_files_regex = f"({authority_regex})_{config.dataset}"
    pl.remove_files(concat_files_regex, existing_files, destination_folder)
    pl.move_files_for_sharing(concat_folder, destination_folder)


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_org_session_folder(config: CleanConfig) -> FS:
    log.info("Creating session folder...")
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesOrg
    )
    log.info("Opening incoming folder...")
    session_folder = session_folder.opendir(SessionNamesOrg.INCOMING_FOLDER)

    log.info("Opening concatenated files...")
    concat_folder = incoming_folder().opendir(f"concatenated/{config.dataset}")

    log.info("Moving concat folder to session...")
    pl.move_files_for_sharing(concat_folder, session_folder)

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def create_reports(
    session_folder: FS,
    config: CleanConfig,
):
    log.info("Creating Export Directories...")
    export_folder = workspace_folder().makedirs(
        f"current/{config.dataset}", recreate=True
    )
    log.info("Aggregating Data Frames...")
    aggregate = DataframeAggregator(session_folder, pipeline_config(config))
    aggregate_data = aggregate.current()
    log.debug(f"Using config: {config}")
    for report in pipeline_config(config).retention_period.keys():
        log.info(f"Processing report {report}...")
        report_folder = export_folder.makedirs(report, recreate=True)
        report_data = prepare_export(
            aggregate_data, pipeline_config(config), profile=report
        )
        log.info(f"Applying retention to {report}...")
        report_data = apply_retention(
            report_data,
            pipeline_config(config),
            profile=report,
            year_column=pipeline_config(config).retention_columns["year_column"],
            la_column=pipeline_config(config).retention_columns["la_column"],
        )

        existing_report_files = report_folder.listdir("/")
        pl.remove_files(f"{config.dataset}", existing_report_files, report_folder)
        log.info(f"Exporting report {report} to report folder...")
        report_data.export(report_folder, f"{config.dataset}_", "csv")

        existing_shared_files = shared_folder().listdir("/")
        log.info(f"Exporting report {report} to shared folder...")
        pl.remove_files(f"{report}", existing_shared_files, shared_folder())
        report_data.export(shared_folder(), f"{report}_{config.dataset}_", "csv")
