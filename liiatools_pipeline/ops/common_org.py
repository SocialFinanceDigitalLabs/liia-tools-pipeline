from dagster import op, In, Out
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


@op()
def move_error_report():
    source_folder = incoming_folder().opendir("logs")
    destination_folder = shared_folder().makedirs("logs", recreate=True)
    pl.move_error_report(source_folder, destination_folder)


@op()
def move_current_and_concat_view(config: CleanConfig):
    current_folder = incoming_folder().opendir("current")
    destination_folder = shared_folder()

    existing_files = destination_folder.listdir("/")

    authority_regex = "|".join(authorities.codes)
    current_files_regex = f"({authority_regex})_\d{{4}}"
    pl.remove_files(current_files_regex, existing_files, destination_folder)
    pl.move_files_for_sharing(current_folder, destination_folder)

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
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesOrg
    )
    session_folder = session_folder.opendir(SessionNamesOrg.INCOMING_FOLDER)

    concat_folder = incoming_folder().opendir(f"concatenated/{config.dataset}")
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
    export_folder = workspace_folder().makedirs(
        f"current/{config.dataset}", recreate=True
    )
    aggregate = DataframeAggregator(session_folder, pipeline_config(config))
    aggregate_data = aggregate.current()

    for report in pipeline_config(config).retention_period.keys():
        report_folder = export_folder.makedirs(report, recreate=True)
        report_data = prepare_export(
            aggregate_data, pipeline_config(config), profile=report
        )
        report_data = apply_retention(
            report_data,
            pipeline_config(config),
            profile=report,
            year_column=pipeline_config(config).retention_columns["year_column"],
            la_column=pipeline_config(config).retention_columns["la_column"],
        )

        existing_report_files = report_folder.listdir("/")
        pl.remove_files(f"{config.dataset}", existing_report_files, report_folder)
        report_data.export(report_folder, f"{config.dataset}_", "csv")

        existing_shared_files = shared_folder().listdir("/")
        pl.remove_files(f"{report}", existing_shared_files, shared_folder())
        report_data.export(shared_folder(), f"{report}_{config.dataset}_", "csv")
