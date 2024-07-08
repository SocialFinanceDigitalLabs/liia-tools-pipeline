from dagster import op, In, Out
from fs.base import FS

from liiatools.common.aggregate import DataframeAggregator
from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesOrg
from liiatools.common.transform import prepare_export

from liiatools_pipeline.assets.common import (
    pipeline_config,
    incoming_folder,
    workspace_folder,
    shared_folder,
    dataset,
)


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_org_session_folder() -> FS:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesOrg
    )
    session_folder = session_folder.opendir(SessionNamesOrg.INCOMING_FOLDER)

    concat_folder = incoming_folder().opendir("concatenated")
    pl.move_files_for_sharing(concat_folder, session_folder)

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def create_reports(
    session_folder: FS,
):
    export_folder = workspace_folder().makedirs(f"current/{dataset()}", recreate=True)
    aggregate = DataframeAggregator(session_folder, pipeline_config())
    aggregate_data = aggregate.current()

    for report in ["PAN", "SUFFICIENCY"]:
        report_folder = export_folder.makedirs(report, recreate=True)
        report_data = prepare_export(aggregate_data, pipeline_config(), profile=report)
        report_data.data.export(report_folder, f"{dataset()}", "csv")
        report_data.data.export(shared_folder(), f"{report}_{dataset()}_", "csv")
