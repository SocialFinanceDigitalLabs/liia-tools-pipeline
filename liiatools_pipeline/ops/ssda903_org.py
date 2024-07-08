from dagster import op, In, Out
from fs.base import FS

from liiatools.common.aggregate import DataframeAggregator
from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesOrg
from liiatools.common.transform import prepare_export, apply_retention

from liiatools_pipeline.assets.ssda903 import (
    pipeline_config,
    incoming_folder,
    workspace_folder,
    shared_folder,
)

from sufficiency_data_transform.all_dim_and_fact import (
    create_dim_tables,
    create_dimONSArea,
    create_dimLookedAfterChild,
    create_dimOfstedProvider,
    create_dimPostcode,
    create_factEpisode,
    create_factOfstedInspection,
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
    export_folder = workspace_folder().makedirs("current/ssda903", recreate=True)
    aggregate = DataframeAggregator(session_folder, pipeline_config())
    aggregate_data = aggregate.current()

    for report in ["PAN", "SUFFICIENCY"]:
        report_folder = export_folder.makedirs(report, recreate=True)
        report_data = prepare_export(aggregate_data, pipeline_config(), profile=report)
        report_data = apply_retention(
            report_data, pipeline_config(), profile=report, year_column="YEAR"
        )
        report_data.export(report_folder, "ssda903_", "csv")
        report_data.export(shared_folder(), f"{report}_ssda903_", "csv")


@op
def dim_tables():
    create_dim_tables()
    return True


@op
def ons_area():
    create_dimONSArea()
    return True


@op
def looked_after_child(area):
    create_dimLookedAfterChild()
    return True


@op
def ofsted_provider(area):
    create_dimOfstedProvider()
    return True


@op
def postcode():
    create_dimPostcode()
    return True


@op
def episode(area, lac, pc, prov, dim):
    create_factEpisode()


@op
def ofsted_inspection(dim, prov):
    create_factOfstedInspection()
