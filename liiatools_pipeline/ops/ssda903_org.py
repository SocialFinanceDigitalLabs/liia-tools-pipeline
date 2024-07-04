from dagster import op

from liiatools.common.aggregate import DataframeAggregator
from liiatools.common.transform import prepare_export

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


@op
def create_reports():
    export_folder = workspace_folder().makedirs("current/ssda903", recreate=True)
    concat_folder = incoming_folder().opendir("concatenated")

    aggregate = DataframeAggregator(concat_folder, pipeline_config())
    aggregate_data = aggregate.current()

    for report in ["PAN", "SUFFICIENCY"]:
        report_folder = export_folder.makedirs(report, recreate=True)
        report = prepare_export(aggregate_data, pipeline_config(), profile=report)
        report.data.export(report_folder, "ssda903_", "csv")


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
