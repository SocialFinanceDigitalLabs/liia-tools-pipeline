from dagster import In, Out, op
from liiatools.common.archive import DataframeArchive
from liiatools.common.reference import authorities
from liiatools_pipeline.assets.common import (
    incoming_folder,
    workspace_folder,
    shared_folder,
    pipeline_config,
    la_code as input_la_code,
    dataset,
)


@op(
    ins={"current": In(DataframeArchive)},
)
def create_concatenated_view(current: DataframeArchive):
    concat_folder = shared_folder().makedirs(f"concatenated/{dataset()}", recreate=True)
    for la_code in authorities.codes:
        concat_data = current.current(la_code)

        if concat_data:
            concat_data.export(concat_folder, f"{la_code}_ssda903_", "csv")

