from dagster import job

from liiatools_pipeline.ops.common_org import (
    dim_tables,
    ons_area,
    looked_after_child,
    ofsted_provider,
    postcode,
    episode,
    ofsted_inspection
)


@job()
def ssda903_sufficiency():
    dim = dim_tables()
    area = ons_area()
    lac = looked_after_child(area)
    prov = ofsted_provider(area)
    pc = postcode()
    episode(area, lac, pc, prov, dim)
    ofsted_inspection(dim, prov)
