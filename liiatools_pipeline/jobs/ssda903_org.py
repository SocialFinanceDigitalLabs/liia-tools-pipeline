from dagster import job

from liiatools_pipeline.ops import ssda903_org as ssda903

@job
def ssda903_sufficiency():
    ssda903.output_lookup_tables()
    ssda903.create_dim_fact_tables()