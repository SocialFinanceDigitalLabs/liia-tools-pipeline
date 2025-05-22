from dagster import schedule, DefaultScheduleStatus
from decouple import config as env_config

from liiatools_pipeline.jobs.common_la import no_op_job
from liiatools.annex_a_pipeline.spec import load_pipeline_config as load_pipeline_config_annex_a
from liiatools.cin_census_pipeline.spec import load_pipeline_config as load_pipeline_config_cin
from liiatools.pnw_census_pipeline.spec import load_pipeline_config as load_pipeline_config_pnw_census
from liiatools.ssda903_pipeline.spec import load_pipeline_config as load_pipeline_config_ssda903


@schedule(
    job=no_op_job,
    cron_schedule=env_config("CONFIG_SCHEDULE"),
    description="Runs based on CONFIG_SCHEDULE and prints pipeline configs",
    default_status=DefaultScheduleStatus.RUNNING,
)
def pipeline_config_schedule(context):
    allowed_datasets = env_config("ALLOWED_DATASETS").split(",")
    context.log.info(f"Allowed datasets: {allowed_datasets}")

    for dataset in allowed_datasets:
        context.log.info(globals()[f"load_pipeline_config_{dataset}"]())
