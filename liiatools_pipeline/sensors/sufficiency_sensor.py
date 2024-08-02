from dagster import RunRequest, RunsFilter, DagsterRunStatus, sensor,  DefaultSensorStatus
from liiatools_pipeline.jobs.ssda903_la import ssda903_clean
from liiatools_pipeline.jobs.ssda903_org import ssda903_sufficiency


@sensor(
    job=ssda903_sufficiency,
    description="Completion of the 903 cleanfile job before running the sufficiency process for 903 Files",
    default_status=DefaultSensorStatus.RUNNING,
)
def sufficiency_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=ssda903_clean.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
    )
    for run_record in run_records:
        yield RunRequest(run_key=run_record.dagster_run.run_id)
