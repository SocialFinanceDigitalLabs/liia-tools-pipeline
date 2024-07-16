from dagster import RunRequest, RunsFilter, DagsterRunStatus, sensor
from liiatools_pipeline.jobs.common_la import clean as ssda903_clean, move_current as ssda903_move_current
# from liiatools_pipeline.jobs.common_org import ssda903_sufficiency


@sensor(job=ssda903_move_current)
def ssda903_move_current_sensor(context):
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
