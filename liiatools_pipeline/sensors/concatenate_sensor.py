from dagster import RunRequest, RunsFilter, DagsterRunStatus, sensor, DefaultSensorStatus
from liiatools_pipeline.jobs.common_la import move_current, concatenate


@sensor(
    job=concatenate,
    description="Runs concatenate job once move_current job is complete",
    default_status=DefaultSensorStatus.RUNNING,
)
def concatenate_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=move_current.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
    )

    if run_records:  # Ensure there is at least one run record
        latest_run_record = run_records[0]  # Get the most recent run record
        yield RunRequest(run_key=latest_run_record.dagster_run.run_id)

