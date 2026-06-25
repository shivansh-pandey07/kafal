"""
Airflow DAG — sensor-health rebuild (daily).

Runs the per-UID sensor-health sweep (sensor_health_job.run) once a day. This
can't live in the per-trip worker because it needs history across all of a
vehicle's trips to tell an intermittent sensor problem (maintenance) apart from
a permanent coverage gap (hardware/config). Populates uid_sensor_health.

Requires the repo on PYTHONPATH and the same ClickHouse env as the worker.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def rebuild_sensor_health(**_):
    from scoring.sensor_health_job import run
    return run()


default_args = {
    "owner": "fleet-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="trip_score_sensor_health",
    description="Daily rebuild of per-vehicle sensor-health flags.",
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    start_date=datetime(2025, 11, 15),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["trip_score", "sensor-health"],
) as dag:
    PythonOperator(
        task_id="rebuild_sensor_health",
        python_callable=rebuild_sensor_health,
    )
