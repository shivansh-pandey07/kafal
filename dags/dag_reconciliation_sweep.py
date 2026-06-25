"""
Airflow DAG — reconciliation sweep (every 5 minutes).

Safety net for the live worker: if a trip_completed event was dropped, the worker
crashed mid-score, or telemetry arrived late, the trip ends up in trip_telemetry
(status COMPLETED) with no row in trip_score. This sweep finds those and scores
them with the SAME code path (score_one_trip) and the SAME frozen bounds, so a
swept trip is identical to a worker-scored one. In steady state it scores 0.

Requires the repo on PYTHONPATH and the same env as the worker (CLICKHOUSE_*,
BOUNDS_S3_URI / BOUNDS_FILE). Idempotent: re-scoring overwrites (newest scored_at
wins on the ReplacingMergeTree merge).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

LOOKBACK_HOURS = 6   # only chase recently-completed trips; backfill owns old gaps


def sweep_missed_trips(**_):
    from scoring.scoring_worker import (
        get_clickhouse, fetch_bounds_file, score_one_trip, upsert_trip_score,
    )
    from scoring.bounds import load_frozen_bounds

    ch = get_clickhouse()
    bounds, bounds_version = load_frozen_bounds(fetch_bounds_file())

    # COMPLETED trips in the lookback window with no scored row yet.
    rows = ch.query(
        """
        SELECT t.id, t.uid, t.date, t.start_time, t.end_time, t.duration, t.distance
        FROM trip_telemetry AS t
        LEFT ANTI JOIN trip_score AS s ON s.id = t.id
        WHERE t.status = 'COMPLETED'
          AND t.end_time >= now() - INTERVAL {h:UInt16} HOUR
        """,
        parameters={"h": LOOKBACK_HOURS},
    ).result_rows
    cols = ["id", "uid", "date", "start_time", "end_time", "duration", "distance"]

    scored_n = 0
    for r in rows:
        trip = dict(zip(cols, r))
        trip["uid"] = str(trip["uid"]).strip().upper()
        try:
            scored = score_one_trip(ch, trip, bounds, bounds_version)
            upsert_trip_score(ch, scored)
            scored_n += 1
        except Exception as e:  # noqa: BLE001
            print(f"  sweep error on trip {trip['id']}: {e}")

    print(f"(re)scored {scored_n} trips")
    return scored_n


default_args = {
    "owner": "fleet-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="trip_score_reconciliation_sweep",
    description="Re-score any trip the live worker missed (safety net).",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 11, 15),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["trip_score", "safety-net"],
) as dag:
    PythonOperator(
        task_id="sweep_missed_trips",
        python_callable=sweep_missed_trips,
    )
