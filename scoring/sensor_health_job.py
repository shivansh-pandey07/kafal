"""
Sensor-health sweep (periodic, per-UID — NOT per-trip).

Transcribed from Scoring.ipynb cell 12. Needs history ACROSS trips, so it can't
live in the per-trip worker. Run daily (Airflow). For every vehicle it decides:

  - hardware_issue : the tractor NEVER reaches the scoring coverage threshold
                     (max_n_telem_sources < SCORING_THRESHOLD). A permanent state
                     -> escalate to fleet ops (likely a different sensor package
                     or device config), do NOT dispatch a technician.

  - maintenance    : the tractor DOES reach the threshold sometimes, but >= 50%
                     of its trips are low-coverage -> intermittent issue, dispatch
                     a technician.

Reads n_telem_sources straight off the trip_score table (the worker/backfill
already computed and stored it), aggregates per uid, and writes one row per uid
to uid_sensor_health.
"""
import os
from datetime import datetime, timezone

import clickhouse_connect

from config.config import UNHEALTHY_SENSOR_THRESHOLD, SCORING_THRESHOLD

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "fleet")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")


def get_clickhouse():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB,
        username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
    )


def compute_uid_health(ch):
    """
    One aggregate query over trip_score gives, per uid:
      total_trips, low_coverage_trips (n_telem < threshold), max_n_tel_ever.
    Returns a list of dicts with the two boolean flags applied.
    """
    rows = ch.query(
        """
        SELECT
            uid,
            count() AS total_trips,
            countIf(n_telem_sources < {thr:UInt8}) AS low_coverage_trips,
            max(n_telem_sources) AS max_n_tel_ever
        FROM trip_score FINAL
        GROUP BY uid
        """,
        parameters={"thr": SCORING_THRESHOLD},
    ).result_rows

    out = []
    for uid, total, low, max_n in rows:
        low_pct = (low / total) if total else 0.0
        hardware_issue = max_n < SCORING_THRESHOLD
        maintenance = (max_n >= SCORING_THRESHOLD) and (low_pct >= UNHEALTHY_SENSOR_THRESHOLD)
        out.append({
            "uid": uid,
            "total_trips": int(total),
            "low_coverage_trips": int(low),
            "low_coverage_pct": round(float(low_pct), 4),
            "max_n_tel_ever": int(max_n),
            "hardware_issue_flag": bool(hardware_issue),
            "maintenance_flag": bool(maintenance),
        })
    return out


def write_uid_health(ch, records):
    if not records:
        return 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cols = ["uid", "total_trips", "low_coverage_trips", "low_coverage_pct",
            "max_n_tel_ever", "hardware_issue_flag", "maintenance_flag",
            "computed_at"]
    data = [[r["uid"], r["total_trips"], r["low_coverage_trips"], r["low_coverage_pct"],
             r["max_n_tel_ever"], r["hardware_issue_flag"], r["maintenance_flag"],
             now] for r in records]
    ch.insert("uid_sensor_health", data, column_names=cols)
    return len(data)


def run():
    ch = get_clickhouse()
    records = compute_uid_health(ch)
    n = write_uid_health(ch, records)
    n_hw = sum(1 for r in records if r["hardware_issue_flag"])
    n_maint = sum(1 for r in records if r["maintenance_flag"])
    print(f"uid_sensor_health updated: {n} uids "
          f"({n_maint} maintenance, {n_hw} hardware/config)")
    return n


if __name__ == "__main__":
    run()
