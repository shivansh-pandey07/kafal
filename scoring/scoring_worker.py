"""
Real-time trip scoring worker (Pattern A).

Flow per trip:
  1. Consume a `trip_completed` event from Kafka.
  2. Wait out a grace window so late telemetry has landed in ClickHouse.
  3. Read the trip's telemetry window (vehicle / health / alert) from ClickHouse.
  4. Build features (Stage 1) and score (Stage 2) using the SHARED package.
  5. UPSERT the result into trip_analytics (ReplacingMergeTree).

This worker scores ONE trip per event. It does NOT compute per-UID sensor-health
flags — those need cross-trip history and are produced by the periodic job
(sensor_health_job.py).

Dependencies (install in the service image):
    pip install kafka-python clickhouse-connect pandas numpy

The connection details below are placeholders — wire them to your fleet config.
Credentials must come from your secret store / environment, never hard-coded.
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import clickhouse_connect
import requests
from kafka import KafkaConsumer

from dtos.dto import ScoreRequest
from .aggregate import build_trip_features
from .bounds import load_frozen_bounds
from scoring.scoring import score_feature_row, features_to_domain_json
from config.config import SHORT_TRIP_SECONDS, SHORT_TRIP_BUFFER_SECONDS, LOGIC_VERSION

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("trip_score_worker")

# ── Config (move to env / config service in production) ─────────────────────
KAFKA_BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP", "kafka-cp-kafka-headless.kafka:9092")
KAFKA_TOPIC       = os.environ.get("TRIP_COMPLETED_TOPIC", "trip_completed")
KAFKA_GROUP       = os.environ.get("KAFKA_GROUP", "trip-score-worker")
CLICKHOUSE_HOST   = os.environ.get("CLICKHOUSE_HOST", "nfwv00w215.ap-south-1.aws.clickhouse.cloud")
CLICKHOUSE_PORT   = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
CLICKHOUSE_DB     = os.environ.get("CLICKHOUSE_DB", "sixsense")
BOUNDS_FILE       = os.environ.get("BOUNDS_FILE", "score_bounds_reference.json")
# Optional: pull the frozen bounds from S3 at startup (recommended on ECS).
BOUNDS_S3_URI     = os.environ.get("BOUNDS_S3_URI", "https://asia-general-info.s3.ap-south-1.amazonaws.com/analytics/score_bounds_reference.json")   # e.g. s3://fleet-config/trip_score/score_bounds_reference.json
# Grace window: how long after end_time before the trip's last telemetry rows
# are reliably persisted in ClickHouse. Set above your p99 ingestion lag.
# Measured lag p50=1s, p99=2-3s -> 10s gives comfortable tail-latency margin.
GRACE_SECONDS = int(os.environ.get("SCORE_GRACE_SECONDS", "10"))


def _utcnow():
    """Naive UTC 'now' — matches the naive-UTC trip timestamps from _parse_dt."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _touch(path):
    """Write an integer epoch heartbeat for the k8s liveness probe to check."""
    try:
        with open(path, "w") as f:
            f.write(str(int(time.time())))
    except OSError:
        pass




def fetch_bounds_file():
    """Download the bounds file from a URL."""
    response = requests.get(BOUNDS_S3_URI, timeout=30)
    response.raise_for_status()

    Path(BOUNDS_FILE).write_bytes(response.content)

    log.info("Downloaded bounds file from %s", BOUNDS_S3_URI)
    return BOUNDS_FILE


def get_clickhouse():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB,
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "yNRtV8WpJSn5_"),
        secure=True,
    )


def fetch_trip_window(ch, uid, start_time, end_time, duration):
    """
    Pull all telemetry rows for one trip's (buffered) window from ClickHouse.
    Tables are assumed ORDER BY (uid, timestamp), so each query is a tight
    bounded range scan, not a full table read.
    """
    # Short-trip buffer (matches Stage 1)
    buf = SHORT_TRIP_BUFFER_SECONDS if (duration is not None and duration < SHORT_TRIP_SECONDS) else 0
    w_start = start_time - timedelta(seconds=buf)
    w_end = end_time + timedelta(seconds=buf)
    params = {"uid": uid, "ws": w_start, "we": w_end}

    veh = ch.query(
        "SELECT speed, rpm, cool_temp, eng_idle FROM vehicle_telemetry "
        "WHERE uid = {uid:String} AND timestamp BETWEEN {ws:DateTime} AND {we:DateTime} "
        "ORDER BY timestamp", parameters=params,
    ).result_rows

    health = ch.query(
        "SELECT bvl, acc_x, acc_y, acc_z FROM vehicle_health_telemetry "
        "WHERE uid = {uid:String} AND timestamp BETWEEN {ws:DateTime} AND {we:DateTime} "
        "ORDER BY timestamp", parameters=params,
    ).result_rows

    alert = ch.query(
        "SELECT lvl, msg FROM alert_telemetry "
        "WHERE uid = {uid:String} AND timestamp BETWEEN {ws:DateTime} AND {we:DateTime} "
        "ORDER BY timestamp", parameters=params,
    ).result_rows

    from .aggregate import gforce_magnitude
    window = {
        "speed":     [r[0] for r in veh],
        "rpm":       [r[1] for r in veh],
        "cool_temp": [r[2] for r in veh],
        "eng_idle":  [r[3] for r in veh],
        "bvl":       [r[0] for r in health],
        "acc_x":     [r[1] for r in health],
        "acc_y":     [r[2] for r in health],
        "acc_z":     [r[3] for r in health],
        "g_force_mag": [gforce_magnitude(r[1], r[2], r[3]) for r in health],
        "alert_lvl": [str(r[0]).strip().upper() for r in alert],
        "alert_msg": [str(r[1]).strip() for r in alert],
    }
    return window


def score_one_trip(ch, trip, bounds, bounds_version):
    window = fetch_trip_window(
        ch, trip["uid"], trip["start_time"], trip["end_time"], trip.get("duration")
    )
    features = build_trip_features(trip, window)
    scored = score_feature_row(features, bounds)
    scored.update(features_to_domain_json(scored))   # speed_json ... alert_json
    scored["duration"] = trip.get("duration")
    scored["distance"] = trip.get("distance")
    scored["logic_version"] = LOGIC_VERSION
    scored["bounds_version"] = bounds_version
    scored["scored_at"] = _utcnow()
    return scored


def upsert_trip_score(ch, scored):
    """
    Insert into the dedicated trip_score ReplacingMergeTree. Columns mirror the
    per-domain detail (7 JSON columns + score +
    coverage) plus the production essentials (score_status, versions, scored_at).
    A re-score is a safe idempotent overwrite (newest scored_at wins on merge).
    """
    cols = [
        "trip_id", "uid", "date", "start_time", "end_time", "duration", "distance",
        "trip_score", "trip_risk", "score_status",
        "n_telem_sources", "data_coverage_pct", "coverage_tier",
        "speed_json", "rpm_json", "cool_temp_json", "eng_idle_json",
        "bvl_json", "g_force_json", "alert_json",
        "logic_version", "bounds_version", "scored_at",
    ]
    row = [scored.get(c) for c in cols]
    ch.insert("trip_score", [row], column_names=cols)


def handle_event(ch, event: ScoreRequest, bounds, bounds_version):
    trip = {
        "id":         event.id,
        "uid":        str(event.uid).strip().upper(),
        "date":       event.date,
        "start_time": _parse_dt(event.startTime),
        "end_time":   _parse_dt(event.endTime),
        "duration":   event.duration,
        "distance":   event.distance,
    }
    # Grace window — let late telemetry settle before scoring
    target = trip["end_time"] + timedelta(seconds=GRACE_SECONDS)
    wait = (target - _utcnow()).total_seconds()
    if wait > 0:
        time.sleep(min(wait, GRACE_SECONDS))

    scored = score_one_trip(ch, trip, bounds, bounds_version)
    upsert_trip_score(ch, scored)
    log.info("Scored trip %s: %s (%s, %d/6 sources)",
             trip["id"], scored["trip_score"], scored["score_status"],
             scored["n_telem_sources"])


def _parse_dt(v):
    if isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v).replace("Z", "+00:00")).replace(tzinfo=None)


def calc_score(data: ScoreRequest):
    bounds_path = fetch_bounds_file()
    bounds, bounds_version = load_frozen_bounds(bounds_path)

    log.info(
        "Loaded frozen bounds (version %s), logic %s",
        bounds_version,
        LOGIC_VERSION,
    )

    ch = get_clickhouse()

    try:
        handle_event(
            ch,
            data,
            bounds,
            bounds_version,
        )
    except Exception as e:
        log.exception("Error processing event: %s", e)


