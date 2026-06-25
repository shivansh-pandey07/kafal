"""
Step A: compute and freeze the normalisation ranges (bounds).

Scoring normalises each value against frozen P5/P95 ranges, so a trip from any
date is scored on the same yardstick. This reads a representative reference window
of trips from ClickHouse, runs Stage 1 (build_trip_features) from raw telemetry to
get the per-trip features, freezes the P5/P95 ranges, and uploads them to S3.

Output: score_bounds_reference.json (uploaded to S3 if BOUNDS_S3_URI is set).

Run this ONCE, before the backfill. Env:
  CLICKHOUSE_HOST/PORT/DB, CLICKHOUSE_USER/PASSWORD
  REF_START / REF_END  (representative reference window)
  BOUNDS_FILE          (output path) + BOUNDS_S3_URI (upload target)
"""
import os
import sys

import pandas as pd

sys.path.insert(0, ".")
from .aggregate import build_trip_features
from .bounds import compute_and_freeze_bounds
from config.config import LOGIC_VERSION
from .scoring_worker import get_clickhouse, fetch_trip_window, _parse_dt

REF_START   = os.environ.get("REF_START", "2025-11-15")
REF_END     = os.environ.get("REF_END",   "2026-06-01")
BOUNDS_FILE = os.environ.get("BOUNDS_FILE", "score_bounds_reference_v2.json")


def fetch_reference_trips(ch):
    rows = ch.query(
        """
        SELECT id, uid, date, start_time, end_time, duration, distance
        FROM trip_telemetry
        WHERE status = 'COMPLETED' AND date >= {a:Date} AND date < {b:Date}
          AND end_time > start_time
          AND dateDiff('hour', start_time, end_time) <= 24
        """,
        parameters={"a": REF_START, "b": REF_END},
    ).result_rows
    cols = ["id", "uid", "date", "start_time", "end_time", "duration", "distance"]
    return [dict(zip(cols, r)) for r in rows]


def recalibrate_bounds():
    ch = get_clickhouse()
    trips = fetch_reference_trips(ch)
    print(f"Reference window {REF_START}..{REF_END}: {len(trips):,} trips")

    feats = []
    for i, t in enumerate(trips, 1):
        t["uid"] = str(t["uid"]).strip().upper()
        window = fetch_trip_window(ch, t["uid"], t["start_time"], t["end_time"], t.get("duration"))
        feats.append(build_trip_features(t, window))
        if i % 1000 == 0:
            print(f"  aggregated {i:,}/{len(trips):,}")

    df = pd.DataFrame(feats)
    if os.path.exists(BOUNDS_FILE):
        os.remove(BOUNDS_FILE)   # deliberate recalibration
    bounds = compute_and_freeze_bounds(df, BOUNDS_FILE)
    print(f"\nFroze {len(bounds)} bounds -> {BOUNDS_FILE}  (logic {LOGIC_VERSION})")
    print("cool_temp_over bounds:", bounds.get("cool_temp_over"))
    print("bvl_under bounds     :", bounds.get("bvl_under"))

    # Upload to S3 so the worker + backfill pick it up (the one source of truth).
    s3_uri = os.environ.get("BOUNDS_S3_URI")
    if s3_uri:
        import boto3
        bucket, key = s3_uri.replace("s3://", "").split("/", 1)
        boto3.client("s3").upload_file(BOUNDS_FILE, bucket, key)
        print(f"Uploaded bounds to {s3_uri}")
    else:
        print("\nBOUNDS_S3_URI not set — upload this file to S3 manually before the backfill.")



if __name__ == "__main__":
    recalibrate_bounds()
