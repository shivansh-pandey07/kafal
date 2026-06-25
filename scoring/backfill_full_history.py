"""
History backfill — aggregate + score every trip from RAW telemetry.

One-time job to score all trips from BACKFILL_START forward. It loops the worker's
own score_one_trip over each completed trip, so the result is identical to what the
live worker produces going forward. Idempotent (ReplacingMergeTree), checkpointed
by date so it can resume after interruption.

Env: CLICKHOUSE_*  + BOUNDS_FILE (+ BOUNDS_S3_URI)  +  BACKFILL_START (date).
"""
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")
from .bounds import load_frozen_bounds
from .scoring_worker import (
    get_clickhouse, fetch_bounds_file, score_one_trip, upsert_trip_score,
)

# Earliest date raw telemetry exists in ClickHouse. Set to your true day-one.
BACKFILL_START = os.environ.get("BACKFILL_START", "2025-11-15")
CHECKPOINT = os.environ.get("CHECKPOINT_FILE", "backfill_checkpoint.txt")


def _read_checkpoint(default):
    try:
        return datetime.strptime(open(CHECKPOINT).read().strip(), "%Y-%m-%d").date()
    except (OSError, ValueError):
        return datetime.strptime(default, "%Y-%m-%d").date()


def _write_checkpoint(d):
    with open(CHECKPOINT, "w") as f:
        f.write(d.isoformat())


def trips_on_day(ch, day):
    rows = ch.query(
        """
        SELECT id, uid, date, start_time, end_time, duration, distance
        FROM trip_telemetry
        WHERE status = 'COMPLETED' AND date = {d:Date}
          AND end_time > start_time
          AND dateDiff('hour', start_time, end_time) <= 24
        """,
        parameters={"d": day.isoformat()},
    ).result_rows
    cols = ["id", "uid", "date", "start_time", "end_time", "duration", "distance"]
    return [dict(zip(cols, r)) for r in rows]


def backfill_data():
    ch = get_clickhouse()
    bounds, bounds_version = load_frozen_bounds(fetch_bounds_file())

    start = _read_checkpoint(BACKFILL_START)
    today = date.today()
    print(f"Backfilling from {start} to {today} (bounds {bounds_version})")

    day = start
    total = 0
    while day <= today:
        trips = trips_on_day(ch, day)
        for t in trips:
            t["uid"] = str(t["uid"]).strip().upper()
            try:
                scored = score_one_trip(ch, t, bounds, bounds_version)

                # Map the old 'id' key to the new 'trip_id' key
                if 'id' in scored:
                    scored['trip_id'] = scored.pop('id')
                elif 'trip_id' not in scored:
                    scored['trip_id'] = t.get('id')  # Fallback mapping directly from raw telemetry

                upsert_trip_score(ch, scored)
                total += 1
            except Exception as e:
                print(f"  ERROR scoring trip {t.get('id')} on {day}: {e}")

    print(f"\nDone. Re-scored {total:,} trips through the full-history backfill.")
    print("Verify in ClickHouse that cool_temp/bvl now have non-zero oor_pct (section 4 of the schema file).")



if __name__ == "__main__":
    backfill_data()
