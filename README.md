# Trip Score — Real-Time Pipeline

Scores every tractor trip on a 0–5 scale and stores it in a new ClickHouse table
`trip_score`. One shared logic library powers both the live worker (new trips) and
the backfill (history from 15 Nov 2025), so they score identically.

## Start here

- `DEPLOYMENT_GUIDE.md` — step-by-step deployment for the backend team.
- `BACKEND_TEAM_BRIEFING.md` — plain-language explanation of the architecture.

## Layout

```
trip_score_pipeline/
├── trip_score/                  # SHARED logic — worker, backfill, jobs all import this
│   ├── config.py                # constants (weights, bands, physical limits, thresholds)
│   ├── aggregate.py             # Stage 1 cleaning + aggregation
│   ├── features.py              # Stage 1: raw window -> one feature row (+ per-domain JSON)
│   ├── bounds.py                # frozen P5/P95 ranges: load / freeze
│   ├── scoring.py               # Stage 2: normalise -> domain risk -> 0–5 score
│   └── __init__.py
├── scoring_worker.py            # live worker: trip_completed -> score -> write
├── recalibrate_bounds.py        # one-time: freeze scoring ranges from raw, upload to S3
├── backfill_full_history.py     # one-time: score trips from 15 Nov 2025 into trip_score
├── sensor_health_job.py         # reconciliation sweep + per-vehicle health flags
├── dag_reconciliation_sweep.py  # Airflow: re-score missed trips (every 5 min)
├── dag_sensor_health.py         # Airflow: per-vehicle sensor flags (daily)
├── trip_score_schema.sql        # creates trip_score, uid_sensor_health, trip_with_score view
├── Dockerfile, requirements.txt # build the worker image
└── k8s/
    ├── trip-score-worker.yaml   # live worker (ServiceAccount, ConfigMap, Deployment)
    └── trip-score-jobs.yaml     # one-time recalibrate + backfill Jobs (same image)
```

## The two stages

- Stage 1 (aggregate): clean a trip's raw telemetry from the 4 tables and crunch it
  into one summary row.
- Stage 2 (score): compare each value to frozen ranges, weight the areas, output 0–5.

## The core idea

`trip_score/` is the single source of truth. The worker, the backfill, and the
safety-net jobs all import it, so there is only one copy of the scoring code and the
live path cannot drift.

## Deploy

Follow `DEPLOYMENT_GUIDE.md` (9 stages): create tables → build image → freeze ranges
→ backfill from 15 Nov 2025 → verify in-table → deploy worker → deploy Airflow jobs.
