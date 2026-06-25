# Trip Score — Deployment Guide (for the backend team)

The full implementation, start to finish, for the team that controls Kafka and
ClickHouse. Each stage says **DO / WHY / HOW / CHECK** and names the exact file to
use. Follow the stages in order. When every CHECK passes, the `trip_score` table is
live in ClickHouse, holds every trip from 15 Nov 2025 onward, and updates itself
for every new trip.

This is a single deployment of the final logic — there is no multi-step migration.

---

## A. Plain-words glossary

- **Kafka topic** — a named stream of messages. We listen to `trip_completed`;
  one message arrives when a trip ends.
- **ClickHouse** — the database holding telemetry and where scores are written.
- **Image / ECR** — an "image" is the app packaged with its dependencies; ECR is
  Amazon's registry that stores it so the cluster can run it.
- **Pod / Deployment (EKS)** — a pod is one running copy; a Deployment keeps one
  pod alive and restarts it if it crashes.
- **Job (Kubernetes)** — a pod that runs once and stops. Used for the one-time
  range-freeze and backfill.
- **ConfigMap / Secret** — settings and passwords injected into the pod as
  environment variables.
- **IRSA** — how an EKS pod is granted AWS permissions (here: read/write one S3
  file). Attached to the pod's ServiceAccount.
- **S3** — Amazon file storage; holds one small "frozen ranges" file.

---

## B. The logic in brief (so each step makes sense)

Every trip gets a 0–5 score computed in two stages:

- **Stage 1 — Aggregate.** Take all of one trip's raw telemetry rows from the 4
  tables, clean them (drop impossible sensor readings, remove outliers), and crunch
  them into one summary row per trip (max speed, 95th-percentile g-force, % of time
  coolant or battery voltage is outside its healthy range, etc.).
- **Stage 2 — Score.** Compare each summary value to a frozen reference range,
  weight the areas (speed, rpm, coolant, battery, harsh movement, idling, alerts),
  and produce the 0–5 score plus a per-area breakdown.

The score is a whole-trip number, so it's computed once the trip ends.

---

## C. File map — what each file is and when it's used

| File | What it is | Stage |
|------|-----------|-------|
| `trip_score/` (package) | The shared scoring logic. Everything imports this — one copy, no drift. | Built into the image (3) |
| `trip_score_schema.sql` | Creates the `trip_score` + `uid_sensor_health` tables and the `trip_with_score` view. | 2 |
| `Dockerfile`, `requirements.txt` | Package the worker into a runnable image. | 3 |
| `recalibrate_bounds.py` | Reads raw telemetry, freezes the scoring ranges, uploads to S3. | 4 |
| `backfill_full_history.py` | Scores every trip from 15 Nov 2025 forward into `trip_score`. | 5 |
| `scoring_worker.py` | The always-on live worker: trip ends → score → write. | 7 |
| `k8s/trip-score-worker.yaml` | Kubernetes config for the live worker. | 7 |
| `k8s/trip-score-jobs.yaml` | Kubernetes Jobs to run range-freeze + backfill in-cluster (same image). | 4–5 |
| `sensor_health_job.py` | Functions for the safety-net sweep + per-vehicle health flags. | 8 |
| `dag_reconciliation_sweep.py` | Airflow job (every 5 min): re-scores anything missed. | 8 |
| `dag_sensor_health.py` | Airflow job (daily): rebuilds per-vehicle sensor flags. | 8 |

---

## D. Prerequisites — confirm/provide these first

1. The four telemetry tables are `ORDER BY (uid, timestamp)` (fast per-trip reads).
2. Raw telemetry still exists back to **15 Nov 2025** — check
   `SELECT min(timestamp) FROM vehicle_health_telemetry` (and the other two). If the
   oldest data is later than 15 Nov 2025, that later date is the real backfill start.
3. The `trip_completed` event includes `id, uid, start_time, end_time, duration,
   distance` (or we can look them up in `trip_telemetry`).
4. Telemetry timestamps are stored in **UTC**.
5. Access: an EKS namespace (`fleet`), an IRSA role with S3 read+write on the bounds
   file, the ClickHouse credentials as a Kubernetes Secret, the Kafka broker list +
   auth (is MSK using IAM?), and an S3 path for the ranges file
   (`s3://fleet-config/trip_score/score_bounds_reference.json`).
6. Agreement that the grace window is 10s (measured ingestion lag p99 ~2–3s).

---

## Stage 1 — Get the code

**DO:** Put the repo (the `trip_score/` package + all files) in version control.
**WHY:** Everything else builds from it. The `trip_score/` package is the single
copy of the scoring logic that the worker, the range-freeze, and the backfill all
import, so the live path can't drift from the agreed logic.
**HOW:** Commit the folder; `pip install -r requirements.txt` in any environment
that will run the scripts.
**CHECK:** The package imports cleanly (`python -c "import trip_score"`).

## Stage 2 — Create the tables in ClickHouse

**DO:** Run `trip_score_schema.sql`.
**WHY:** The scores need somewhere to live. We use a **new** table, not extra
columns on `trip_analytics`, because ClickHouse merges replace whole rows — two
writers on one table would erase each other's columns. The new table makes the
scoring service the only writer. The `trip_with_score` view joins it back to
`trip_analytics` so dashboards see one combined row.
**HOW:** `clickhouse-client < trip_score_schema.sql` (sections 1–3).
**CHECK:** `SHOW CREATE TABLE trip_score` shows engine `ReplacingMergeTree(scored_at)`,
`ORDER BY id`, and the per-domain JSON columns.

## Stage 3 — Build the worker image and push it to ECR

**DO:** Build the image from the `Dockerfile`, push to ECR.
**WHY:** One image runs the live worker AND the one-time jobs (Stages 4, 5, 7), so
all three use identical code.
**HOW:**
```bash
docker build -t trip-score-worker:1.0 .
aws ecr create-repository --repository-name trip-score-worker   # first time only
docker tag trip-score-worker:1.0 <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/trip-score-worker:1.0
docker push <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/trip-score-worker:1.0
```
**CHECK:** The image appears in ECR. (Use this same tag everywhere.)

## Stage 4 — Freeze the scoring ranges from your telemetry, upload to S3

**DO:** Run the range-freeze Job (`recalibrate_bounds.py`).
**WHY:** Scoring normalises each value against frozen P5/P95 ranges, so every trip
is judged on the same yardstick. We compute those ranges once from a representative
window of real telemetry and freeze them. The job uploads the result to S3, which
the backfill and the worker both read.
**HOW:** Fill the ConfigMap + Secret + IRSA, then apply the jobs manifest and watch
the range-freeze job:
```bash
kubectl apply -f k8s/trip-score-jobs.yaml
kubectl logs -f job/trip-score-recalibrate -n fleet
```
**CHECK:** Logs show "Froze N bounds" and "Uploaded bounds to s3://…"; the S3 file
exists.

## Stage 5 — Score trips from 15 Nov 2025 forward (backfill)

**DO:** Run the backfill Job (`backfill_full_history.py`). `BACKFILL_START` is set to
`2025-11-15` in the jobs manifest.
**WHY:** Fills the table with every trip from 15 Nov 2025 to now, computed fresh
from raw telemetry with the same code the live worker uses — so old and new trips
are scored identically. Checkpointed by date.
**HOW:** Created by the same `kubectl apply` in Stage 4. Watch it:
```bash
kubectl logs -f job/trip-score-backfill -n fleet
```
If the pod dies partway, note the last "scored" date in the logs, set
`BACKFILL_START` to that date, and re-apply — re-scoring is safe (idempotent).
**CHECK:** Job completes; logs show the running total of trips scored.

## Stage 6 — Verify in the table (the correctness gate)

**DO:** Run the verification query.
**WHY:** This is how we confirm the deployment is correct: the table is populated
and every scoring area is contributing (no area silently producing nothing).
**HOW:**
```sql
SELECT count() AS total,
       countIf(trip_score IS NOT NULL) AS scored,
       round(avg(trip_score), 2) AS mean_score,
       round(min(trip_score), 2) AS min_score,
       round(max(trip_score), 2) AS max_score,
       countIf(JSONExtractFloat(cool_temp_json,'oor_pct') > 0) AS cool_contributing,
       countIf(JSONExtractFloat(bvl_json,'oor_pct')       > 0) AS bvl_contributing
FROM trip_score FINAL;
```
**CHECK:** `total`/`scored` look right for trips since 15 Nov 2025; `mean_score` is
in a sensible 0–5 range; and `cool_contributing` / `bvl_contributing` are greater
than 0 (confirms every area, including coolant and battery, is feeding the score).

## Stage 7 — Deploy the live worker (new trips, forever forward)

**DO:** Deploy `k8s/trip-score-worker.yaml` with the image tag from Stage 3.
**WHY:** The always-on service that scores each new trip the moment its
`trip_completed` event fires, using the same ranges as the backfill.
**HOW:** Ensure the IRSA role (S3 read), the ClickHouse Secret, and the ConfigMap
(`KAFKA_BOOTSTRAP`, `CLICKHOUSE_HOST`, `BOUNDS_S3_URI`, `SCORE_GRACE_SECONDS=10`)
are set, then:
```bash
kubectl apply -f k8s/trip-score-worker.yaml
kubectl get pods -n fleet
kubectl logs -f deployment/trip-score-worker -n fleet
```
**CHECK:** Pod is Running; logs show "Downloaded bounds from s3://…", "Loaded frozen
bounds", and "Worker started, consuming trip_completed".

## Stage 8 — Deploy the safety-net jobs (Airflow)

**DO:** Add `dag_reconciliation_sweep.py` and `dag_sensor_health.py` to Airflow.
**WHY:** The sweep (every 5 min) re-scores any trip the worker missed, so nothing
is ever permanently unscored. The daily job rebuilds per-vehicle sensor-health
flags (these need history across trips, so they can't live in the per-trip worker).
**HOW:** Drop both files in the Airflow DAGs folder; ensure the Airflow workers have
the repo on PYTHONPATH, the requirements installed, and the same ClickHouse + bounds
env vars.
**CHECK:** The sweep DAG logs "(re)scored 0 trips" in steady state; the health DAG
populates `uid_sensor_health`.

## Stage 9 — Final end-to-end check and go-live

**DO:** Publish one test `trip_completed` event; watch the row appear.
**HOW:**
```sql
SELECT id, trip_score, score_status, scored_at
FROM trip_score FINAL ORDER BY scored_at DESC LIMIT 5;
```
**CHECK:** A new row appears within ~10s. Point dashboards at the `trip_with_score`
view. Set alerts on Kafka consumer lag, worker error rate, and rows-written-per-hour.

---

## E. Why the order is fixed (the recheck)

- Tables (2) must exist before anything writes to them (5, 7).
- The image (3) must exist before the Jobs and worker (4, 5, 7) — they all use it.
- The frozen ranges (4) must be in S3 before the backfill (5) and the worker (7) —
  both read them, and **both must use the same ranges** or old and new trips won't
  agree.
- The backfill (5) and worker (7) use the **same image and the same ranges** — so
  they score identically, by construction.

## F. Common mistakes to avoid (rechecked)

1. Source tables not sorted by `(uid, timestamp)` → slow reads. Confirm in §D.
2. Raw telemetry not retained back to 15 Nov 2025 → backfill can't reach it. Confirm
   the earliest date in §D and adjust `BACKFILL_START` if needed.
3. Running the backfill before the ranges are in S3 → wrong scores. Stage 4 before 5.
4. Reading `trip_score` without `FINAL` → you may see an old version of a re-scored
   row. Always read with `FINAL`.
5. Different image tags for the worker vs the backfill → drift. Use one tag everywhere.
6. Secrets baked into the image → use the Kubernetes Secret + IRSA instead.
7. Forgetting the grace window (`SCORE_GRACE_SECONDS=10`) → scoring on incomplete
   data. Keep it ≥ the p99 ingestion lag.
