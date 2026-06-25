"""
Stage 1 — Aggregate.

Transcribed verbatim (logic-identical) from RAW_DATASET_SCORE.ipynb. Takes one
trip's raw telemetry window and produces one summary feature row. The notebook
did this for a whole DataFrame; here it is one trip at a time so the live worker,
the backfill, and the recalibrate job all call the SAME code path.

Bug fixes preserved from the notebook:
  - Bug 2 : physical limits (wide) are separate from scoring bands (narrow), so
            over/under and oor_pct are non-zero for unhealthy-but-valid values.
  - Bug 5a: acc_*_p95_abs for axis consistency.
  - Bug 5b: g-force aggregated from PER-SAMPLE magnitude sqrt(ax^2+ay^2+az^2),
            never from per-axis aggregates.
"""
import numpy as np

from config.config import (
    PHYSICAL_LIMITS,
    COOL_TEMP_LO, COOL_TEMP_HI,
    BVL_LO, BVL_HI,
    ENG_IDLE_MAX_PCT,
    SEVERITY_RANK,
)


# ── g-force magnitude (per-sample) — Bug 5b ─────────────────────────────────
def gforce_magnitude(ax, ay, az):
    """sqrt(ax^2 + ay^2 + az^2) for a single sample. None/NaN axes treated as 0,
    matching the notebook's chunk['acc_*'].fillna(0) before the sqrt."""
    def z(v):
        return 0.0 if v is None or (isinstance(v, float) and np.isnan(v)) else float(v)
    return float(np.sqrt(z(ax) ** 2 + z(ay) ** 2 + z(az) ** 2))


# ── Step 1A: physical-limit filter (wide, data-validity) ────────────────────
def apply_physical_limits(values, col):
    lo, hi = PHYSICAL_LIMITS.get(col, (-np.inf, np.inf))
    return [v for v in values
            if v is not None
            and not (isinstance(v, float) and np.isnan(v))
            and lo <= v <= hi]


# ── Step 1B: MAD outlier cleaning ───────────────────────────────────────────
def mad_clean(values):
    if len(values) < 4:
        return values
    arr = np.array(values, dtype=float)
    median = np.median(arr)
    mad = np.median(np.abs(arr - median))
    if mad == 0:
        return values
    threshold = 3 * 1.4826 * mad
    return arr[np.abs(arr - median) <= threshold].tolist()


# ── Step 2A: one-sided stats (speed, rpm) ───────────────────────────────────
def one_sided_stats(values, col, prefix):
    values = apply_physical_limits(values, col)
    values = mad_clean(values)
    if not values:
        return {f'{prefix}_min': np.nan, f'{prefix}_max': np.nan,
                f'{prefix}_mean': np.nan, f'{prefix}_std': np.nan,
                f'{prefix}_p95': np.nan}
    arr = np.array(values, dtype=float)
    return {
        f'{prefix}_min':  float(np.min(arr)),
        f'{prefix}_max':  float(np.max(arr)),
        f'{prefix}_mean': float(np.mean(arr)),
        f'{prefix}_std':  float(np.std(arr)),
        f'{prefix}_p95':  float(np.percentile(arr, 95)),
    }


# ── Step 2B: band-bounded stats (cool_temp, bvl) — Bug 2 ────────────────────
def band_stats(values, col, prefix, lo, hi):
    values = apply_physical_limits(values, col)   # wide physical filter
    values = mad_clean(values)
    if not values:
        return {f'{prefix}_min': np.nan, f'{prefix}_max': np.nan,
                f'{prefix}_mean': np.nan, f'{prefix}_std': np.nan,
                f'{prefix}_oor_pct': np.nan,
                f'{prefix}_over': 0.0, f'{prefix}_under': 0.0}
    arr = np.array(values, dtype=float)
    over_mask = arr > hi      # narrow scoring band
    under_mask = arr < lo
    return {
        f'{prefix}_min':     float(np.min(arr)),
        f'{prefix}_max':     float(np.max(arr)),
        f'{prefix}_mean':    float(np.mean(arr)),
        f'{prefix}_std':     float(np.std(arr)),
        f'{prefix}_oor_pct': float((over_mask | under_mask).mean()),
        f'{prefix}_over':    float((arr[over_mask] - hi).sum()) if over_mask.any() else 0.0,
        f'{prefix}_under':   float((lo - arr[under_mask]).sum()) if under_mask.any() else 0.0,
    }


# ── Step 2C: accelerometer stats (no MAD — keep harsh events) — Bug 5a ──────
def acc_stats(values, col, prefix):
    values = apply_physical_limits(values, col)
    if not values:
        return {f'{prefix}_min': np.nan, f'{prefix}_max': np.nan,
                f'{prefix}_mean': np.nan, f'{prefix}_std': np.nan,
                f'{prefix}_p95_abs': np.nan}
    arr = np.array(values, dtype=float)
    return {
        f'{prefix}_min':     float(np.min(arr)),
        f'{prefix}_max':     float(np.max(arr)),
        f'{prefix}_mean':    float(np.mean(arr)),
        f'{prefix}_std':     float(np.std(arr)),
        f'{prefix}_p95_abs': float(np.percentile(np.abs(arr), 95)),
    }


# ── Step 2C2: g-force magnitude stats — Bug 5b ──────────────────────────────
def gforce_stats(values, prefix='g_force'):
    values = apply_physical_limits(values, 'g_force')
    if not values:
        return {f'{prefix}_min': np.nan, f'{prefix}_max': np.nan,
                f'{prefix}_mean': np.nan, f'{prefix}_std': np.nan,
                f'{prefix}_p95': np.nan}
    arr = np.array(values, dtype=float)
    return {
        f'{prefix}_min':  float(np.min(arr)),
        f'{prefix}_max':  float(np.max(arr)),
        f'{prefix}_mean': float(np.mean(arr)),
        f'{prefix}_std':  float(np.std(arr)),
        f'{prefix}_p95':  float(np.percentile(arr, 95)),
    }


# ── Step 2D: eng_idle (cumulative counter — delta method) ───────────────────
def eng_idle_stats(values, duration_s):
    values = [v for v in values
              if v is not None and not (isinstance(v, float) and np.isnan(v))]
    if len(values) < 2 or duration_s is None or duration_s <= 0:
        return {'eng_idle_seconds': np.nan, 'eng_idle_pct': np.nan, 'eng_idle_over': 0.0}
    idle_sec = values[-1] - values[0]
    if idle_sec < 0:
        idle_sec = max(values) - min(values)
    pct = (idle_sec / duration_s) * 100
    over = max(0.0, pct - ENG_IDLE_MAX_PCT)
    return {
        'eng_idle_seconds': float(idle_sec),
        'eng_idle_pct':     float(pct),
        'eng_idle_over':    float(over),
    }


# ── Step 2E: alert aggregation ──────────────────────────────────────────────
def alert_stats(msg_list, lvl_list):
    lvl_list = [l for l in lvl_list if l and l != 'NAN']
    msg_list = [m for m in msg_list if m and m != 'NAN' and m != 'nan']
    if not lvl_list:
        return {'alert_count': 0, 'alert_critical': 0, 'alert_warning': 0,
                'alert_lvl_max': None, 'alert_msg_mode': None}
    return {
        'alert_count':    len(lvl_list),
        'alert_critical': sum(1 for l in lvl_list if l == 'CRITICAL'),
        'alert_warning':  sum(1 for l in lvl_list if l == 'WARNING'),
        'alert_lvl_max':  max(lvl_list, key=lambda l: SEVERITY_RANK.get(l, 0)),
        'alert_msg_mode': max(set(msg_list), key=msg_list.count) if msg_list else None,
    }


def build_trip_features(trip, window):
    """
    Stage 1 for ONE trip. `window` holds the trip's raw telemetry lists (already
    time-windowed by the caller), with keys:
        speed, rpm, cool_temp, eng_idle,
        bvl, acc_x, acc_y, acc_z, g_force_mag,
        alert_lvl, alert_msg
    Returns one flat feature dict (the per-trip equivalent of one notebook row),
    including the trip base fields needed downstream and for bounds.
    """
    duration = trip.get("duration")

    feats = {
        "trip_id":         trip.get("id"),
        "uid":        trip.get("uid"),
        "date":       trip.get("date"),
        "start_time": trip.get("start_time"),
        "end_time":   trip.get("end_time"),
        "duration":   duration,
        "distance":   trip.get("distance"),
    }

    # Vehicle telemetry
    feats.update(one_sided_stats(window.get("speed", []),     "speed",     "speed"))
    feats.update(one_sided_stats(window.get("rpm", []),       "rpm",       "rpm"))
    feats.update(band_stats(window.get("cool_temp", []),      "cool_temp", "cool_temp",
                            COOL_TEMP_LO, COOL_TEMP_HI))
    feats.update(eng_idle_stats(window.get("eng_idle", []),   duration))

    # Health telemetry
    feats.update(band_stats(window.get("bvl", []), "bvl", "bvl", BVL_LO, BVL_HI))
    feats.update(acc_stats(window.get("acc_x", []), "acc_x", "acc_x"))
    feats.update(acc_stats(window.get("acc_y", []), "acc_y", "acc_y"))
    feats.update(acc_stats(window.get("acc_z", []), "acc_z", "acc_z"))
    feats.update(gforce_stats(window.get("g_force_mag", [])))

    # Alerts
    feats.update(alert_stats(window.get("alert_msg", []), window.get("alert_lvl", [])))

    return feats
