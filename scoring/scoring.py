"""
Stage 2 — Score.

Transcribed (logic-identical) from Scoring.ipynb. Operates on ONE feature row
(the dict from build_trip_features) instead of a DataFrame, so the live worker,
backfill, and recalibrate share the exact scoring path. Bounds are passed in
(frozen P5/P95), never recomputed here.

Preserves every notebook fix:
  - data-quality fixes : impossible eng_idle (pct>100) nulled, alert spam capped.
  - Bug 4 : 'INFORMATION' mapped (not just 'INFO').
  - Bug 3 : normalize_risk returns NaN for missing data; column_risk uses a
            per-group INDICATOR so zero-default columns can't fake "risk 0".
  - Fix 2A: MIN_DATA_SOURCES coverage gate (telemetry groups only).
  - non-trip filter : speed/distance/physical-possibility check -> score NaN.
  - Fix 2B: coverage metadata (n_telem_sources, data_coverage_pct, coverage_tier).
"""
import json
import math

import numpy as np

from config.config import (
    SEVERITY_MAP, ALERT_COUNT_CAP, ENG_IDLE_MAX_PCT,
    OUTER_WEIGHTS, INNER_WEIGHTS, GROUP_INDICATOR, GROUP_TO_RISK,
    TELEMETRY_GROUPS, MIN_DATA_SOURCES,
    NON_TRIP_MIN_SPEED_KMH, NON_TRIP_MIN_DISTANCE_M, NON_TRIP_SPEED_MULTIPLIER,
)


def _isna(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


# ── Data-quality fixes (notebook cell 2) ────────────────────────────────────
def _apply_quality_fixes(f):
    # Impossible idle (cumulative-counter artifact): pct > 100 -> null the group.
    pct = f.get("eng_idle_pct")
    if not _isna(pct) and pct > ENG_IDLE_MAX_PCT:
        f["eng_idle_seconds"] = np.nan
        f["eng_idle_pct"] = np.nan
        f["eng_idle_over"] = 0.0
    # Alert spam cap.
    ac = f.get("alert_count", 0) or 0
    if ac > ALERT_COUNT_CAP:
        f["alert_count"] = ALERT_COUNT_CAP
    return f


# ── Alert level encoding — Bug 4 ────────────────────────────────────────────
def _alert_lvl_encoded(f):
    lvl = f.get("alert_lvl_max")
    if _isna(lvl) or lvl is None:
        return 0.0
    return float(SEVERITY_MAP.get(str(lvl).strip().upper(), 0))


# ── normalize_risk — Bug 3 part 1 ───────────────────────────────────────────
def normalize_risk(value, col, bounds):
    if _isna(value):
        return np.nan
    p5, p95 = bounds.get(col, (0, 1))
    if p95 == p5:
        p95 = p5 + 1e-6
    return float(np.clip((value - p5) / (p95 - p5), 0, 1) * 100)


# ── column_risk — Bug 3 part 2 + missing-data guard ─────────────────────────
def column_risk(f, group, bounds):
    indicator = GROUP_INDICATOR.get(group)
    if indicator is not None and _isna(f.get(indicator)):
        return np.nan

    inner = INNER_WEIGHTS[group]
    total_weight = 0.0
    weighted_sum = 0.0
    for col, w in inner.items():
        # alert_lvl_encoded is derived, not a raw feature column
        val = f.get(col)
        if col == "alert_lvl_encoded":
            val = f.get("alert_lvl_encoded")
        if _isna(val):
            continue
        risk = normalize_risk(val, col, bounds)
        if _isna(risk):
            continue
        weighted_sum += w * risk
        total_weight += w

    if total_weight == 0:
        return np.nan
    return weighted_sum / total_weight


def _n_telem_sources(f):
    return (
        int(not _isna(f.get("speed_max"))) +
        int(not _isna(f.get("rpm_max"))) +
        int(not _isna(f.get("cool_temp_max"))) +
        int(not _isna(f.get("bvl_max"))) +
        int(not _isna(f.get("g_force_max"))) +
        int(not _isna(f.get("eng_idle_pct")))
    )


def _coverage_tier(n):
    if n <= 2:
        return "Minimal (<=2)"
    if n <= 4:
        return "Partial (3-4)"
    return "Full (5-6)"


def _is_non_trip(f):
    speed_max = f.get("speed_max")
    speed_max = 0.0 if _isna(speed_max) else float(speed_max)
    distance = f.get("distance")
    duration = f.get("duration")
    if _isna(distance) or distance < NON_TRIP_MIN_DISTANCE_M:
        return True
    if speed_max <= NON_TRIP_MIN_SPEED_KMH:
        return True
    if not _isna(duration) and duration > 0:
        required_avg_speed = distance * 3.6 / duration
        if required_avg_speed > speed_max * NON_TRIP_SPEED_MULTIPLIER:
            return True
    return False


def score_feature_row(features, bounds):
    """
    Stage 2 for one feature row. Returns the feature dict enriched with:
      trip_risk, trip_score, score_status,
      n_telem_sources, data_coverage_pct, coverage_tier,
      and the per-group risk_* columns.
    Mirrors the notebook exactly: quality fixes -> alert encode -> column risks
    -> outer-weighted trip risk with coverage gate -> non-trip filter.
    """
    f = dict(features)
    f = _apply_quality_fixes(f)
    f["alert_lvl_encoded"] = _alert_lvl_encoded(f)

    # Per-group risks
    for group, risk_col in GROUP_TO_RISK.items():
        f[risk_col] = column_risk(f, group, bounds)

    # Coverage metadata (always computed, even if unscored)
    n_tel = _n_telem_sources(f)
    f["n_telem_sources"] = n_tel
    f["data_coverage_pct"] = round(n_tel / 6 * 100, 1)
    f["coverage_tier"] = _coverage_tier(n_tel)

    # Outer-weighted trip risk + coverage gate
    total_w = 0.0
    weighted = 0.0
    telem_present = 0
    for group, risk_col in GROUP_TO_RISK.items():
        risk = f.get(risk_col)
        if _isna(risk):
            continue
        w = OUTER_WEIGHTS[group]
        weighted += w * risk
        total_w += w
        if group in TELEMETRY_GROUPS:
            telem_present += 1

    if total_w == 0 or telem_present < MIN_DATA_SOURCES:
        trip_risk = np.nan
        trip_score = np.nan
    else:
        trip_risk = weighted / total_w
        trip_score = round((100 - trip_risk) / 100 * 5, 2)

    # Non-trip filter
    if not _isna(trip_score) and _is_non_trip(f):
        trip_risk = np.nan
        trip_score = np.nan

    f["trip_risk"] = None if _isna(trip_risk) else float(trip_risk)
    f["trip_score"] = None if _isna(trip_score) else float(trip_score)
    f["score_status"] = "scored" if f["trip_score"] is not None else "unscored"

    return f


# ── JSON domain columns (notebook cell 13) ──────────────────────────────────
def _safe(val):
    if val is None:
        return None
    try:
        if isinstance(val, float) and math.isnan(val):
            return None
        if isinstance(val, (int, np.integer)):
            return int(val)
        return round(float(val), 4)
    except Exception:
        return str(val) if val else None


def features_to_domain_json(scored):
    """Emit the 7 *_json columns the trip_score table stores."""
    r = scored
    out = {}
    out["speed_json"] = json.dumps({
        "min": _safe(r.get("speed_min")), "max": _safe(r.get("speed_max")),
        "mean": _safe(r.get("speed_mean")), "std": _safe(r.get("speed_std")),
        "p95": _safe(r.get("speed_p95")),
    })
    out["rpm_json"] = json.dumps({
        "min": _safe(r.get("rpm_min")), "max": _safe(r.get("rpm_max")),
        "mean": _safe(r.get("rpm_mean")), "std": _safe(r.get("rpm_std")),
        "p95": _safe(r.get("rpm_p95")),
    })
    out["cool_temp_json"] = json.dumps({
        "min": _safe(r.get("cool_temp_min")), "max": _safe(r.get("cool_temp_max")),
        "mean": _safe(r.get("cool_temp_mean")), "std": _safe(r.get("cool_temp_std")),
        "oor_pct": _safe(r.get("cool_temp_oor_pct")),
        "over": _safe(r.get("cool_temp_over")), "under": _safe(r.get("cool_temp_under")),
    })
    out["eng_idle_json"] = json.dumps({
        "seconds": _safe(r.get("eng_idle_seconds")), "pct": _safe(r.get("eng_idle_pct")),
        "over": _safe(r.get("eng_idle_over")),
    })
    out["bvl_json"] = json.dumps({
        "min": _safe(r.get("bvl_min")), "max": _safe(r.get("bvl_max")),
        "mean": _safe(r.get("bvl_mean")), "std": _safe(r.get("bvl_std")),
        "oor_pct": _safe(r.get("bvl_oor_pct")),
        "over": _safe(r.get("bvl_over")), "under": _safe(r.get("bvl_under")),
    })
    out["g_force_json"] = json.dumps({
        "min": _safe(r.get("g_force_min")), "max": _safe(r.get("g_force_max")),
        "mean": _safe(r.get("g_force_mean")), "std": _safe(r.get("g_force_std")),
        "p95": _safe(r.get("g_force_p95")),
    })
    lvl_max = r.get("alert_lvl_max")
    msg_mode = r.get("alert_msg_mode")
    out["alert_json"] = json.dumps({
        "count": _safe(r.get("alert_count")), "critical": _safe(r.get("alert_critical")),
        "warning": _safe(r.get("alert_warning")),
        "lvl_max": str(lvl_max) if not _isna(lvl_max) and lvl_max is not None else None,
        "msg_mode": str(msg_mode) if not _isna(msg_mode) and msg_mode is not None else None,
    })
    return out
