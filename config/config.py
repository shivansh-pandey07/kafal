"""
Scoring constants — single source of truth.

Every value here is the agreed scoring logic
. Nothing in this file may be
changed without a documented decision, because scoring
depends on these exact numbers. Both the backfill and the real-time
worker import from here, so the two paths cannot drift.
"""

# Logic version — bump this on any approved change to weights, bands,
# thresholds, or aggregation. Stamped onto every scored row.
LOGIC_VERSION = "1.2.0"

# ── Short-trip buffer (Stage 1) ──────────────────────────────────────────────
# Trips shorter than this get a +/- buffer on the join window so they capture
# at least a couple of telemetry rows.
SHORT_TRIP_SECONDS = 120
SHORT_TRIP_BUFFER_SECONDS = 60

# ── PHYSICAL LIMITS (Stage 1) — sensor-fault filter, WIDE range ──────────────
# Values outside these are physically impossible -> dropped before aggregation.
# These are data-validity checks, NOT scoring thresholds. They are deliberately
# WIDER than the healthy bands below, so that physically-valid-but-unhealthy
# readings survive the filter and can be penalised by the bands.
PHYSICAL_LIMITS = {
    "speed":     (0,     35),     # tractor speed ceiling (governed)
    "rpm":       (0,     2500),   # tractor RPM ceiling
    "cool_temp": (35,    120),    # coolant fault envelope: readings above 120C are sensor faults, removed before scoring
    "bvl":       (12,    16),     # 12V system fault envelope
    "acc_x":     (-3,    3),
    "acc_y":     (-3,    3),
    "acc_z":     (-3,    3),
    "g_force":   (0,     1.5),    # per-sample magnitude envelope
}

# ── SCORING BANDS (Stage 1) — "healthy" operating range, NARROW ──────────────
# Values OUTSIDE the band drive the over/under/oor_pct penalties. Each band sits
# strictly inside its physical envelope above.
COOL_TEMP_LO, COOL_TEMP_HI = 60.0, 105.0   # healthy coolant operating range (C)
BVL_LO,       BVL_HI       = 13.5, 15.0    # healthy 12V system range

# ── Engine idle ──────────────────────────────────────────────────────────────
ENG_IDLE_MAX_PCT = 100.0   # idle > 100% of trip duration = counter artifact

# ── Alert handling (Stage 2) ─────────────────────────────────────────────────
# NOTE: data uses 'INFORMATION', not 'INFO' — both mapped to be safe.
SEVERITY_MAP  = {"CRITICAL": 100, "WARNING": 50, "INFORMATION": 10, "INFO": 10}
SEVERITY_RANK = {"CRITICAL": 3,   "WARNING": 2,  "INFORMATION": 1,  "INFO": 1}
ALERT_COUNT_CAP = 50   # cap sensor-spam alert counts

# ── OUTER WEIGHTS (Stage 2) — across domains, must sum to 1.0 ────────────────
OUTER_WEIGHTS = {
    "speed":     0.20,
    "rpm":       0.10,
    "cool_temp": 0.12,
    "bvl":       0.08,
    "g_force":   0.25,
    "eng_idle":  0.10,
    "alert":     0.15,
}

# ── INNER WEIGHTS (Stage 2) — within each domain ─────────────────────────────
INNER_WEIGHTS = {
    "speed": {
        "speed_p95": 0.50,
        "speed_max": 0.30,
        "speed_std": 0.20,
    },
    "rpm": {
        "rpm_p95": 0.55,
        "rpm_max": 0.45,
    },
    "cool_temp": {
        "cool_temp_oor_pct": 0.40,
        "cool_temp_over":    0.35,
        "cool_temp_under":   0.25,
    },
    "bvl": {
        "bvl_oor_pct": 0.40,
        "bvl_over":    0.30,
        "bvl_under":   0.30,
    },
    "g_force": {
        "g_force_p95": 0.50,
        "g_force_max": 0.30,
        "g_force_std": 0.20,
    },
    "eng_idle": {
        "eng_idle_pct": 1.00,
    },
    "alert": {
        "alert_lvl_encoded": 0.50,
        "alert_critical":    0.30,
        "alert_count":       0.20,
    },
}

# Indicator column per group — must be non-null for the group to score at all.
# (over/under default to 0.0, so they can't be used as presence indicators.)
GROUP_INDICATOR = {
    "speed":     "speed_max",
    "rpm":       "rpm_max",
    "cool_temp": "cool_temp_oor_pct",
    "bvl":       "bvl_oor_pct",
    "g_force":   "g_force_max",
    "eng_idle":  "eng_idle_pct",
    "alert":     "alert_lvl_max",
}

GROUP_TO_RISK = {
    "speed":     "risk_speed",
    "rpm":       "risk_rpm",
    "cool_temp": "risk_cool",
    "bvl":       "risk_bvl",
    "g_force":   "risk_g_force",
    "eng_idle":  "risk_eng_idle",
    "alert":     "risk_alert",
}

# Telemetry domains that count toward the coverage gate (alert is excluded —
# "0 alerts" is not a measured telemetry source).
TELEMETRY_GROUPS = {"speed", "rpm", "cool_temp", "bvl", "g_force", "eng_idle"}
MIN_DATA_SOURCES = 5   # raised from 3 after the coverage-bias analysis

# Columns that need frozen normalisation bounds (derived from inner weights).
SCORE_COLS = sorted({col for grp in INNER_WEIGHTS.values() for col in grp.keys()})

# ── Non-trip filter (Stage 2) ────────────────────────────────────────────────
NON_TRIP_MIN_SPEED_KMH = 2.0
NON_TRIP_MIN_DISTANCE_M = 10
NON_TRIP_SPEED_MULTIPLIER = 2   # required_avg_speed > speed_max * 2 => impossible

# ── Sensor-health flags (periodic per-UID job, NOT per-trip) ─────────────────
UNHEALTHY_SENSOR_THRESHOLD = 0.50
SCORING_THRESHOLD = 5  # n_telem_sources required to be scored
