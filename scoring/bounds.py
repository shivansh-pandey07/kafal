"""
Frozen normalisation bounds (P5/P95).

Transcribed from Scoring.ipynb cell 5, generalised so the file path is a caller
argument (the recalibrate job writes it, the worker/backfill read it). Adds a
bounds_version (sha1 of the bounds payload) so every scored row records exactly
which yardstick produced it — load_frozen_bounds returns (bounds, version).
"""
import hashlib
import json
import os

import numpy as np

from config.config import SCORE_COLS


def _version_of(payload_bounds):
    blob = json.dumps(payload_bounds, sort_keys=True).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()[:12]


def compute_and_freeze_bounds(df, bounds_file, score_cols=None):
    """
    Run ONCE on a reference window. Computes P5/P95 for every scoring column from
    the feature DataFrame, writes them to `bounds_file`, returns the bounds dict
    ({col: (p5, p95)}). Caller is responsible for deciding to overwrite (the
    recalibrate job removes any existing file first — a deliberate recalibration).
    """
    cols = score_cols or SCORE_COLS
    # Zero-inflated magnitude columns: most trips are 0 (in-band), a minority have
    # large positive values. Plain P95 lands in the zero mass and collapses to the
    # 1e-06 fallback, throwing away real signal. For these, anchor the upper bound
    # on the P95 of the NON-ZERO values so the normalisation range reflects the
    # actual spread of out-of-band events.
    ZERO_INFLATED = {c for c in cols if c.endswith("_over") or c.endswith("_under")}

    bounds = {}
    for col in cols:
        valid = df[col].dropna() if col in df.columns else None
        if valid is None or len(valid) == 0:
            bounds[col] = [0, 1]
            print(f"  {col}: no data -> [0, 1]")
            continue

        if col in ZERO_INFLATED:
            arr = np.asarray(valid, dtype=float)
            nonzero = arr[arr > 0]
            frac_nonzero = len(nonzero) / len(arr)
            if len(nonzero) == 0:
                # Genuinely always in-band: leave collapsed (no signal to scale).
                p5, p95 = 0.0, 1e-6
            else:
                p5 = 0.0  # in-band trips are the floor
                p95 = float(np.percentile(nonzero, 95))
                if p95 <= p5:
                    p95 = p5 + 1e-6
            print(f"  {col}: {frac_nonzero*100:.1f}% non-zero -> [{p5}, {p95}]")
        else:
            p5 = float(np.percentile(valid, 5))
            p95 = float(np.percentile(valid, 95))
            if p95 == p5:
                p95 = p5 + 1e-6
            print(f"  {col}: [{p5}, {p95}]")

        bounds[col] = [p5, p95]

    computed_on = str(df["date"].max()) if "date" in df.columns else None
    payload = {
        "computed_on": computed_on,
        "n_trips": int(len(df)),
        "bounds": bounds,
        "version": _version_of(bounds),
    }
    with open(bounds_file, "w") as fh:
        json.dump(payload, fh, indent=2)

    return {col: tuple(v) for col, v in bounds.items()}


def load_frozen_bounds(bounds_file):
    """
    Production path — load frozen bounds from disk. Returns (bounds, version)
    where bounds is {col: (p5, p95)}. Raises if the file is absent.
    """
    if not os.path.exists(bounds_file):
        raise FileNotFoundError(
            f"{bounds_file} not found. Run compute_and_freeze_bounds() once first."
        )
    with open(bounds_file) as fh:
        data = json.load(fh)
    bounds = {col: tuple(v) for col, v in data["bounds"].items()}
    version = data.get("version") or _version_of(data["bounds"])
    return bounds, version
