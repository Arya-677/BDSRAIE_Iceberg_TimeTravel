"""Exact retraining from a registry row, and the L1/L2/L3 comparison used as evidence."""
from __future__ import annotations

import json

import lightgbm as lgb
import numpy as np
from pyspark.sql import SparkSession

from . import registry
from .config import Config, names_sha256
from .features import build_matrix
from .frames import to_pandas
from .snapshots import read_at_snapshot, snapshot_exists
from .train import prepare_frame, train


class EvidenceLost(Exception):
    """The snapshot a model was trained on no longer exists (expired)."""


def settings_from_row(row) -> dict:
    p = json.loads(row["params_json"])
    if names_sha256(p["features"]) != row["feature_list_sha256"]:
        raise ValueError("registry row is internally inconsistent: feature list does not match feature_list_sha256")
    return {"params": p["lgbm"], "features": p["features"], "train_end": p["train_end"],
            "seed": p["seed"], "data_filter": row["data_filter"]}


def retrain(spark: SparkSession, cfg: Config, model_version: str, new_version: str, trained_by: str | None = None,
            extra: dict | None = None) -> dict:
    """Rebuild a model using ONLY what the registry recorded (snapshot, filter, features, split, params, seed)."""
    row = registry.get_model(spark, cfg, model_version)
    if row is None:
        raise ValueError(f"unknown model_version {model_version!r}")
    if not snapshot_exists(spark, row["table_name"], row["snapshot_id"]):
        raise EvidenceLost(f"snapshot {row['snapshot_id']} for {model_version} no longer exists (expired)")
    return train(spark, cfg, new_version, snapshot_id=row["snapshot_id"], overrides={**settings_from_row(row), **(extra or {})},
                 trained_by=trained_by, tag=False)


def _probe_matrix(spark, row, limit: int = 5000):
    """Feature matrix of the first `limit` rows (by loan_id) of the snapshot `row` was trained on."""
    s = settings_from_row(row)
    df = read_at_snapshot(spark, row["table_name"], row["snapshot_id"])
    pdf = to_pandas(prepare_frame(df, s["features"], s["data_filter"]))
    return build_matrix(pdf, s["features"]).to_numpy()[:limit]


def compare_models(spark: SparkSession, cfg: Config, a: str, b: str) -> dict:
    """L1: same data (snapshot + fingerprint). L2: bit-identical predictions. L3: identical model file hash.

    Both models predict on the SAME probe rows (taken from model a's snapshot), so the comparison is meaningful even
    when the two models were trained on different snapshots (then L1 is false and L2 normally is too)."""
    ra, rb = registry.get_model(spark, cfg, a), registry.get_model(spark, cfg, b)
    l1 = ra["snapshot_id"] == rb["snapshot_id"] and ra["data_fingerprint"] == rb["data_fingerprint"]
    X = _probe_matrix(spark, ra)
    pa = lgb.Booster(model_file=ra["artifact_uri"]).predict(X)
    try:
        pb = lgb.Booster(model_file=rb["artifact_uri"]).predict(X)
        l2, diff = bool(np.array_equal(pa, pb)), (float(np.max(np.abs(pa - pb))) if len(pa) else 0.0)
    except Exception:                            # e.g. different feature count: not comparable
        l2, diff = False, None
    l3 = ra["artifact_sha256"] == rb["artifact_sha256"]
    return {"a": a, "b": b, "snapshot_a": ra["snapshot_id"], "snapshot_b": rb["snapshot_id"],
            "L1_same_data": bool(l1), "L2_identical_predictions": l2, "L3_identical_model_file": bool(l3),
            "fingerprint_a": ra["data_fingerprint"].asDict(), "fingerprint_b": rb["data_fingerprint"].asDict(),
            "artifact_sha_a": ra["artifact_sha256"], "artifact_sha_b": rb["artifact_sha256"],
            "max_abs_pred_diff": diff, "n_probe": int(len(pa))}
