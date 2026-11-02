"""Training with a pinned snapshot.

Order of operations (each step prevents a specific audit failure):
  1. pin snapshot id ONCE            -> training cannot see later commits
  2. read VERSION AS OF that id      -> data is reproducible
  3. sort by loan_id, fingerprint    -> row order cannot matter; content is provable
  4. deterministic LightGBM          -> same data + params => same model
  5. save artifact + sidecar, hash   -> artifact is verifiable
  6. append registry row, tag snap   -> model cannot exist without lineage; evidence cannot be expired
"""
from __future__ import annotations

import copy
import getpass
import hashlib
import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from sklearn.metrics import roc_auc_score, roc_curve

from . import registry
from .config import Config, names_sha256
from .features import array_sha256, build_matrix
from .frames import to_pandas
from .snapshots import (current_snapshot_id, fingerprint, read_at_snapshot, snapshot_committed_at,
                        tag_snapshot)

KEEP_COLS = ["loan_id", "issue_d", "target"]


# ---------------------------------------------------------------- settings
def merged_settings(cfg: Config, overrides: dict | None = None) -> dict:
    """Training settings = config defaults + overrides (e.g. from a registry row). Includes the feature list."""
    t = copy.deepcopy(cfg.training)
    ov = copy.deepcopy(overrides or {})
    params = {**t.get("params", {}), **ov.pop("params", {})}
    t.update(ov)
    t["params"] = params
    t.setdefault("features", None)
    if not t["features"]:
        t["features"] = cfg.features()
    return t


def lgbm_params(params: dict, seed: int) -> dict:
    """Deterministic LightGBM configuration. Single thread + row-wise histograms + fixed seeds."""
    p = dict(params)
    p.update(random_state=int(seed), deterministic=True, force_row_wise=True, n_jobs=1, verbose=-1,
             objective="binary", bagging_seed=int(seed), feature_fraction_seed=int(seed), data_random_seed=int(seed))
    return p


# ---------------------------------------------------------------- environment / provenance
def git_info(root: str | Path = ".") -> dict:
    """Commit hash and whether TRACKED files have uncommitted changes (untracked output folders do not count)."""
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, stderr=subprocess.DEVNULL, text=True).strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root,
                                             stderr=subprocess.DEVNULL, text=True).strip())
        return {"git_commit": commit, "git_dirty": dirty}
    except Exception:
        return {"git_commit": "no-git", "git_dirty": True}


def env_info(cfg: Config, lock_path: str | None = None) -> dict:
    import pyspark, sklearn
    libs = {"python": platform.python_version(), "pyspark": pyspark.__version__, "lightgbm": lgb.__version__,
            "numpy": np.__version__, "pandas": pd.__version__, "scikit-learn": sklearn.__version__,
            "iceberg_runtime": str(cfg.raw["catalog"]["iceberg_version"]), "machine": platform.machine()}
    p = Path(lock_path) if lock_path else cfg.path("environment.lock")
    lock_hash = hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else \
        hashlib.sha256(json.dumps(libs, sort_keys=True).encode()).hexdigest()
    return {"lib_versions": libs, "env_lock_sha256": lock_hash}


# ---------------------------------------------------------------- core (no Iceberg, no registry: unit-testable)
def prepare_frame(df: DataFrame, features: list[str], data_filter: str = "1 = 1") -> DataFrame:
    """Filter, project to the allow-list, and impose a total order (loan_id is unique)."""
    return df.filter(data_filter).select(*KEEP_COLS, *features).orderBy("loan_id")


def ks_statistic(y, p) -> float:
    fpr, tpr, _ = roc_curve(y, p)
    return float(np.max(tpr - fpr))


def fit_and_eval(pdf: pd.DataFrame, features: list[str], train_end: str, params: dict, seed: int):
    """pdf must already be sorted by loan_id. Returns (model, metrics_dict, X_test)."""
    pdf = pdf.copy()
    pdf["issue_d"] = pd.to_datetime(pdf["issue_d"])
    is_train = (pdf["issue_d"] <= pd.Timestamp(train_end)).to_numpy()
    tr, te = pdf[is_train], pdf[~is_train]
    if len(tr) == 0 or len(te) == 0:
        raise ValueError(f"train_end={train_end} leaves {len(tr)} train / {len(te)} test rows; adjust the split")
    Xtr, Xte = build_matrix(tr, features), build_matrix(te, features)
    ytr, yte = tr["target"].to_numpy(), te["target"].to_numpy()
    model = lgb.LGBMClassifier(**lgbm_params(params, seed))
    if len(set(ytr.tolist())) < 2 or len(set(yte.tolist())) < 2:
        raise ValueError("train or test window contains a single class; widen the data or move train_end "
                         f"(train classes={sorted(set(ytr.tolist()))}, test classes={sorted(set(yte.tolist()))})")
    model.fit(Xtr.to_numpy(), ytr)
    pte = model.predict_proba(Xte.to_numpy())[:, 1]
    metrics = {"auc": float(roc_auc_score(yte, pte)), "ks": ks_statistic(yte, pte),
               "rows_train": int(len(tr)), "rows_test": int(len(te)), "base_rate_test": float(yte.mean()),
               "x_train_sha256": array_sha256(Xtr.to_numpy()), "test_pred_sha256": array_sha256(pte)}
    return model, metrics, Xte


def model_text(model) -> str:
    return model.booster_.model_to_string()


def train_from_frame(df: DataFrame, settings: dict) -> dict:
    """Everything except snapshot pinning and registry. Returns model + metrics + data fingerprint."""
    feats = settings["features"]
    frame = prepare_frame(df, feats, settings["data_filter"])
    fp = fingerprint(frame)                                   # fingerprint of EXACTLY what the model sees
    pdf = to_pandas(frame)
    model, metrics, Xte = fit_and_eval(pdf, feats, settings["train_end"], settings["params"], settings["seed"])
    text = model_text(model)
    return {"model": model, "metrics": metrics, "fingerprint": fp, "model_text": text,
            "model_sha256": hashlib.sha256(text.encode()).hexdigest(), "X_test": Xte}


def save_artifact(root: str | Path, model_version: str, text: str, card: dict) -> tuple[str, str]:
    d = Path(root) / model_version
    d.mkdir(parents=True, exist_ok=True)
    mp = d / "model.txt"
    # write BYTES: text mode would turn \n into \r\n on Windows, so the file on disk would no longer match the SHA-256
    # recorded in the registry (computed over the exact text below).
    mp.write_bytes(text.encode("utf-8"))
    (d / "model_card.json").write_text(json.dumps(card, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return str(mp), hashlib.sha256(text.encode()).hexdigest()


# ---------------------------------------------------------------- full pipeline
def train(spark: SparkSession, cfg: Config, model_version: str, snapshot_id: int | None = None,
          overrides: dict | None = None, trained_by: str | None = None, tag: bool = True) -> dict:
    table = cfg.loans_table
    settings = merged_settings(cfg, overrides)
    sid = snapshot_id if snapshot_id is not None else current_snapshot_id(spark, table)      # 1. pin ONCE
    if sid is None:
        raise ValueError("table has no snapshots yet; ingest data first")
    if registry.get_model(spark, cfg, model_version) is not None:
        raise ValueError(f"model_version {model_version!r} already exists")
    df = read_at_snapshot(spark, table, sid)                                                 # 2. VERSION AS OF
    res = train_from_frame(df, settings)                                                     # 3-4
    registry.validate_identifier(model_version, "model_version")
    g, env = git_info(cfg.root), env_info(cfg)
    art_dir = str(cfg.path(settings.get("artifacts_dir", "artifacts")))      # absolute, so the stored URI works from any cwd
    card = {"model_version": model_version, "table": table, "snapshot_id": sid, "data_fingerprint": res["fingerprint"],
            "settings": {k: settings[k] for k in ("train_end", "data_filter", "seed", "params", "features")},
            "metrics": res["metrics"], **g, **env, "created_utc": datetime.now(timezone.utc).isoformat()}
    uri, sha = save_artifact(art_dir, model_version, res["model_text"], card)                # 5
    recorded = {"lgbm": settings["params"], "features": settings["features"], "train_end": settings["train_end"],
                "seed": settings["seed"]}
    if tag:                                                                                   # 6a: protect FIRST, so a
        tag_snapshot(spark, table, sid, f"model-{model_version}", settings["experiment_retention_days"])  # model never exists unprotected
    registry.register_model(                                                                 # 6b
        spark, cfg, model_version=model_version, trained_by=trained_by or getpass.getuser(), table_name=table,
        snapshot_id=sid, snapshot_committed_at=snapshot_committed_at(spark, table, sid),
        data_filter=settings["data_filter"], split_rule=f"time split: train issue_d <= {settings['train_end']}, test later",
        feature_list_sha256=names_sha256(settings["features"]), data_fingerprint=res["fingerprint"],
        params_json=recorded, seed=int(settings["seed"]), git_commit=g["git_commit"], git_dirty=g["git_dirty"],
        env_lock_sha256=env["env_lock_sha256"], lib_versions=env["lib_versions"], metrics_json=res["metrics"],
        artifact_uri=uri, artifact_sha256=sha)
    return {"model_version": model_version, "snapshot_id": sid, "fingerprint": res["fingerprint"],
            "metrics": res["metrics"], "artifact_uri": uri, "artifact_sha256": sha}
