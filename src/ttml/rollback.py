"""Detect a bad batch, contain the models trained on it, roll the table back, and PROVE the result."""
from __future__ import annotations

import getpass

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import registry
from .config import Config
from .frames import to_pandas
from .snapshots import (current_snapshot_id, descendants_from_parents, fingerprint, head_ancestors, parent_map,
                        read_at_snapshot, rollback_to_snapshot)


# ---------------------------------------------------------------- detection
def flag_outlier_batches(stats: pd.DataFrame, z: float = 5.0) -> pd.DataFrame:
    """Pure function. `stats` has one row per batch: batch, rows, label_rate, int_rate_mean, int_rate_max, dup_ids.
    Flags batches whose label_rate / int_rate_mean is a robust outlier (median/MAD z-score) versus the others,
    or that contain duplicates, or an impossible int_rate. Returns flagged rows with a `why` column."""
    s = stats.copy()
    why = [[] for _ in range(len(s))]
    for col in ("label_rate", "int_rate_mean"):
        med = s[col].median()
        mad = (s[col] - med).abs().median()
        # floor the scale: with few batches the MAD can be tiny by chance and would flag ordinary months
        # floors are deliberately generous: real data drifts slowly over years (rates, default share) and
        # this heuristic must catch gross failures (unit errors, flipped labels), not trend.
        floor = 0.04 if col == "label_rate" else max(abs(med) * 0.10, 1e-6)
        scale = max(1.4826 * mad, floor)
        zs = (s[col] - med).abs() / scale
        for i, v in enumerate(zs):
            if len(s) >= 4 and v > z:
                why[i].append(f"{col} {s[col].iloc[i]:.3g} is {v:.0f} robust-sigma from median {med:.3g}")
    for i in range(len(s)):
        if s["dup_ids"].iloc[i] > 0:
            why[i].append(f"{int(s['dup_ids'].iloc[i])} duplicate loan_ids")
        if s["int_rate_max"].iloc[i] > 40:
            why[i].append(f"int_rate max {s['int_rate_max'].iloc[i]:.3g} > 40")
    s["why"] = ["; ".join(w) for w in why]
    return s[s["why"] != ""].reset_index(drop=True)


def batch_stats(spark: SparkSession, table: str, snapshot_id: int | None = None) -> pd.DataFrame:
    df = read_at_snapshot(spark, table, snapshot_id) if snapshot_id else spark.table(table)
    g = df.groupBy("ingest_batch").agg(
        F.count("*").alias("rows"), F.avg("target").alias("label_rate"), F.avg("int_rate").alias("int_rate_mean"),
        F.max("int_rate").alias("int_rate_max"), (F.count("*") - F.countDistinct("loan_id")).alias("dup_ids"))
    return to_pandas(g.withColumnRenamed("ingest_batch", "batch").orderBy("batch"))


def audit_batches(spark: SparkSession, cfg: Config) -> pd.DataFrame:
    """Post-hoc data-quality audit of everything currently in the table. Adds the snapshot that introduced each batch."""
    flagged = flag_outlier_batches(batch_stats(spark, cfg.loans_table))
    if flagged.empty:
        flagged["snapshot_id"] = []
        return flagged
    log = (to_pandas(spark.table(cfg.ingest_log_table).select("batch_id", "snapshot_id_after", "loaded_at"))
           .sort_values("loaded_at").drop_duplicates("batch_id", keep="last").rename(columns={"batch_id": "batch", "snapshot_id_after": "snapshot_id"}))
    return flagged.merge(log[["batch", "snapshot_id"]], on="batch", how="left")


def snapshot_for_batch(spark: SparkSession, cfg: Config, batch_id: str) -> int:
    """Snapshot that introduced `batch_id` into the CURRENT table history (latest load wins). Does not depend on the audit."""
    registry.validate_identifier(batch_id, "batch_id")
    live = head_ancestors(spark, cfg.loans_table)
    rows = [(r["loaded_at"], r["snapshot_id_after"]) for r in
            spark.table(cfg.ingest_log_table).filter(f"batch_id = '{batch_id}'").collect() if r["snapshot_id_after"] in live]
    if not rows:
        raise ValueError(f"batch {batch_id!r} has no snapshot in the table's current history")
    return max(rows)[1]


# ---------------------------------------------------------------- containment + rollback
def rollback_workflow(spark: SparkSession, cfg: Config, bad_snapshot_id: int, bad_batch: str, reason: str,
                      actor: str | None = None) -> dict:
    """1. find last good snapshot (parent of the bad one)  2. quarantine every model trained on the bad snapshot or
    anything after it  3. roll back  4. verify head fingerprint == fingerprint logged when that snapshot was good
    5. write an append-only rollback_log row."""
    actor = actor or getpass.getuser()
    table = cfg.loans_table
    parents = parent_map(spark, table)
    if bad_snapshot_id not in head_ancestors(spark, table):
        raise ValueError(f"snapshot {bad_snapshot_id} is not in the current table history; nothing to roll back")
    good = parents.get(bad_snapshot_id)
    if good is None:
        raise ValueError("bad snapshot has no parent; nothing to roll back to")
    from_sid = current_snapshot_id(spark, table)

    tainted = registry.models_for_snapshot(spark, cfg, sorted(descendants_from_parents(parents, bad_snapshot_id)))
    quarantined = []
    for m in tainted:
        registry.log_status(spark, cfg, m["model_version"], "quarantined",
                            f"trained on snapshot {m['snapshot_id']} at/after bad batch {bad_batch}: {reason}", actor)
        quarantined.append(m["model_version"])

    rollback_to_snapshot(spark, table, good)

    logged = (spark.table(cfg.ingest_log_table).filter(f"snapshot_id_after = {int(good)}").collect())
    head_fp = fingerprint(spark.table(table))
    verified = bool(logged) and logged[0]["table_hash_after"] is not None and \
        (head_fp["rows"], head_fp["hash"]) == (logged[0]["table_rows_after"], logged[0]["table_hash_after"])
    registry.log_rollback(spark, cfg, table_name=table, from_snapshot_id=from_sid, to_snapshot_id=good, bad_batch=bad_batch,
                          reason=reason, actor=actor, models_quarantined=",".join(quarantined),
                          head_fingerprint_verified=verified)
    return {"rolled_back_to": good, "from": from_sid, "models_quarantined": quarantined,
            "head_fingerprint": head_fp, "fingerprint_matches_logged_good_state": verified}
