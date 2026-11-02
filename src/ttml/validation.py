"""Ingest gate. Split in two so the decision logic is testable without Spark:

  compute_stats(df, existing_ids_df)  -> dict of numbers   (Spark)
  evaluate_stats(stats, thresholds)   -> CheckResult       (pure Python)
"""
from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CRITICAL_COLUMNS = ["loan_id", "issue_d", "loan_amnt", "int_rate", "annual_inc", "target"]


@dataclass
class CheckResult:
    ok: bool
    reasons: list[str] = field(default_factory=list)
    stats: dict = field(default_factory=dict)


def compute_stats(df: DataFrame, existing: DataFrame | None = None) -> dict:
    """Summary numbers for one monthly batch. `existing` = DataFrame with a loan_id column
    (the current table) used to detect ids already loaded in earlier batches."""
    present = [c for c in CRITICAL_COLUMNS if c in df.columns]
    aggs = [F.count("*").alias("rows"),
            F.countDistinct("loan_id").alias("distinct_ids"),
            F.avg("target").alias("label_rate")]
    for c in present:
        aggs.append(F.avg(F.col(c).isNull().cast("int")).alias(f"null_{c}"))
    if "int_rate" in df.columns:
        aggs += [F.min("int_rate").alias("int_rate_min"), F.max("int_rate").alias("int_rate_max"),
                 F.avg("int_rate").alias("int_rate_mean")]
    row = df.agg(*aggs).first().asDict()

    rows = row["rows"] or 0
    stats = {
        "rows": rows,
        "duplicate_ids_in_batch": rows - (row["distinct_ids"] or 0),
        "label_rate": row["label_rate"],
        "null_rates": {c: (row[f"null_{c}"] or 0.0) for c in present},
        "int_rate_min": row.get("int_rate_min"),
        "int_rate_max": row.get("int_rate_max"),
        "int_rate_mean": row.get("int_rate_mean"),
        "ids_already_loaded": 0,
    }
    if existing is not None and rows:
        stats["ids_already_loaded"] = df.select("loan_id").join(existing.select("loan_id"), "loan_id", "inner").count()
    return stats


def evaluate_stats(stats: dict, t: dict) -> CheckResult:
    """Apply thresholds. Returns every failed rule, not just the first, so the log is useful."""
    reasons: list[str] = []
    if stats["rows"] < t["min_rows"]:
        reasons.append(f"too few rows: {stats['rows']} < {t['min_rows']}")
    for col, rate in stats["null_rates"].items():
        if rate > t["max_null_rate"]:
            reasons.append(f"null rate for {col} is {rate:.1%} (max {t['max_null_rate']:.1%})")
    lr = stats["label_rate"]
    if lr is not None and not (t["label_rate_min"] <= lr <= t["label_rate_max"]):
        reasons.append(f"label (default) rate {lr:.1%} outside [{t['label_rate_min']:.0%}, {t['label_rate_max']:.0%}]")
    lo, hi = stats.get("int_rate_min"), stats.get("int_rate_max")
    if lo is not None and lo < t["int_rate_min"]:
        reasons.append(f"int_rate min {lo} below {t['int_rate_min']}")
    if hi is not None and hi > t["int_rate_max"]:
        reasons.append(f"int_rate max {hi} above {t['int_rate_max']}")
    dup = stats["duplicate_ids_in_batch"] + stats["ids_already_loaded"]
    if dup > t["max_duplicate_ids"]:
        reasons.append(f"{dup} duplicate loan_id values (in batch: {stats['duplicate_ids_in_batch']}, already in table: {stats['ids_already_loaded']})")
    return CheckResult(ok=not reasons, reasons=reasons, stats=stats)


def validate(df: DataFrame, thresholds: dict, existing: DataFrame | None = None) -> CheckResult:
    return evaluate_stats(compute_stats(df, existing), thresholds)
