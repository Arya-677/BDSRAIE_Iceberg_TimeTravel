"""Audit tables: model registry, model status events, ingest log.

Design rule: these tables are APPEND-ONLY. There is deliberately no update/delete function.
A change of state (e.g. 'quarantined') is a new row in model_status_events, so history is never lost.
"""
from __future__ import annotations

import json
import re

from pyspark.sql import SparkSession

from .config import Config
from .rows import frame_from_rows
from .timeutil import utc_now

# (name, spark SQL type). Single source of truth for DDL and DataFrame construction.
REGISTRY_FIELDS = [
    ("model_version", "STRING"), ("created_at", "TIMESTAMP"), ("trained_by", "STRING"),
    ("table_name", "STRING"),
    ("snapshot_id", "BIGINT"),                     # <- the point of the whole project
    ("snapshot_committed_at", "TIMESTAMP"),
    ("data_filter", "STRING"), ("split_rule", "STRING"), ("feature_list_sha256", "STRING"),
    ("data_fingerprint", "STRUCT<rows: BIGINT, hash: STRING>"),
    ("params_json", "STRING"), ("seed", "INT"),
    ("git_commit", "STRING"), ("git_dirty", "BOOLEAN"),
    ("env_lock_sha256", "STRING"), ("lib_versions", "MAP<STRING,STRING>"),
    ("metrics_json", "STRING"), ("artifact_uri", "STRING"), ("artifact_sha256", "STRING"),
]
REQUIRED_ON_REGISTER = ["model_version", "table_name", "snapshot_id", "data_fingerprint",
                        "feature_list_sha256", "params_json", "git_commit"]

EVENT_FIELDS = [("model_version", "STRING"), ("status", "STRING"), ("reason", "STRING"),
                ("actor", "STRING"), ("event_time", "TIMESTAMP")]
VALID_STATUS = {"candidate", "approved", "deployed", "quarantined", "retired"}

INGEST_LOG_FIELDS = [
    ("batch_id", "STRING"), ("source_file", "STRING"), ("source_sha256", "STRING"),
    ("rows_in", "BIGINT"), ("snapshot_id_after", "BIGINT"),
    ("table_rows_after", "BIGINT"), ("table_hash_after", "STRING"),   # table fingerprint at that snapshot
    ("gate_bypassed", "BOOLEAN"), ("validation_json", "STRING"), ("loaded_at", "TIMESTAMP"),
]


ROLLBACK_FIELDS = [
    ("event_time", "TIMESTAMP"), ("table_name", "STRING"), ("from_snapshot_id", "BIGINT"),
    ("to_snapshot_id", "BIGINT"), ("bad_batch", "STRING"), ("reason", "STRING"), ("actor", "STRING"),
    ("models_quarantined", "STRING"), ("head_fingerprint_verified", "BOOLEAN"),
]


def _ddl(table: str, fields: list[tuple[str, str]]) -> str:
    cols = ",\n  ".join(f"{n} {t}" for n, t in fields)
    return f"CREATE TABLE IF NOT EXISTS {table} (\n  {cols}\n) USING iceberg TBLPROPERTIES ('format-version'='2')"


def _schema(fields) -> str:
    return ", ".join(f"{n} {t}" for n, t in fields)


def row_frame(spark: SparkSession, fields, rec: dict):
    """One-row DataFrame for an audit table, built by the JVM only (no Python worker; see ttml/rows.py).
    Datetimes in `rec` must be timezone-aware."""
    return frame_from_rows(spark, [tuple(rec.get(n) for n, _ in fields)], _schema(fields))


def create_audit_tables(spark: SparkSession, cfg: Config) -> None:
    spark.sql(_ddl(cfg.registry_table, REGISTRY_FIELDS))
    spark.sql(_ddl(cfg.status_events_table, EVENT_FIELDS))
    spark.sql(_ddl(cfg.ingest_log_table, INGEST_LOG_FIELDS))
    spark.sql(_ddl(cfg.rollback_log_table, ROLLBACK_FIELDS))


_now = utc_now      # timezone-aware UTC: naive datetimes would be reinterpreted in the Python process' local timezone

_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")


def validate_identifier(value: str, what: str = "identifier") -> str:
    """Model versions and batch ids are interpolated into SQL and tag names; allow only a safe alphabet."""
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):   # fullmatch: '$' would accept a trailing newline
        raise ValueError(f"invalid {what} {value!r}: use letters, digits, '.', '_' or '-' (max 64 chars, must start alphanumeric)")
    return value


# ---------------------------------------------------------------- model registry
def register_model(spark: SparkSession, cfg: Config, **rec) -> None:
    """Append one immutable registry row. Fails loudly if the lineage is incomplete or the version exists."""
    validate_identifier(rec.get("model_version"), "model_version")
    missing = [k for k in REQUIRED_ON_REGISTER if rec.get(k) in (None, "")]
    if missing:
        raise ValueError(f"cannot register model: missing required lineage fields {missing}")
    if get_model(spark, cfg, rec["model_version"]) is not None:
        raise ValueError(f"model_version {rec['model_version']!r} already exists (registry is append-only)")
    fp = rec["data_fingerprint"]
    rec = dict(rec)
    rec["data_fingerprint"] = (int(fp["rows"]), str(fp["hash"]))
    rec.setdefault("created_at", _now())
    for k in ("params_json", "metrics_json"):
        if isinstance(rec.get(k), dict):
            rec[k] = json.dumps(rec[k], sort_keys=True)
    row_frame(spark, REGISTRY_FIELDS, rec).writeTo(cfg.registry_table).append()
    log_status(spark, cfg, rec["model_version"], "candidate", "registered at training time", rec.get("trained_by") or "unknown")


def get_model(spark: SparkSession, cfg: Config, model_version: str):
    validate_identifier(model_version, "model_version")
    rows = spark.table(cfg.registry_table).filter(f"model_version = '{model_version}'").collect()
    return rows[0] if rows else None


def models_for_snapshot(spark: SparkSession, cfg: Config, snapshot_ids: list[int]):
    ids = ",".join(str(int(i)) for i in snapshot_ids) or "NULL"
    return spark.table(cfg.registry_table).filter(f"snapshot_id IN ({ids})").collect()


# ---------------------------------------------------------------- status events
def log_status(spark: SparkSession, cfg: Config, model_version: str, status: str, reason: str, actor: str) -> None:
    validate_identifier(model_version, "model_version")
    if status not in VALID_STATUS:
        raise ValueError(f"status must be one of {sorted(VALID_STATUS)}")
    rec = {"model_version": model_version, "status": status, "reason": reason, "actor": actor, "event_time": _now()}
    row_frame(spark, EVENT_FIELDS, rec).writeTo(cfg.status_events_table).append()


def current_status(spark: SparkSession, cfg: Config, model_version: str) -> str | None:
    validate_identifier(model_version, "model_version")
    r = (spark.table(cfg.status_events_table).filter(f"model_version = '{model_version}'")
         .orderBy("event_time", ascending=False).limit(1).collect())
    return r[0]["status"] if r else None


# ---------------------------------------------------------------- ingest log
def log_ingest(spark: SparkSession, cfg: Config, **rec) -> None:
    rec.setdefault("loaded_at", _now())
    if isinstance(rec.get("validation_json"), dict):
        rec["validation_json"] = json.dumps(rec["validation_json"], sort_keys=True, default=str)
    row_frame(spark, INGEST_LOG_FIELDS, rec).writeTo(cfg.ingest_log_table).append()


def batch_already_loaded(spark: SparkSession, cfg: Config, batch_id: str) -> bool:
    """True only if that batch's snapshot is still part of the table's current history.
    After a rollback, batches that were rolled away count as NOT loaded, so they can be re-ingested."""
    from .snapshots import head_ancestors
    validate_identifier(batch_id, "batch_id")
    live = head_ancestors(spark, cfg.loans_table)
    ids = [r[0] for r in spark.table(cfg.ingest_log_table).filter(f"batch_id = '{batch_id}'").select("snapshot_id_after").collect()]
    return any(i in live for i in ids)


def log_rollback(spark: SparkSession, cfg: Config, **rec) -> None:
    rec.setdefault("event_time", _now())
    row_frame(spark, ROLLBACK_FIELDS, rec).writeTo(cfg.rollback_log_table).append()
