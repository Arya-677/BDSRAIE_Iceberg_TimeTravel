"""Create namespaces and all tables (idempotent); drop helpers for demos and tests."""
from __future__ import annotations

from pyspark.sql import SparkSession

from .config import Config
from .registry import create_audit_tables
from .schema import loans_ddl


def all_tables(cfg: Config) -> list[str]:
    return [cfg.loans_table, cfg.registry_table, cfg.status_events_table, cfg.ingest_log_table, cfg.rollback_log_table]


def ensure_namespaces(spark: SparkSession, cfg: Config) -> None:
    for ns in sorted({t.rsplit(".", 1)[0] for t in all_tables(cfg)}):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")


def init_tables(spark: SparkSession, cfg: Config) -> None:
    ensure_namespaces(spark, cfg)
    spark.sql(loans_ddl(cfg))
    create_audit_tables(spark, cfg)


def drop_all(spark: SparkSession, cfg: Config) -> None:
    """Drop every project table (namespaces are created first so DROP IF EXISTS works on a fresh warehouse)."""
    ensure_namespaces(spark, cfg)
    for t in all_tables(cfg):
        spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
