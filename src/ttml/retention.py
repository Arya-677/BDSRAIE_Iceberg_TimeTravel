"""Tiered retention with a safety guard.

Tier 1 release evidence (approved/deployed/quarantined models) : tag, long RETAIN (release_retention_days)
Tier 2 experiment/candidate models                               : tag, short RETAIN (experiment_retention_days)
Tier 3 routine ingestion snapshots                               : untagged; expire after retention.routine_days,
                                                                   always keep retention.retain_last
The guard refuses to run if any registered, non-retired model's snapshot would lose its protection.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from . import registry
from .config import Config
from .snapshots import list_refs, set_tag_retention, snapshot_exists, split_table
from .timeutil import utc_naive

RELEASE_STATUSES = {"approved", "deployed"}


class RetentionGuardError(Exception):
    def __init__(self, problems: list[str]):
        super().__init__("retention aborted, evidence would be lost: " + "; ".join(problems))
        self.problems = problems


def tag_name(model_version: str) -> str:
    return f"model-{model_version}"


def promote(spark: SparkSession, cfg: Config, model_version: str, status: str, actor: str, reason: str) -> None:
    """Change a model's status; approving/deploying extends its snapshot tag to the release retention period (tier 1)."""
    if status in RELEASE_STATUSES and cfg.training.get("require_clean_git"):
        row = registry.get_model(spark, cfg, model_version)
        if row is None:
            raise ValueError(f"unknown model_version {model_version!r}")
        if row["git_dirty"]:
            raise ValueError(f"{model_version} was trained from uncommitted code (git_dirty=true); "
                             "retrain from a clean commit before approving (training.require_clean_git is on)")
    registry.log_status(spark, cfg, model_version, status, reason, actor)
    if status in RELEASE_STATUSES:
        set_tag_retention(spark, cfg.loans_table, tag_name(model_version), cfg.training["release_retention_days"])


def check_guard(spark: SparkSession, cfg: Config) -> list[str]:
    """Problems found; empty list means safe. Every non-retired registered model needs: snapshot exists AND a tag on it."""
    table = cfg.loans_table
    tags = {r["name"]: r["snapshot_id"] for r in list_refs(spark, table) if r["type"] == "TAG"}
    tagged_ids = set(tags.values())
    problems = []
    for m in spark.table(cfg.registry_table).select("model_version", "snapshot_id").collect():
        v, sid = m["model_version"], m["snapshot_id"]
        if registry.current_status(spark, cfg, v) == "retired":
            continue
        if not snapshot_exists(spark, table, sid):
            problems.append(f"{v}: snapshot {sid} already missing")
        elif sid not in tagged_ids:
            problems.append(f"{v}: snapshot {sid} has no protecting tag")
    return problems


def run_retention(spark: SparkSession, cfg: Config, now: datetime | None = None, older_than: datetime | None = None,
                  skip_orphans: bool = False, dry_run: bool = False, retain_last: int | None = None) -> dict:
    problems = check_guard(spark, cfg)
    if problems:
        raise RetentionGuardError(problems)
    now = now or utc_naive()
    r = cfg.retention
    cutoff = older_than or (now - timedelta(days=r["routine_days"]))
    cat, name = split_table(cfg.loans_table)
    out = {"guard": "passed", "expire_cutoff": cutoff.isoformat(sep=" ", timespec="seconds"), "dry_run": dry_run}
    if dry_run:
        return out
    ts = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    res = spark.sql(f"CALL {cat}.system.expire_snapshots(table => '{name}', older_than => TIMESTAMP '{ts}', "
                    f"retain_last => {int(retain_last if retain_last is not None else r['retain_last'])})").collect()
    out["expire_result"] = [x.asDict() for x in res]
    if not skip_orphans:
        o = (now - timedelta(days=r["orphan_days"])).strftime("%Y-%m-%d %H:%M:%S")
        out["orphans"] = [x.asDict() for x in spark.sql(
            f"CALL {cat}.system.remove_orphan_files(table => '{name}', older_than => TIMESTAMP '{o}')").collect()]
    spark.sql(f"CALL {cat}.system.rewrite_manifests('{name}')")
    leftover = check_guard(spark, cfg)
    out["guard_after"] = "passed" if not leftover else leftover
    return out
