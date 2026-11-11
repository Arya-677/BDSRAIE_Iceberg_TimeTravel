"""Iceberg snapshot helpers: pin, list, time-travel read, fingerprint, tag, rollback."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .frames import to_pandas
from .timeutil import from_micros


def split_table(fq: str) -> tuple[str, str]:
    """'lake.credit.loans' -> ('lake', 'credit.loans'). CALL procedures want the catalog-less name."""
    cat, rest = fq.split(".", 1)
    return cat, rest


def current_snapshot_id(spark: SparkSession, table: str) -> int | None:
    """Snapshot ID that the 'main' branch points to (None for a table with no commits yet).
    Uses the refs metadata table rather than 'latest by commit time', which is safer with concurrent writers."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    return rows[0][0] if rows else None


def list_snapshots(spark: SparkSession, table: str):
    """pandas DataFrame of snapshots, oldest first."""
    return to_pandas(spark.sql(f"SELECT snapshot_id, parent_id, committed_at, operation FROM {table}.snapshots ORDER BY committed_at"))


def snapshot_exists(spark: SparkSession, table: str, snapshot_id: int) -> bool:
    return spark.sql(f"SELECT 1 FROM {table}.snapshots WHERE snapshot_id = {int(snapshot_id)}").count() > 0


def snapshot_committed_at(spark: SparkSession, table: str, snapshot_id: int):
    r = spark.sql(f"SELECT unix_micros(committed_at) FROM {table}.snapshots WHERE snapshot_id = {int(snapshot_id)}").collect()
    if not r:
        raise ValueError(f"snapshot {snapshot_id} not found in {table} (expired?)")
    return from_micros(r[0][0])       # aware UTC; avoids local-timezone reinterpretation by PySpark


def read_at_snapshot(spark: SparkSession, table: str, snapshot_id: int) -> DataFrame:
    """Time-travel read. Raises if the snapshot no longer exists."""
    if not snapshot_exists(spark, table, snapshot_id):
        raise ValueError(f"snapshot {snapshot_id} not found in {table} (expired or wrong table)")
    return spark.read.format("iceberg").option("snapshot-id", int(snapshot_id)).load(table)


_NULL = "\u2205"   # sentinel so NULL and the string 'NULL' differ


def fingerprint(df: DataFrame, exclude: list[str] | None = None) -> dict:
    """Order-independent content fingerprint: {'rows': n, 'hash': '<decimal string>'}.

    Each row is rendered as 'col=value' pairs in sorted column order (NULL gets a sentinel, so a NULL
    cannot silently shift between columns), hashed with xxhash64, and the hashes are summed into a
    DECIMAL(38,0) so the total cannot overflow. Same rows in any order -> same fingerprint.
    NOT cryptographic: it detects accidental or unnoticed change, not a deliberate collision attack.
    """
    cols = sorted(c for c in df.columns if c not in set(exclude or []))
    parts = [F.concat(F.lit(f"{c}="), F.coalesce(F.col(c).cast("string"), F.lit(_NULL))) for c in cols]
    row_hash = F.xxhash64(F.concat_ws("\u001f", *parts))
    r = df.agg(F.count(F.lit(1)).alias("n"), F.sum(row_hash.cast("decimal(38,0)")).alias("h")).first()
    return {"rows": int(r["n"]), "hash": str(r["h"] if r["h"] is not None else 0)}


def tag_snapshot(spark: SparkSession, table: str, snapshot_id: int, tag: str, retain_days: int | None = None) -> None:
    """Create a named tag so expire_snapshots will not delete this snapshot (until RETAIN elapses)."""
    retain = f" RETAIN {int(retain_days)} DAYS" if retain_days else ""
    spark.sql(f"ALTER TABLE {table} CREATE TAG `{tag}` AS OF VERSION {int(snapshot_id)}{retain}")


def rollback_to_snapshot(spark: SparkSession, table: str, snapshot_id: int) -> None:
    """Move 'main' back to an ancestor snapshot. History is not erased; later snapshots stay until expired."""
    cat, name = split_table(table)
    spark.sql(f"CALL {cat}.system.rollback_to_snapshot('{name}', {int(snapshot_id)})")


# ---------------------------------------------------------------- lineage (pure + Spark)
def ancestors_from_parents(parents: dict, sid: int) -> list:
    """[sid, parent, grandparent, ...]. `parents` maps snapshot_id -> parent_id (or None)."""
    out, seen = [], set()
    while sid is not None and sid not in seen:
        out.append(sid)
        seen.add(sid)
        sid = parents.get(sid)
    return out


def descendants_from_parents(parents: dict, sid: int) -> set:
    """Every snapshot whose ancestry contains `sid`, including `sid` itself."""
    return {s for s in parents if sid in ancestors_from_parents(parents, s)}


def parent_map(spark: SparkSession, table: str) -> dict:
    return {r["snapshot_id"]: r["parent_id"] for r in spark.sql(f"SELECT snapshot_id, parent_id FROM {table}.snapshots").collect()}


def head_ancestors(spark: SparkSession, table: str) -> set:
    head = current_snapshot_id(spark, table)
    return set(ancestors_from_parents(parent_map(spark, table), head)) if head is not None else set()


def list_refs(spark: SparkSession, table: str):
    """Branches and tags: name, type, snapshot_id, max_reference_age_in_ms."""
    return spark.sql(f"SELECT name, type, snapshot_id, max_reference_age_in_ms FROM {table}.refs").collect()


def set_tag_retention(spark: SparkSession, table: str, tag: str, retain_days: int) -> None:
    sid = next((r["snapshot_id"] for r in list_refs(spark, table) if r["type"] == "TAG" and r["name"] == tag), None)
    if sid is None:
        raise ValueError(f"tag {tag!r} not found on {table}")
    stmt = f"TAG `{tag}` AS OF VERSION {int(sid)} RETAIN {int(retain_days)} DAYS"
    try:
        spark.sql(f"ALTER TABLE {table} REPLACE {stmt}")
    except Exception:                                  # syntax differs slightly across Iceberg versions
        spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE {stmt}")
