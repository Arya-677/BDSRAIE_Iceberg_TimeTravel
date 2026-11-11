"""Monthly batch ingest. One calendar month = one append commit = one Iceberg snapshot."""
from __future__ import annotations

<<<<<<< HEAD
import gzip
=======
>>>>>>> origin/main
import hashlib
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config
from .data_gen import months_between
from .registry import batch_already_loaded, log_ingest, validate_identifier
from .schema import clean, required_source_columns
from .snapshots import current_snapshot_id, fingerprint, read_at_snapshot
from .validation import compute_stats, evaluate_stats


class BatchRejected(Exception):
    def __init__(self, batch_id: str, reasons: list[str]):
        super().__init__(f"batch {batch_id} rejected by ingest gate: " + "; ".join(reasons))
        self.batch_id, self.reasons = batch_id, reasons


def sha256_file(path: str | Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


<<<<<<< HEAD
ID_CANDIDATES = ["id", "loan_id", "Id", "ID", "_c0", "Unnamed: 0"]      # '_c0' = Spark's name for a blank header cell


def sniff_delimiter(path: str) -> str:
    """Comma, tab, semicolon or pipe, judged from the header line (handles .gz). Tab matters: pasted/exported samples are often TSV."""
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8-sig", errors="replace", newline="") as f:
        header = f.readline()
    counts = {d: header.count(d) for d in (",", "\t", ";", "|")}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ","


def resolve_id_column(columns: list[str]) -> str | None:
    """The loan-id column: 'id', 'loan_id', or the blank-header first column that Excel/pandas exports produce."""
    for c in ID_CANDIDATES:
        if c in columns:
            return c
    return None


def read_source_csv(spark: SparkSession, path: str, features: list[str], multiline: bool = True, sep: str | None = None) -> DataFrame:
=======
def read_source_csv(spark: SparkSession, path: str, features: list[str], multiline: bool = True) -> DataFrame:
>>>>>>> origin/main
    """Read raw CSV as strings (no schema inference: faster, and clean() owns all parsing).

    multiLine=True because real LendingClub exports contain quoted free-text fields with embedded newlines
    (e.g. loan descriptions); without it rows are silently split and corrupted. It makes the read less parallel;
    set data.csv_multiline: false if your file has none and you need speed."""
<<<<<<< HEAD
    r = spark.read.option("header", True).option("mode", "PERMISSIVE").option("sep", sep or sniff_delimiter(path))
    if multiline:
        r = r.option("multiLine", True).option("quote", '"').option("escape", '"')
    raw = r.csv(path)
    raw = raw.toDF(*[c.replace("\ufeff", "").strip() for c in raw.columns])      # a UTF-8 BOM would otherwise corrupt the first name
    id_col = resolve_id_column(raw.columns)
    if id_col is None:
        raise ValueError(f"source file {path} has no loan-id column. Expected a header named 'id' or 'loan_id', or a first "
                         f"column with a blank header. Columns found (first 8): {raw.columns[:8]}")
    need = [c for c in required_source_columns(features) if c != "id"]
    missing = [c for c in need if c not in raw.columns]
    if missing:
        raise ValueError(f"source file {path} is missing expected columns: {missing}. "
                         "Check conf/features_allowlist.txt against the file header (names differ between LendingClub versions).")
    return raw.select(F.col(f"`{id_col}`").alias("id"), *[F.col(f"`{c}`") for c in need])


def load_clean_frame(spark: SparkSession, cfg: Config, csv_path: str) -> DataFrame:
    d = cfg.raw.get("data", {})
    return clean(read_source_csv(spark, csv_path, cfg.features(), d.get("csv_multiline", True), d.get("csv_sep")), cfg.features())
=======
    r = spark.read.option("header", True).option("mode", "PERMISSIVE")
    if multiline:
        r = r.option("multiLine", True).option("quote", '"').option("escape", '"')
    raw = r.csv(path)
    missing = [c for c in required_source_columns(features) if c not in raw.columns]
    if missing:
        raise ValueError(f"source file {path} is missing expected columns: {missing}. "
                         "Check conf/features_allowlist.txt against the file header (names differ between Kaggle versions).")
    return raw.select(*required_source_columns(features))


def load_clean_frame(spark: SparkSession, cfg: Config, csv_path: str) -> DataFrame:
    multiline = cfg.raw.get("data", {}).get("csv_multiline", True)
    return clean(read_source_csv(spark, csv_path, cfg.features(), multiline), cfg.features())
>>>>>>> origin/main


def ingest_month(spark: SparkSession, cfg: Config, df_month: DataFrame, batch_id: str,
                 source_file: str, source_sha256: str, bypass_gate: bool = False, force: bool = False):
    """Validate then append one month. Returns the new snapshot id, or None if skipped.

    bypass_gate / force exist ONLY for the rollback demo (loading a deliberately bad batch, and
    re-loading a corrected batch under the same id). Both are recorded in the ingest log.
    """
    validate_identifier(batch_id, "batch_id")
    if not force and batch_already_loaded(spark, cfg, batch_id):
        print(f"[ingest] {batch_id}: already loaded, skipping")
        return None

    table = cfg.loans_table
    existing = spark.table(table) if current_snapshot_id(spark, table) is not None else None
    stats = compute_stats(df_month, existing)
    if stats["rows"] == 0:
        print(f"[ingest] {batch_id}: no rows, skipping")
        return None
    result = evaluate_stats(stats, cfg.validation)
    if not result.ok and not bypass_gate:
        raise BatchRejected(batch_id, result.reasons)

    # write columns in table order, so the append never depends on DataFrame column order
    target_cols = spark.table(table).columns
    out = df_month.drop("issue_month").withColumn("ingest_batch", F.lit(batch_id)).select(*target_cols)
    out.writeTo(table).append()                                    # <- exactly one commit

    sid = current_snapshot_id(spark, table)
    # Table-level fingerprint at this snapshot: lets a later rollback PROVE the table is back to this state.
    # It rescans the whole table every month; for very large loads set ingest.log_table_fingerprint: false.
    if cfg.raw.get("ingest", {}).get("log_table_fingerprint", True):
        fp = fingerprint(read_at_snapshot(spark, table, sid))
    else:
        fp = {"rows": None, "hash": None}
    log_ingest(spark, cfg, batch_id=batch_id, source_file=source_file, source_sha256=source_sha256,
               rows_in=stats["rows"], snapshot_id_after=sid, table_rows_after=fp["rows"],
               table_hash_after=fp["hash"], gate_bypassed=bool(bypass_gate and not result.ok),
               validation_json={"ok": result.ok, "reasons": result.reasons, "stats": stats})
    print(f"[ingest] {batch_id}: {stats['rows']} rows -> snapshot {sid}"
          + ("  (GATE BYPASSED)" if bypass_gate and not result.ok else ""))
    return sid


def ingest_csv(spark: SparkSession, cfg: Config, csv_path: str, start: str | None = None,
<<<<<<< HEAD
               end: str | None = None, max_months: int | None = None, skip_rejected: bool = False) -> list[int]:
    """Load a CSV month by month in chronological order. Returns the snapshot ids created.

    skip_rejected=True: a month the ingest gate rejects (typically a sparse recent month where most loans are still 'Current',
    leaving few rows and a skewed default rate) is reported and skipped instead of stopping the whole load."""
=======
               end: str | None = None, max_months: int | None = None) -> list[int]:
    """Load a CSV month by month in chronological order. Returns the snapshot ids created."""
>>>>>>> origin/main
    start = start or cfg.raw["data"]["start_month"]
    end = end or cfg.raw["data"]["end_month"]
    sha = sha256_file(csv_path)
    df = load_clean_frame(spark, cfg, csv_path).cache()            # parse once, slice per month
    present = {r[0] for r in df.select("issue_month").distinct().collect()}
    months = [m for m in months_between(start, end) if m in present]
    if max_months:
        months = months[:max_months]
    ids = []
    try:
        for m in months:
<<<<<<< HEAD
            try:
                sid = ingest_month(spark, cfg, df.filter(F.col("issue_month") == m), m, csv_path, sha)
            except BatchRejected as e:
                if not skip_rejected:
                    raise BatchRejected(e.batch_id, e.reasons + ["(use --skip-rejected to skip such months, or adjust validation.* "
                                                                 "in conf/pipeline.yaml, or choose another date range)"]) from None
                print(f"[ingest] {m}: SKIPPED by the gate: {'; '.join(e.reasons)}")
                continue
=======
            sid = ingest_month(spark, cfg, df.filter(F.col("issue_month") == m), m, csv_path, sha)
>>>>>>> origin/main
            if sid is not None:
                ids.append(sid)
    finally:
        df.unpersist()
    return ids
