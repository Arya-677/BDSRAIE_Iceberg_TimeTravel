"""Storage-overhead experiments E1-E5 (see project plan section 8). Produces CSVs and charts.

E1  append-only monthly loads                       overhead vs number of snapshots
E2  monthly appends + corrections, copy-on-write    overhead vs update rate
E3  same, merge-on-read                             overhead vs update rate
E4  compaction (rewrite_data_files)                 before / after compaction / after expiring old snapshots
E5  expiry sweep                                    overhead vs retain_last N
Corrections are synthetic: a seeded pseudo-random fraction of the PREVIOUS month's loans get their label flipped
(a stand-in for late outcome corrections; LendingClub has no real update feed). Say so in the report.
"""
from __future__ import annotations

import time
from datetime import timedelta
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import Config
from .data_gen import write_csv
from .ingest import load_clean_frame
<<<<<<< HEAD
from . import snapshot_storage
=======
>>>>>>> origin/main
from .schema import loans_ddl
from .snapshots import split_table
from .storage_metrics import measure
from .timeutil import utc_naive

NS = "lake.exp"


def _months(n: int, start: str = "2008-01") -> list[str]:
    p = pd.Period(start, freq="M")
    return [str(p + i) for i in range(n)]


def _create(spark, cfg, name: str, mode: str) -> str:
    table = f"{NS}.{name}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(loans_ddl(cfg, table=table, write_mode=mode))
    return table


def _append(spark, table, df_month, batch):
    cols = spark.table(table).columns
    df_month.drop("issue_month").withColumn("ingest_batch", F.lit(batch)).select(*cols).writeTo(table).append()


def _correct_previous(spark, table, prev_month: str, rate: float, salt: int):
    p = pd.Period(prev_month, freq="M")
    lo, hi = f"{p}-01", f"{p + 1}-01"
    k = int(round(rate * 10000))
    spark.sql(f"UPDATE {table} SET target = 1 - target WHERE issue_d >= DATE '{lo}' AND issue_d < DATE '{hi}' "
              f"AND pmod(hash(loan_id, {salt}), 10000) < {k}")


def _series(spark, cfg, df, months, name, mode, rate, checkpoints, exp):
    table = _create(spark, cfg, name, mode)
    rows = []
    for i, m in enumerate(months, 1):
        _append(spark, table, df.filter(F.col("issue_month") == m), m)
        if rate > 0 and i > 1:
            _correct_previous(spark, table, months[i - 2], rate, salt=i)
        if i in checkpoints:
            rows.append({"exp": exp, "mode": mode, "update_rate": rate, "month_index": i, **measure(spark, table)})
    return table, rows


def run_all(spark: SparkSession, cfg: Config, out_dir: str = "reports/storage", months: int = 24,
            rows_per_month: int = 2000, quick: bool = False, seed: int = 42,
            e1_checkpoints: list[int] | None = None) -> pd.DataFrame:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    e1_checkpoints = e1_checkpoints or ([12, 24] if quick else [12, 24, 48, 96])
    n_months = max(max(e1_checkpoints), months)
    csv = write_csv(str(out / "synthetic_for_storage.csv"), start=_months(1)[0], end=_months(n_months)[-1],
                    rows_per_month=rows_per_month, seed=seed)
    df = load_clean_frame(spark, cfg, csv).cache()
    ms = _months(n_months)
    all_rows: list[dict] = []

    # E1
<<<<<<< HEAD
    t_e1, rows = _series(spark, cfg, df, ms[:max(e1_checkpoints)], "e1_append_only", "copy-on-write", 0.0, set(e1_checkpoints), "E1")
    all_rows += rows
    snap_frames: dict = {}

    def per_snapshot(table, prefix, label, title):
        try:
            _, snap_frames[label] = snapshot_storage.write_report(spark, table, str(out), prefix=prefix, title=title)
        except Exception as e:                    # never lose the main experiment results over a reporting problem
            print(f"[storage] per-snapshot report for {table} skipped: {type(e).__name__}: {e}")

    per_snapshot(t_e1, "e1_snapshot_storage", "append-only only", "E1 append-only monthly loads")
=======
    _, rows = _series(spark, cfg, df, ms[:max(e1_checkpoints)], "e1_append_only", "copy-on-write", 0.0, set(e1_checkpoints), "E1")
    all_rows += rows
>>>>>>> origin/main
    # E2 / E3
    rates = [0.001, 0.01, 0.05, 0.10]
    for mode, tag in (("copy-on-write", "E2"), ("merge-on-read", "E3")):
        for rate in rates:
            nm = f"{tag.lower()}_{int(rate * 1000)}pm"
<<<<<<< HEAD
            t_x, rows = _series(spark, cfg, df, ms[:months], nm, mode, rate, {months}, tag)
            all_rows += rows
            if rate == 0.05:
                per_snapshot(t_x, f"{tag.lower()}_snapshot_storage_5pct", f"{mode} + 5% monthly corrections", f"{tag} {mode}, 5% corrected monthly")
=======
            _, rows = _series(spark, cfg, df, ms[:months], nm, mode, rate, {months}, tag)
            all_rows += rows
>>>>>>> origin/main
    # E4 + E5 on a merge-on-read 1% table
    table, rows = _series(spark, cfg, df, ms[:months], "e45_mor_1pct", "merge-on-read", 0.01, {months}, "E4")
    cat, name = split_table(table)
    before = rows[0]; before["exp"], before["stage"] = "E4", "before_compaction"; all_rows.append(before)
    spark.sql(f"CALL {cat}.system.rewrite_data_files(table => '{name}', strategy => 'binpack', "
              f"options => map('min-input-files','2'))")
    all_rows.append({"exp": "E4", "mode": "merge-on-read", "update_rate": 0.01, "month_index": months,
                     "stage": "after_compaction", **measure(spark, table)})
    far = (utc_naive() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"CALL {cat}.system.expire_snapshots(table => '{name}', older_than => TIMESTAMP '{far}', retain_last => 1)")
    all_rows.append({"exp": "E4", "mode": "merge-on-read", "update_rate": 0.01, "month_index": months,
                     "stage": "after_expire", **measure(spark, table)})
    # E5: rebuild a fresh table, then expire progressively (monotonic retain_last)
    table, _ = _series(spark, cfg, df, ms[:months], "e5_mor_1pct", "merge-on-read", 0.01, set(), "E5")
    cat, name = split_table(table)
    all_rows.append({"exp": "E5", "mode": "merge-on-read", "update_rate": 0.01, "month_index": months, "retain_last": "all",
                     **measure(spark, table)})
    for n in (30, 10, 5, 1):
        spark.sql(f"CALL {cat}.system.expire_snapshots(table => '{name}', older_than => TIMESTAMP '{far}', retain_last => {n})")
        all_rows.append({"exp": "E5", "mode": "merge-on-read", "update_rate": 0.01, "month_index": months, "retain_last": n,
                         **measure(spark, table)})
    df.unpersist()
<<<<<<< HEAD
    if len(snap_frames) >= 2:
        snapshot_storage.compare_charts(snap_frames, str(out / "snapshot_storage_compare.png"))
=======
>>>>>>> origin/main
    res = pd.DataFrame(all_rows)
    res.to_csv(out / "storage_results.csv", index=False)
    charts(res, out)
    return res


def charts(res: pd.DataFrame, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    e1 = res[res.exp == "E1"]
    if len(e1):
        fig, ax = plt.subplots(1, 2, figsize=(9, 3.4))
        ax[0].plot(e1.snapshots, e1.overhead_ratio, marker="o"); ax[0].set(xlabel="snapshots", ylabel="retained / live bytes (data)", title="E1 append-only: data overhead")
        ax[1].plot(e1.snapshots, e1.metadata_bytes / 1024, marker="o", color="tab:orange"); ax[1].set(xlabel="snapshots", ylabel="metadata KiB", title="E1: metadata growth")
        fig.tight_layout(); fig.savefig(out / "e1_append_only.png", dpi=150); plt.close(fig)

    e23 = res[res.exp.isin(["E2", "E3"])]
    if len(e23):
        fig, ax = plt.subplots(figsize=(5.5, 3.6))
        for mode, g in e23.groupby("mode"):
            g = g.sort_values("update_rate"); ax.plot(g.update_rate * 100, g.overhead_ratio, marker="o", label=mode)
        ax.set(xlabel="% of previous month corrected, each month", ylabel="retained / live bytes", title="E2/E3 overhead vs update rate"); ax.legend()
        fig.tight_layout(); fig.savefig(out / "e2_e3_update_rate.png", dpi=150); plt.close(fig)

    e4 = res[res.exp == "E4"].dropna(subset=["stage"]) if "stage" in res else res.iloc[0:0]
    if len(e4):
        fig, ax = plt.subplots(figsize=(5.5, 3.6))
        ax.bar(e4.stage, e4.retained_bytes / 1024); ax.set(ylabel="retained KiB", title="E4 compaction, then expiry"); plt.xticks(rotation=15)
        fig.tight_layout(); fig.savefig(out / "e4_compaction.png", dpi=150); plt.close(fig)

    e5 = res[res.exp == "E5"]
    if len(e5):
        fig, ax = plt.subplots(figsize=(5.5, 3.6))
        ax.plot(e5.retain_last.astype(str), e5.overhead_ratio, marker="o"); ax.set(xlabel="retain_last", ylabel="retained / live bytes", title="E5 expiry sweep")
        fig.tight_layout(); fig.savefig(out / "e5_expiry_sweep.png", dpi=150); plt.close(fig)
