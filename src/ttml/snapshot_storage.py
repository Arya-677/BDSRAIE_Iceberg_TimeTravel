"""Storage difference BETWEEN snapshots: how big is the table at each snapshot, what did each commit add / remove, how much is
shared with the previous snapshot, and what does keeping all the history cost compared with keeping full copies?

Source of truth: Iceberg itself records these numbers in every snapshot's summary map when the commit is made
(`total-files-size`, `added-files-size`, `removed-files-size`, `total-records`, `added-records`, ...), so no data is scanned.

Definitions (bytes of data + delete files):
  table_bytes                  size of the table AS OF this snapshot (what a time-travel reader sees)
  added_bytes / removed_bytes  files this commit added / dropped from the table
  shared_with_previous_bytes   table_bytes - added_bytes  (files carried over unchanged from the previous snapshot)
  cumulative_retained_bytes    running sum of added_bytes = what is actually stored if NO snapshot has been expired
                               (files are immutable and shared, so each file is stored once however many snapshots use it)
  full_copy_cumulative_bytes   running sum of table_bytes = what it would cost if every snapshot were a separate full copy
  savings_factor               full_copy_cumulative_bytes / cumulative_retained_bytes
Caveat: after expire_snapshots the cumulative columns describe the snapshots that still exist, not the original history.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .frames import to_pandas

SUMMARY_KEYS = {
    "total_files_size": "total-files-size", "added_files_size": "added-files-size", "removed_files_size": "removed-files-size",
    "total_records": "total-records", "added_records": "added-records", "deleted_records": "deleted-records",
    "total_data_files": "total-data-files", "added_data_files": "added-data-files", "deleted_data_files": "deleted-data-files",
    "total_delete_files": "total-delete-files",
}
UNITS = [("B", 1), ("KiB", 1024), ("MiB", 1024 ** 2), ("GiB", 1024 ** 3)]


def fmt_bytes(b) -> str:
    if b is None or (isinstance(b, float) and pd.isna(b)):
        return "n/a"
    b = float(b)
    for name, div in reversed(UNITS):
        if abs(b) >= div or name == "B":
            return f"{int(b)} B" if name == "B" else f"{b / div:.2f} {name}"
    return f"{b} B"


def read_summaries(spark, table: str) -> pd.DataFrame:
    cols = ", ".join(f"summary['{k}'] AS {c}" for c, k in SUMMARY_KEYS.items())
    return to_pandas(spark.sql(
        f"SELECT snapshot_id, parent_id, unix_micros(committed_at) AS committed_us, operation, {cols} FROM {table}.snapshots ORDER BY committed_at"))


def derive(raw: pd.DataFrame) -> pd.DataFrame:
    """Raw snapshot rows (summary values may be strings/None) -> one row per snapshot with the storage metrics above."""
    d = raw.sort_values("committed_us", kind="stable").reset_index(drop=True).copy()
    if d.empty:
        raise ValueError("the table has no snapshots yet")
    for c in SUMMARY_KEYS:
        d[c] = pd.to_numeric(d[c], errors="coerce")
    if d["total_files_size"].isna().any():
        raise ValueError("Iceberg did not record 'total-files-size' in the snapshot summaries of this table, so per-snapshot sizes "
                         "cannot be computed. (Summaries are written at commit time; tables written by very old writers may lack them.)")
    d["snapshot_no"] = range(1, len(d) + 1)
    d["committed_at"] = pd.to_datetime(d["committed_us"], unit="us", utc=True)
    d["table_bytes"] = d["total_files_size"]
    d["added_bytes"] = d["added_files_size"].fillna(0)
    d["removed_bytes"] = d["removed_files_size"].fillna(0)
    d["net_change_bytes"] = d["table_bytes"].diff().fillna(d["table_bytes"])
    d["shared_with_previous_bytes"] = (d["table_bytes"] - d["added_bytes"]).clip(lower=0)
    d["shared_pct"] = (d["shared_with_previous_bytes"] / d["table_bytes"].where(d["table_bytes"] > 0) * 100).fillna(0.0)
    d["cumulative_retained_bytes"] = d["added_bytes"].cumsum()
    d["full_copy_cumulative_bytes"] = d["table_bytes"].cumsum()
    d["history_overhead_ratio"] = d["cumulative_retained_bytes"] / d["table_bytes"].where(d["table_bytes"] > 0)
    d["records"] = d["total_records"]
    d["added_records_n"] = d["added_records"].fillna(0)
    d["data_files"] = d["total_data_files"]
    keep = ["snapshot_no", "snapshot_id", "parent_id", "committed_at", "operation", "table_bytes", "added_bytes", "removed_bytes",
            "net_change_bytes", "shared_with_previous_bytes", "shared_pct", "cumulative_retained_bytes", "full_copy_cumulative_bytes",
            "history_overhead_ratio", "records", "added_records_n", "data_files"]
    return d[keep]


def summarize(d: pd.DataFrame) -> dict:
    """Headline numbers for a derived frame (also works on a frame re-read from the CSV)."""
    n = len(d)
    last = d.iloc[-1]
    later = d.iloc[1:] if n > 1 else d
    big = d.loc[d["added_bytes"].idxmax()]
    return {
        "snapshots": int(n),
        "first_table_bytes": float(d.iloc[0]["table_bytes"]), "last_table_bytes": float(last["table_bytes"]),
        "cumulative_retained_bytes": float(last["cumulative_retained_bytes"]),
        "full_copy_cumulative_bytes": float(last["full_copy_cumulative_bytes"]),
        "savings_factor": float(last["full_copy_cumulative_bytes"] / last["cumulative_retained_bytes"]) if last["cumulative_retained_bytes"] else None,
        "history_overhead_ratio": float(last["history_overhead_ratio"]) if pd.notna(last["history_overhead_ratio"]) else None,
        "avg_shared_pct_after_first": float(later["shared_pct"].mean()),
        "largest_commit_bytes": float(big["added_bytes"]), "largest_commit_snapshot_no": int(big["snapshot_no"]),
        "total_removed_bytes": float(d["removed_bytes"].sum()),
        "operations": {str(k): int(v) for k, v in d["operation"].value_counts().items()},
    }


def diff_between(d: pd.DataFrame, a: int, b: int) -> dict:
    """Storage difference between two snapshots (ids). Order does not matter; the earlier one is used as the baseline."""
    ids = d["snapshot_id"].astype("int64").tolist()
    for x in (a, b):
        if int(x) not in ids:
            raise ValueError(f"snapshot {x} is not in the table's history (expired, or wrong table)")
    ra, rb = (d[d["snapshot_id"] == int(x)].iloc[0] for x in (a, b))
    first, second = (ra, rb) if ra["snapshot_no"] <= rb["snapshot_no"] else (rb, ra)
    between = d[(d["snapshot_no"] > first["snapshot_no"]) & (d["snapshot_no"] <= second["snapshot_no"])]
    return {"from_snapshot": int(first["snapshot_id"]), "to_snapshot": int(second["snapshot_id"]), "swapped": bool(ra["snapshot_no"] > rb["snapshot_no"]),
            "commits_between": int(len(between)), "table_bytes_from": float(first["table_bytes"]), "table_bytes_to": float(second["table_bytes"]),
            "net_change_bytes": float(second["table_bytes"] - first["table_bytes"]),
            "bytes_added_between": float(between["added_bytes"].sum()), "bytes_removed_between": float(between["removed_bytes"].sum()),
            "records_from": float(first["records"]) if pd.notna(first["records"]) else None,
            "records_to": float(second["records"]) if pd.notna(second["records"]) else None}


# ---------------------------------------------------------------- charts
def _unit(max_bytes: float) -> tuple[str, float]:
    for name, div in reversed(UNITS):
        if max_bytes >= div:
            return name, div
    return "B", 1


def charts(d: pd.DataFrame, out_dir: Path, prefix: str, title: str) -> list[str]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    x = d["snapshot_no"]
    unit, div = _unit(float(max(d["full_copy_cumulative_bytes"].max(), 1)))
    paths = []

    # 1. growth: what a snapshot looks like vs what is stored vs what full copies would cost
    fig, ax = plt.subplots(figsize=(8.5, 4.4))
    ax.plot(x, d["full_copy_cumulative_bytes"] / div, "--", color="tab:red", label="if every snapshot were a full copy")
    ax.plot(x, d["cumulative_retained_bytes"] / div, "-", color="tab:green", linewidth=2.4, label="actually stored (files shared between snapshots)")
    ax.plot(x, d["table_bytes"] / div, "-", color="tab:blue", label="table size as of this snapshot")
    ax.fill_between(x, d["cumulative_retained_bytes"] / div, d["full_copy_cumulative_bytes"] / div, color="tab:red", alpha=0.08)
    s = summarize(d)
    sub = f"history costs {s['history_overhead_ratio']:.2f}x the current table; sharing saves {s['savings_factor']:.1f}x vs full copies" \
        if s["savings_factor"] and s["history_overhead_ratio"] else ""
    ax.set(xlabel="snapshot number (oldest to newest)", ylabel=f"storage ({unit})", title=f"{title}: storage by snapshot\n{sub}")
    ax.legend(fontsize=8, loc="upper left"); ax.grid(alpha=0.25)
    fig.tight_layout(); p = out_dir / f"{prefix}_growth.png"; fig.savefig(p, dpi=150); plt.close(fig); paths.append(str(p))

    # 2. what each commit changed: bytes added / removed, and how much of the snapshot is shared with the previous one
    fig, ax = plt.subplots(figsize=(8.5, 4.4))
    ax.bar(x, d["added_bytes"] / div, color="tab:green", label="bytes added by this commit")
    ax.bar(x, -d["removed_bytes"] / div, color="tab:red", label="bytes removed from the table by this commit")
    ax.axhline(0, color="black", linewidth=0.6)
    other = d[d["operation"] != "append"]
    if len(other):
        ax.scatter(other["snapshot_no"], [0] * len(other), marker="^", color="black", zorder=5, label="update / rewrite commit (not a plain append)")
    ax2 = ax.twinx()
    ax2.plot(x, d["shared_pct"], color="tab:blue", marker="o", markersize=3, label="% of snapshot shared with previous")
    ax2.set_ylim(0, 105); ax2.set_ylabel("% shared with previous snapshot")
    ax.set(xlabel="snapshot number (oldest to newest)", ylabel=f"bytes changed ({unit})", title=f"{title}: what each snapshot changed")
    h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, fontsize=7, loc="upper left"); ax.grid(alpha=0.25)
    fig.tight_layout(); p = out_dir / f"{prefix}_changes.png"; fig.savefig(p, dpi=150); plt.close(fig); paths.append(str(p))
    return paths


def compare_charts(frames: dict, path: str, title: str = "Storage kept as snapshots accumulate") -> str:
    """One line per scenario (e.g. append-only vs copy-on-write vs merge-on-read): cumulative retained bytes by snapshot."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    top = max(float(f["cumulative_retained_bytes"].max()) for f in frames.values())
    unit, div = _unit(max(top, 1))
    fig, ax = plt.subplots(figsize=(8.5, 4.4))
    for name, f in frames.items():
        ax.plot(f["snapshot_no"], f["cumulative_retained_bytes"] / div, marker="o", markersize=2.5, label=name)
    ax.set(xlabel="snapshot number", ylabel=f"storage actually kept ({unit})", title=title)
    ax.legend(fontsize=8); ax.grid(alpha=0.25)
    fig.tight_layout(); fig.savefig(path, dpi=150); plt.close(fig)
    return str(path)


def write_report(spark, table: str, out_dir: str, prefix: str = "snapshot_storage", title: str | None = None):
    """Compute the per-snapshot storage table for `table`, write <prefix>.csv and two charts into out_dir.
    Returns (summary dict, derived DataFrame)."""
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    d = derive(read_summaries(spark, table))
    d.to_csv(out / f"{prefix}.csv", index=False)
    summary = summarize(d)
    summary["files"] = [str(out / f"{prefix}.csv")] + charts(d, out, prefix, title or table)
    return summary, d
