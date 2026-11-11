"""Storage overhead of keeping snapshots.

  overhead_ratio = retained_bytes / live_bytes
    live_bytes     = data + delete files referenced by the CURRENT snapshot
    retained_bytes = every distinct data + delete file referenced by ANY retained snapshot
Metadata bytes (metadata.json, manifest lists, manifests) are reported separately and as a second ratio.
"""
from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import SparkSession


def table_location(spark: SparkSession, table: str) -> str | None:
    for r in spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect():
        if (r[0] or "").strip().lower() == "location":
            u = urlparse(r[1])
            return u.path if u.scheme in ("", "file") else None      # only local filesystems are measured on disk
    return None


def dir_bytes(path: str) -> int:
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file()) if path and os.path.isdir(path) else 0


def measure(spark: SparkSession, table: str) -> dict:
    live = spark.sql(f"""SELECT COALESCE(SUM(file_size_in_bytes),0) AS b,
                                COALESCE(SUM(CASE WHEN content = 0 THEN file_size_in_bytes ELSE 0 END),0) AS d
                         FROM {table}.files""").first()
    try:
        distinct_files = f"SELECT DISTINCT file_path, file_size_in_bytes, content FROM {table}.all_files"
        spark.sql(distinct_files).limit(1).collect()
    except Exception:                      # older Iceberg: no all_files; combine the two specific tables
        distinct_files = (f"SELECT file_path, file_size_in_bytes, content FROM {table}.all_data_files "
                          f"UNION SELECT file_path, file_size_in_bytes, content FROM {table}.all_delete_files")
    allf = spark.sql(f"""SELECT COALESCE(SUM(file_size_in_bytes),0) AS b,
                                COALESCE(SUM(CASE WHEN content = 0 THEN file_size_in_bytes ELSE 0 END),0) AS d
                         FROM ({distinct_files})""").first()
    snaps = spark.sql(f"SELECT COUNT(*) FROM {table}.snapshots").first()[0]
    loc = table_location(spark, table)
    meta = dir_bytes(os.path.join(loc, "metadata")) if loc else None
    on_disk_data = dir_bytes(os.path.join(loc, "data")) if loc else None
    live_b, ret_b = int(live["b"]), int(allf["b"])
    return {
        "snapshots": int(snaps),
        "live_bytes": live_b, "live_data_bytes": int(live["d"]), "live_delete_bytes": live_b - int(live["d"]),
        "retained_bytes": ret_b, "retained_data_bytes": int(allf["d"]), "retained_delete_bytes": ret_b - int(allf["d"]),
        "metadata_bytes": meta, "data_dir_bytes": on_disk_data,
        "overhead_ratio": (ret_b / live_b) if live_b else None,
        "overhead_ratio_incl_metadata": ((ret_b + (meta or 0)) / live_b) if live_b else None,
        "orphan_bytes": (on_disk_data - ret_b) if on_disk_data is not None else None,
    }
