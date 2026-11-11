"""SparkSession factory with the Iceberg runtime and a named catalog."""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from .config import Config


def _pin_python() -> None:
    """Make Spark's Python workers use THIS interpreter (the venv). On Windows the default 'python3' is often missing or
    the Microsoft Store alias, which is a classic cause of 'Python worker exited unexpectedly'. setdefault: an explicit
    PYSPARK_PYTHON always wins (the test suite sets a deliberately missing one, see tests/conftest.py)."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(cfg: Config, warehouse: str | None = None, app_name: str = "ttml") -> SparkSession:
    _pin_python()
    c = cfg.raw["catalog"]
    cat = c["name"]
    wh = warehouse or str(cfg.path(c["warehouse"]))
    pkg = f"org.apache.iceberg:iceberg-spark-runtime-{c['spark_minor']}_{c['scala']}:{c['iceberg_version']}"
    b = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", pkg)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cat}.type", c.get("type", "hadoop"))
        .config(f"spark.sql.catalog.{cat}.warehouse", wh)
<<<<<<< HEAD
        .config("spark.driver.memory", str(cfg.raw.get("spark", {}).get("driver_memory", "4g")))   # real LendingClub data needs more than the 1g default
=======
>>>>>>> origin/main
        .config("spark.driver.bindAddress", "127.0.0.1")      # local mode: avoids hostname/VPN binding errors on macs
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")          # data is small; avoid 200 tiny tasks
        .config("spark.sql.session.timeZone", "UTC")          # timestamps must not depend on host TZ
        .config("spark.sql.ansi.enabled", "false")
    )
    return b.getOrCreate()


def get_plain_spark(app_name: str = "ttml-plain") -> SparkSession:
    """Local Spark WITHOUT Iceberg. Used for tests of pure DataFrame logic (cleaning, validation, fingerprint)."""
    _pin_python()
    return (
        SparkSession.builder.master("local[2]").appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
