"""Deliberately bad data, for the rollback demo and tests."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def make_poisoned(df_month: DataFrame, mode: str = "units") -> DataFrame:
    """mode='units': int_rate off by x100 (a unit/loader bug) and half the labels flipped.
    The ingest gate would reject this; the demo bypasses the gate to simulate a failure that slipped through."""
    if mode == "units":
        return (df_month.withColumn("int_rate", F.col("int_rate") * 100.0)
                .withColumn("target", F.when(F.pmod(F.col("loan_id"), F.lit(2)) == 0, 1 - F.col("target")).otherwise(F.col("target"))))
    if mode == "duplicate":
        return df_month.withColumn("loan_id", F.col("loan_id") - F.lit(0))  # same ids as a month already loaded
    raise ValueError(mode)
