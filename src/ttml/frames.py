"""Spark -> pandas conversion that does NOT use DataFrame.toPandas().

Why: PySpark 3.5.x converts timestamp columns with `ts.tz_localize("tzlocal()")`, which crashes
("'NoneType' object has no attribute 'total_seconds'") with pandas >= 3.1 whenever the machine's timezone is not UTC and the
TZ variable is unset, i.e. on essentially every Mac. Linux CI containers run in UTC, which hides it.

How: timestamp columns are collected as exact epoch microseconds (BIGINT) and converted to timezone-aware UTC in pandas, so no
local-timezone logic is involved anywhere. Everything else is collected as plain Python values.
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType, TimestampType

_TS = (TimestampType, TimestampNTZType)


def to_pandas(df: DataFrame) -> pd.DataFrame:
    """Collect `df` to the driver as a pandas DataFrame. Timestamps become timezone-aware UTC datetimes."""
    names = df.columns
    ts_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, _TS)]
    if ts_cols:
        df = df.select(*[F.expr(f"unix_micros(`{c}`)").alias(c) if c in ts_cols else F.col(f"`{c}`").alias(c) for c in names])
    rows = df.collect()
    pdf = pd.DataFrame.from_records([tuple(r) for r in rows], columns=names) if rows else pd.DataFrame({c: [] for c in names})
    for c in ts_cols:
        pdf[c] = pd.to_datetime(pd.to_numeric(pdf[c]), unit="us", utc=True)
    return pdf
