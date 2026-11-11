"""to_pandas(): replaces DataFrame.toPandas(), which crashes in PySpark 3.5 + pandas 3.1 + non-UTC system timezone (every Mac)."""
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from conftest import rows_df
from pyspark.sql import DataFrame

from ttml.frames import to_pandas

SRC = Path(__file__).resolve().parents[1] / "src" / "ttml"


def test_source_never_calls_topandas():
    offenders = [f"{p.name}:{i + 1}" for p in SRC.glob("*.py") if p.name != "frames.py"
                 for i, line in enumerate(p.read_text(encoding="utf-8").splitlines()) if ".toPandas(" in line]
    assert offenders == [], f"use ttml.frames.to_pandas (PySpark's toPandas breaks on non-UTC machines): {offenders}"


def test_to_pandas_does_not_call_dataframe_topandas(spark, monkeypatch):
    def boom(self, *a, **k):
        raise AssertionError("DataFrame.toPandas must not be used")
    monkeypatch.setattr(DataFrame, "toPandas", boom)
    ts = datetime(2026, 10, 29, 9, 0, 0, 123456, tzinfo=timezone.utc)
    out = to_pandas(rows_df(spark, [(ts, 1)], "t timestamp, i bigint"))
    assert out.shape == (1, 2)


def test_timestamp_is_exact_and_timezone_aware_utc(spark):
    ist = timezone(timedelta(hours=5, minutes=30))
    t1 = datetime(2026, 10, 29, 14, 30, 0, 999999, tzinfo=ist)
    out = to_pandas(rows_df(spark, [(t1,), (None,)], "t timestamp"))
    got = out["t"].iloc[0]
    assert str(out["t"].dt.tz) == "UTC"
    assert got.to_pydatetime() == t1 and got.microsecond == 999999
    assert pd.isna(out["t"].iloc[1])


def test_other_types_and_nulls(spark):
    df = rows_df(spark, [(1, 1.5, "a", date(2016, 2, 29), True), (None, None, None, None, None)],
                 "i bigint, d double, s string, dt date, b boolean")
    out = to_pandas(df)
    assert list(out.columns) == ["i", "d", "s", "dt", "b"]
    assert out["i"].iloc[0] == 1 and out["d"].iloc[0] == 1.5 and out["s"].iloc[0] == "a" and out["dt"].iloc[0] == date(2016, 2, 29)
    assert pd.isna(out["i"].iloc[1]) and pd.isna(out["d"].iloc[1]) and pd.isna(out["s"].iloc[1])


def test_ints_without_nulls_stay_integers_and_nan_is_nan(spark):
    out = to_pandas(rows_df(spark, [(1, float("nan")), (2, 2.0)], "i bigint, d double"))
    assert str(out["i"].dtype).startswith("int") and math.isnan(out["d"].iloc[0])


def test_empty_frame_keeps_columns(spark):
    out = to_pandas(rows_df(spark, [], "a int, t timestamp"))
    assert list(out.columns) == ["a", "t"] and len(out) == 0


def test_column_names_needing_quotes(spark):
    out = to_pandas(rows_df(spark, [(1, 2)], "`a.b` int, `c d` int"))
    assert list(out.columns) == ["a.b", "c d"] and out.iloc[0].tolist() == [1, 2]


def test_snapshot_listing_shape_matches_what_callers_expect(spark):
    now = datetime.now(timezone.utc)
    df = rows_df(spark, [(2, 1, now, "append"), (1, None, now - timedelta(seconds=5), "append")],
                 "snapshot_id bigint, parent_id bigint, committed_at timestamp, operation string")
    out = to_pandas(df.orderBy("committed_at"))
    assert out["snapshot_id"].tolist() == [1, 2] and out["committed_at"].is_monotonic_increasing
