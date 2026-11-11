"""Regression tests for the Windows 'Python worker exited unexpectedly' failure.

Root cause: spark.createDataFrame(list_of_rows) needs a Python worker, which crashes on some Windows setups. The whole
test session runs with a deliberately MISSING worker executable (see conftest.py), so these tests fail on any platform
if code starts needing one again."""
import hashlib
import math
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from conftest import rows_df

from ttml import registry
from ttml.registry import EVENT_FIELDS, INGEST_LOG_FIELDS, REGISTRY_FIELDS, ROLLBACK_FIELDS, row_frame
from ttml.rows import MAX_ROWS, frame_from_rows
from ttml.train import save_artifact

SRC = Path(__file__).resolve().parents[1] / "src" / "ttml"


# ---- the guard itself must be live ---------------------------------------------------------------
@pytest.mark.skipif(os.environ.get("TTML_ALLOW_PYTHON_WORKERS") == "1", reason="guard disabled")
def test_python_worker_guard_is_live(spark):
    """If this stops raising, the suite no longer proves anything about worker independence."""
    with pytest.raises(Exception):
        spark.createDataFrame([(1,), (2,)], "a int").collect()


def test_source_never_uses_createdataframe():
    offenders = [f"{p.name}:{i + 1}" for p in SRC.glob("*.py") if p.name != "rows.py"
                 for i, line in enumerate(p.read_text(encoding="utf-8").splitlines()) if "createDataFrame(" in line]
    assert offenders == [], f"use ttml.rows.frame_from_rows (no Python worker) instead: {offenders}"


# ---- frame_from_rows: types round-trip ------------------------------------------------------------
def test_scalar_types_roundtrip_including_nulls(spark):
    ts = datetime(2026, 10, 29, 12, 34, 56, 123456, tzinfo=timezone(timedelta(hours=5, minutes=30)))     # IST, aware
    df = rows_df(spark, [("a", 1, 2**40, 1.5, True, date(2016, 2, 29), ts), (None,) * 7],
                 "s string, i int, l bigint, d double, b boolean, dt date, t timestamp")
    r1, r2 = df.orderBy("s").collect()[::-1]            # non-null row first
    assert (r1.s, r1.i, r1.l, r1.d, r1.b, r1.dt) == ("a", 1, 2**40, 1.5, True, date(2016, 2, 29))
    assert all(v is None for v in r2)
    micros = df.where("s = 'a'").selectExpr("unix_micros(t)").first()[0]
    assert micros == (ts - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)       # exact, tz-correct


def test_naive_datetime_is_rejected(spark):
    with pytest.raises(ValueError, match="naive datetime rejected"):
        rows_df(spark, [(datetime(2026, 1, 1, 0, 0, 0),)], "t timestamp")


@pytest.mark.parametrize("s", ["plain", "it's", "back\\slash", 'say "hi"', "semi;colon -- comment", "x'; DROP TABLE t; --",
                               "line1\nline2", "tab\there", "unicode \u00e9\u4e2d\u6587 \U0001f600", "100% {braces} $dollar `tick`", ""])
def test_string_escaping_roundtrip(spark, s):
    assert rows_df(spark, [(s,)], "s string").first()[0] == s


def test_special_floats(spark):
    vals = [0.0, -0.0, 1e-300, 1.7976931348623157e308, math.pi, float("inf"), float("-inf")]
    got = [r[0] for r in rows_df(spark, [(v,) for v in vals], "d double").collect()]
    assert got == vals and math.copysign(1, got[1]) == -1
    assert math.isnan(rows_df(spark, [(float("nan"),)], "d double").first()[0])


def test_struct_map_and_array(spark):
    df = rows_df(spark, [((7, "h"), {"a": "1", "b": "2"}, [1, 2, 3]), (None, {}, [])],
                 "st struct<rows: bigint, hash: string>, m map<string,string>, arr array<int>")
    a, b = df.collect()
    assert (a.st.rows, a.st.hash, a.m, a.arr) == (7, "h", {"a": "1", "b": "2"}, [1, 2, 3])
    assert b.st is None and b.m == {} and b.arr == []


def test_dict_rows_and_column_order(spark):
    df = rows_df(spark, [{"b": 2, "a": 1}, {"a": 3}], "a int, b int")
    assert [tuple(r) for r in df.collect()] == [(1, 2), (3, None)]


def test_empty_frame_keeps_schema(spark):
    df = rows_df(spark, [], "a int, b string")
    assert df.count() == 0 and df.schema.simpleString() == "struct<a:int,b:string>"


def test_bad_input_is_rejected_clearly(spark):
    with pytest.raises(ValueError, match="not in the schema"):
        rows_df(spark, [{"zzz": 1}], "a int")
    with pytest.raises(ValueError, match="values, schema has"):
        rows_df(spark, [(1, 2)], "a int")
    with pytest.raises(TypeError):
        rows_df(spark, [(True,)], "a int")
    with pytest.raises(ValueError, match="small frames"):
        frame_from_rows(spark, [(i,) for i in range(MAX_ROWS + 1)], "a int")


def test_many_rows_union_works(spark):
    assert rows_df(spark, [(i, str(i)) for i in range(MAX_ROWS)], "a int, s string").agg({"a": "sum"}).first()[0] == sum(range(MAX_ROWS))


# ---- the audit-table rows the real pipeline writes ------------------------------------------------
NOW = datetime(2026, 10, 29, 9, 0, 0, 5, tzinfo=timezone.utc)


def test_registry_row_frame_has_exact_schema_and_values(spark):
    rec = dict(model_version="v1", created_at=NOW, trained_by="t", table_name="lake.credit.loans", snapshot_id=8_000_000_000_000_000_001,
               snapshot_committed_at=NOW, data_filter="1 = 1", split_rule="time split", feature_list_sha256="ab",
               data_fingerprint=(1234, "-99999999999999999999"), params_json='{"a": 1}', seed=42, git_commit="abc", git_dirty=False,
               env_lock_sha256="cd", lib_versions={"python": "3.12.1", "lightgbm": "4.7.0"}, metrics_json="{}",
               artifact_uri="/x/model.txt", artifact_sha256="ef")
    df = row_frame(spark, REGISTRY_FIELDS, rec)
    assert [f.name for f in df.schema.fields] == [n for n, _ in REGISTRY_FIELDS]
    r = df.first()
    assert r.snapshot_id == 8_000_000_000_000_000_001 and r.data_fingerprint.rows == 1234
    assert r.data_fingerprint.hash == "-99999999999999999999" and r.lib_versions["lightgbm"] == "4.7.0" and r.git_dirty is False
    assert df.selectExpr("unix_micros(created_at)").first()[0] == (NOW - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)


def test_other_audit_rows_build(spark):
    ev = row_frame(spark, EVENT_FIELDS, dict(model_version="v1", status="quarantined", reason="it's bad", actor="a", event_time=NOW)).first()
    assert ev.reason == "it's bad"
    il = row_frame(spark, INGEST_LOG_FIELDS, dict(batch_id="2015-01", source_file="C:\\data\\x.csv", source_sha256="s", rows_in=5,
                                                  snapshot_id_after=1, table_rows_after=None, table_hash_after=None, gate_bypassed=False,
                                                  validation_json='{"ok": true}', loaded_at=NOW)).first()
    assert il.source_file == "C:\\data\\x.csv" and il.table_rows_after is None            # Windows path survives escaping
    rb = row_frame(spark, ROLLBACK_FIELDS, dict(event_time=NOW, table_name="t", from_snapshot_id=2, to_snapshot_id=1, bad_batch="b",
                                                reason="r", actor="a", models_quarantined="v3,v4", head_fingerprint_verified=True)).first()
    assert rb.models_quarantined == "v3,v4" and rb.head_fingerprint_verified is True


def test_audit_rows_reject_naive_datetimes(spark):
    with pytest.raises(ValueError, match="naive"):
        row_frame(spark, EVENT_FIELDS, dict(model_version="v1", status="candidate", reason="", actor="a", event_time=datetime(2026, 1, 1)))
    assert registry._now().tzinfo is not None


# ---- Windows newline: the hash we record must be the hash of the bytes on disk ---------------------
def test_saved_model_bytes_match_recorded_hash_and_have_no_crlf(tmp_path):
    text = "tree\nversion=v4\nnum_class=1\n"
    uri, sha = save_artifact(tmp_path, "v1", text, {"k": "v"})
    raw = Path(uri).read_bytes()
    assert raw == text.encode("utf-8") and b"\r" not in raw
    assert hashlib.sha256(raw).hexdigest() == sha
    assert re.fullmatch(r"[0-9a-f]{64}", sha)
