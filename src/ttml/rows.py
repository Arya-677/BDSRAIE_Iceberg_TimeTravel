"""Build small DataFrames from Python rows WITHOUT a Python worker process.

Why this exists: spark.createDataFrame(list_of_rows) routes the data through a Python RDD, so the first action needs a
Python worker process (pyspark.daemon). On Windows (and some macOS/venv/firewall setups) that worker fails to start,
giving "Python worker exited unexpectedly" / "EOFException ... reading the port number from pyspark.daemon's stdout".
Every audit-table write in this project (model registry, status events, ingest log, rollback log) used to go through
createDataFrame, so the pipeline itself would have failed there, not just the tests.

How: each row is rendered as typed SQL literals and evaluated by the JVM only:  SELECT CAST(..) AS a, ... UNION ALL ...
Rules: datetimes MUST be timezone-aware (a naive one would be ambiguous), strings are escaped, floats keep NaN/Infinity,
and None becomes a typed NULL. Intended for small frames (audit rows, test fixtures), not bulk data.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (ArrayType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType,
                               IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampType,
                               _parse_datatype_string)

MAX_ROWS = 500
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _q(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _lit(v, dt: DataType) -> str:
    t = dt.simpleString()
    if v is None:
        return f"CAST(NULL AS {t})"
    if isinstance(dt, StringType):
        return _q(str(v))
    if isinstance(dt, (ByteType, ShortType, IntegerType, LongType)):
        if isinstance(v, bool):
            raise TypeError(f"expected an integer for {t}, got bool {v!r}")
        return f"CAST({int(v)} AS {t})"
    if isinstance(dt, (FloatType, DoubleType)):
        f = float(v)
        s = "NaN" if math.isnan(f) else ("Infinity" if f > 0 else "-Infinity") if math.isinf(f) else repr(f)
        return f"CAST('{s}' AS {t})"
    if isinstance(dt, BooleanType):
        return f"CAST({'true' if v else 'false'} AS BOOLEAN)"
    if isinstance(dt, DateType):
        if isinstance(v, datetime):
            raise TypeError("expected a date, got a datetime")
        return f"CAST('{v.isoformat() if isinstance(v, date) else v}' AS DATE)"
    if isinstance(dt, TimestampType):
        if not isinstance(v, datetime):
            raise TypeError(f"expected a timezone-aware datetime for timestamp, got {type(v).__name__}")
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError("naive datetime rejected: pass a timezone-aware datetime (e.g. datetime.now(timezone.utc)); "
                             "naive values are ambiguous and get reinterpreted in the local timezone")
        return f"timestamp_micros({(v - _EPOCH) // timedelta(microseconds=1)})"
    if isinstance(dt, DecimalType):
        return f"CAST('{v}' AS {t})"
    if isinstance(dt, StructType):
        vals = [v.get(f.name) for f in dt.fields] if isinstance(v, dict) else list(v)
        if len(vals) != len(dt.fields):
            raise ValueError(f"struct value has {len(vals)} fields, schema has {len(dt.fields)}")
        return "named_struct(" + ", ".join(f"{_q(f.name)}, {_lit(x, f.dataType)}" for f, x in zip(dt.fields, vals)) + ")"
    if isinstance(dt, MapType):
        if not v:
            return f"CAST(map() AS {t})"
        return "map(" + ", ".join(f"{_lit(k, dt.keyType)}, {_lit(x, dt.valueType)}" for k, x in v.items()) + ")"
    if isinstance(dt, ArrayType):
        if not v:
            return f"CAST(array() AS {t})"
        return "array(" + ", ".join(_lit(x, dt.elementType) for x in v) + ")"
    raise TypeError(f"unsupported type for frame_from_rows: {t}")


def frame_from_rows(spark: SparkSession, rows, schema) -> DataFrame:
    """rows: tuples/lists/dicts/Rows. schema: DDL string ('a string, b bigint') or StructType."""
    st = schema if isinstance(schema, StructType) else _parse_datatype_string(schema)
    if not isinstance(st, StructType):
        raise TypeError("schema must describe a struct (a list of columns)")
    rows = list(rows)
    if len(rows) > MAX_ROWS:
        raise ValueError(f"frame_from_rows is for small frames (<= {MAX_ROWS} rows); got {len(rows)}. Write a file instead.")
    names = [f.name for f in st.fields]
    if not rows:
        sel = ", ".join(f"CAST(NULL AS {f.dataType.simpleString()}) AS {_ident(f.name)}" for f in st.fields)
        return spark.sql(f"SELECT {sel} WHERE 1 = 0")
    parts = []
    for r in rows:
        if isinstance(r, dict):
            extra = set(r) - set(names)
            if extra:
                raise ValueError(f"row has fields not in the schema: {sorted(extra)}")
            vals = [r.get(n) for n in names]
        else:
            vals = list(r)
            if len(vals) != len(names):
                raise ValueError(f"row has {len(vals)} values, schema has {len(names)} columns: {r!r}")
        parts.append("SELECT " + ", ".join(f"{_lit(v, f.dataType)} AS {_ident(f.name)}" for v, f in zip(vals, st.fields)))
    return spark.sql(" UNION ALL ".join(parts))
