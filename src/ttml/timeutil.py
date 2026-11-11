"""Time helpers. PySpark interprets NAIVE datetimes in the Python process's LOCAL timezone, so every datetime we
send to Spark must be timezone-aware (UTC). Naive-UTC values are only used to format SQL literals, because the
Spark session timezone is pinned to UTC (see spark_session.py)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_naive() -> datetime:
    return utc_now().replace(tzinfo=None)


def from_micros(us: int) -> datetime:
    """Exact microseconds-since-epoch -> aware UTC datetime."""
    return _EPOCH + timedelta(microseconds=int(us))
