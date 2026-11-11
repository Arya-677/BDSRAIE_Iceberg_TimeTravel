from conftest import rows_df
from ttml.snapshots import fingerprint

SCHEMA = "id bigint, a string, b string, x double"


def mk(spark, rows):
    return rows_df(spark, rows, SCHEMA)


def test_same_rows_any_order_same_fingerprint(spark):
    rows = [(1, "p", "q", 1.5), (2, "r", None, 2.5), (3, None, "s", 3.5)]
    assert fingerprint(mk(spark, rows)) == fingerprint(mk(spark, list(reversed(rows))))


def test_column_order_does_not_matter(spark):
    df = mk(spark, [(1, "p", "q", 1.5)])
    assert fingerprint(df) == fingerprint(df.select("x", "b", "a", "id"))


def test_value_change_changes_fingerprint(spark):
    a = mk(spark, [(1, "p", "q", 1.5), (2, "r", "s", 2.5)])
    b = mk(spark, [(1, "p", "q", 1.5), (2, "r", "s", 2.6)])
    assert fingerprint(a) != fingerprint(b)


def test_row_added_or_removed_changes_fingerprint(spark):
    a = mk(spark, [(1, "p", "q", 1.5)])
    b = mk(spark, [(1, "p", "q", 1.5), (2, "r", "s", 2.5)])
    fa, fb = fingerprint(a), fingerprint(b)
    assert fa["rows"] == 1 and fb["rows"] == 2 and fa["hash"] != fb["hash"]


def test_null_cannot_move_between_columns(spark):
    a = mk(spark, [(1, None, "z", 1.0)])
    b = mk(spark, [(1, "z", None, 1.0)])
    assert fingerprint(a)["hash"] != fingerprint(b)["hash"]


def test_null_differs_from_string_null_and_empty(spark):
    h = lambda v: fingerprint(mk(spark, [(1, v, "q", 1.0)]))["hash"]  # noqa: E731
    assert len({h(None), h("NULL"), h("")}) == 3


def test_duplicate_rows_are_counted(spark):
    one = mk(spark, [(1, "p", "q", 1.5)])
    two = mk(spark, [(1, "p", "q", 1.5), (1, "p", "q", 1.5)])
    assert fingerprint(one) != fingerprint(two)


def test_empty_frame(spark):
    assert fingerprint(mk(spark, [])) == {"rows": 0, "hash": "0"}
