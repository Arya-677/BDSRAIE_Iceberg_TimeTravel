"""Per-snapshot storage metrics. The numbers below are hand-built fixtures that exercise the arithmetic; real figures come from
Iceberg's own snapshot summaries (see tests/test_reproducibility_iceberg.py::test_snapshot_storage_report_on_real_iceberg)."""
import math

import pandas as pd
import pytest

from ttml import snapshot_storage as ss


def raw(rows):
    """rows: (id, parent, ts_us, operation, total, added, removed)"""
    return pd.DataFrame([{"snapshot_id": i, "parent_id": p, "committed_us": t, "operation": op, "total_files_size": str(tot),
                          "added_files_size": None if a is None else str(a), "removed_files_size": None if r is None else str(r),
                          "total_records": str(tot // 10), "added_records": None, "deleted_records": None, "total_data_files": "1",
                          "added_data_files": None, "deleted_data_files": None, "total_delete_files": "0"}
                         for i, p, t, op, tot, a, r in rows])


APPENDS = raw([(11, None, 1_000, "append", 1000, 1000, 0), (12, 11, 2_000, "append", 2000, 1000, 0), (13, 12, 3_000, "append", 3500, 1500, 0)])
# a copy-on-write style update: rewrites 400 bytes of files (adds 400, removes 400) -> table size unchanged, history grows
WITH_UPDATE = raw([(11, None, 1_000, "append", 1000, 1000, 0), (12, 11, 2_000, "append", 2000, 1000, 0), (13, 12, 3_000, "overwrite", 2000, 400, 400)])


def test_append_only_history_costs_exactly_the_table_and_shares_everything_old():
    d = ss.derive(APPENDS)
    assert d["table_bytes"].tolist() == [1000, 2000, 3500]
    assert d["added_bytes"].tolist() == [1000, 1000, 1500]
    assert d["cumulative_retained_bytes"].tolist() == [1000, 2000, 3500]              # no overhead: every file used by the head
    assert d["full_copy_cumulative_bytes"].tolist() == [1000, 3000, 6500]
    assert d["history_overhead_ratio"].tolist() == [1.0, 1.0, 1.0]
    assert [round(x, 4) for x in d["shared_pct"]] == [0.0, 50.0, 57.1429]
    s = ss.summarize(d)
    assert s["snapshots"] == 3 and s["savings_factor"] == pytest.approx(6500 / 3500) and s["largest_commit_snapshot_no"] == 3


def test_update_rewrites_files_so_history_costs_more_than_the_live_table():
    d = ss.derive(WITH_UPDATE)
    assert d["table_bytes"].iloc[-1] == 2000 and d["net_change_bytes"].iloc[-1] == 0          # table did not grow...
    assert d["cumulative_retained_bytes"].iloc[-1] == 2400                                     # ...but 400 more bytes are kept
    assert d["history_overhead_ratio"].iloc[-1] == pytest.approx(1.2)
    assert d["removed_bytes"].iloc[-1] == 400 and d["operation"].iloc[-1] == "overwrite"
    assert ss.summarize(d)["operations"] == {"append": 2, "overwrite": 1} and ss.summarize(d)["total_removed_bytes"] == 400


def test_rows_are_sorted_by_commit_time_whatever_the_input_order():
    d = ss.derive(APPENDS.iloc[::-1])
    assert d["snapshot_id"].tolist() == [11, 12, 13] and d["snapshot_no"].tolist() == [1, 2, 3]


def test_missing_sizes_raise_a_clear_error():
    bad = APPENDS.copy(); bad.loc[1, "total_files_size"] = None
    with pytest.raises(ValueError, match="did not record 'total-files-size'"):
        ss.derive(bad)
    with pytest.raises(ValueError, match="no snapshots"):
        ss.derive(APPENDS.iloc[0:0])


def test_missing_added_removed_default_to_zero():
    r = APPENDS.copy(); r.loc[2, "removed_files_size"] = None
    assert ss.derive(r)["removed_bytes"].iloc[2] == 0


def test_diff_between_two_snapshots_in_either_order():
    d = ss.derive(WITH_UPDATE)
    x = ss.diff_between(d, 11, 13)
    assert (x["commits_between"], x["net_change_bytes"], x["bytes_added_between"], x["bytes_removed_between"]) == (2, 1000, 1400, 400)
    assert x["swapped"] is False and ss.diff_between(d, 13, 11)["swapped"] is True and ss.diff_between(d, 13, 11)["net_change_bytes"] == 1000
    assert ss.diff_between(d, 12, 12)["commits_between"] == 0
    with pytest.raises(ValueError, match="not in the table's history"):
        ss.diff_between(d, 11, 999)


def test_single_snapshot_table():
    d = ss.derive(APPENDS.iloc[:1])
    s = ss.summarize(d)
    assert s["snapshots"] == 1 and s["history_overhead_ratio"] == 1.0 and s["avg_shared_pct_after_first"] == 0.0


def test_zero_size_table_does_not_divide_by_zero():
    z = raw([(1, None, 1, "append", 0, 0, 0)])
    d = ss.derive(z)
    assert d["shared_pct"].iloc[0] == 0.0 and math.isnan(d["history_overhead_ratio"].iloc[0])


def test_fmt_bytes():
    assert ss.fmt_bytes(0) == "0 B" and ss.fmt_bytes(1536) == "1.50 KiB" and ss.fmt_bytes(5 * 1024 ** 2) == "5.00 MiB" and ss.fmt_bytes(None) == "n/a"


def test_charts_are_written_and_non_empty(tmp_path):
    d = ss.derive(WITH_UPDATE)
    paths = ss.charts(d, tmp_path, "t", "demo table")
    assert [p.split("/")[-1] for p in paths] == ["t_growth.png", "t_changes.png"]
    assert all((tmp_path / n).stat().st_size > 5000 for n in ("t_growth.png", "t_changes.png"))
    cmp = ss.compare_charts({"append-only": ss.derive(APPENDS), "CoW update": d}, str(tmp_path / "cmp.png"))
    assert (tmp_path / "cmp.png").stat().st_size > 5000 and cmp.endswith("cmp.png")
