import pandas as pd

from ttml.rollback import flag_outlier_batches
from ttml.snapshots import ancestors_from_parents, descendants_from_parents

#   1 - 2 - 3 - 4 - 5          (5 = bad head)       after a rollback to 3 and a new commit: 3 - 6 - 7
PARENTS = {1: None, 2: 1, 3: 2, 4: 3, 5: 4, 6: 3, 7: 6}


def test_lineage_ancestors():
    assert ancestors_from_parents(PARENTS, 5) == [5, 4, 3, 2, 1]
    assert ancestors_from_parents(PARENTS, 7) == [7, 6, 3, 2, 1]
    assert ancestors_from_parents({5: 4}, 5) == [5, 4]                           # parent already expired


def test_lineage_descendants_include_self_and_exclude_siblings():
    assert descendants_from_parents(PARENTS, 4) == {4, 5}
    assert descendants_from_parents(PARENTS, 3) == {3, 4, 5, 6, 7}
    assert descendants_from_parents(PARENTS, 6) == {6, 7}


def _stats(n=12):
    return pd.DataFrame({"batch": [f"2015-{i:02d}" for i in range(1, n + 1)], "rows": 500,
                         "label_rate": [0.15 + 0.004 * (i % 3) for i in range(n)],
                         "int_rate_mean": [13.0 + 0.1 * (i % 4) for i in range(n)], "int_rate_max": 28.0, "dup_ids": 0})


def test_outlier_batches_clean_history_flags_nothing():
    assert flag_outlier_batches(_stats()).empty


def test_outlier_batches_flags_unit_error_and_label_flip():
    s = _stats()
    s.loc[5, ["int_rate_mean", "int_rate_max"]] = [1300.0, 3100.0]
    s.loc[8, "label_rate"] = 0.50
    f = flag_outlier_batches(s)
    assert set(f["batch"]) == {"2015-06", "2015-09"}
    assert all(f["why"].str.len() > 0)


def test_outlier_batches_flags_duplicates():
    s = _stats(); s.loc[2, "dup_ids"] = 4
    assert flag_outlier_batches(s)["batch"].tolist() == ["2015-03"]
