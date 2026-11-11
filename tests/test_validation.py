from conftest import rows_df
from ttml.validation import compute_stats, evaluate_stats

T = dict(min_rows=50, max_null_rate=0.05, label_rate_min=0.01, label_rate_max=0.60,
         int_rate_min=0.0, int_rate_max=40.0, max_duplicate_ids=0)


def good_stats(**over):
    s = dict(rows=500, duplicate_ids_in_batch=0, label_rate=0.15, null_rates={"int_rate": 0.0, "loan_id": 0.0},
             int_rate_min=5.3, int_rate_max=28.0, int_rate_mean=13.0, ids_already_loaded=0)
    s.update(over)
    return s


def test_good_batch_passes():
    r = evaluate_stats(good_stats(), T)
    assert r.ok and r.reasons == []


def test_each_rule_trips_independently():
    assert not evaluate_stats(good_stats(rows=10), T).ok
    assert not evaluate_stats(good_stats(null_rates={"int_rate": 0.2}), T).ok
    assert not evaluate_stats(good_stats(label_rate=0.0), T).ok          # all-good labels: suspicious
    assert not evaluate_stats(good_stats(label_rate=0.9), T).ok          # flipped labels
    assert not evaluate_stats(good_stats(int_rate_max=250.0), T).ok      # shifted column
    assert not evaluate_stats(good_stats(duplicate_ids_in_batch=3), T).ok
    assert not evaluate_stats(good_stats(ids_already_loaded=500), T).ok  # same month loaded twice


def test_all_failures_are_reported():
    r = evaluate_stats(good_stats(rows=10, label_rate=0.9, ids_already_loaded=5), T)
    assert len(r.reasons) == 3


def test_compute_stats_detects_duplicates_and_existing_ids(spark):
    df = rows_df(
        spark,
        [(1, 10.0, 0, 5000.0, 50000.0), (2, 11.0, 1, 6000.0, 60000.0), (2, 12.0, 0, 7000.0, 70000.0)],
        "loan_id bigint, int_rate double, target int, loan_amnt double, annual_inc double")
    existing = rows_df(spark, [(1,), (99,)], "loan_id bigint")
    s = compute_stats(df, existing)
    assert s["rows"] == 3 and s["duplicate_ids_in_batch"] == 1
    assert s["ids_already_loaded"] == 1
    assert abs(s["label_rate"] - 1 / 3) < 1e-9
    assert s["int_rate_max"] == 12.0
