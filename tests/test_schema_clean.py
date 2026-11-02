import pytest

from conftest import rows_df
from ttml.ingest import load_clean_frame
from ttml.schema import clean, loans_ddl


def test_clean_output_shape_and_types(spark, cfg, synthetic_csv):
    df = load_clean_frame(spark, cfg, synthetic_csv)
    feats = cfg.features()
    assert df.columns == ["loan_id", "issue_d", "issue_month", *feats, "target"]
    t = dict(df.dtypes)
    assert t["loan_id"] == "bigint" and t["term"] == "int" and t["int_rate"] == "double"
    assert t["issue_d"] == "date" and t["target"] == "int"


def test_clean_drops_open_loans_footer_and_keeps_binary_target(spark, cfg, synthetic_csv):
    df = load_clean_frame(spark, cfg, synthetic_csv)
    assert {r[0] for r in df.select("target").distinct().collect()} <= {0, 1}
    assert df.count() < 4 * 400                         # ~8% 'Current' dropped (+ footer row ignored)
    assert df.count() > 4 * 400 * 0.85
    months = sorted(r[0] for r in df.select("issue_month").distinct().collect())
    assert months == ["2015-01", "2015-02", "2015-03", "2015-04"]


def test_clean_parses_messy_values(spark, cfg):
    feats = cfg.features()
    base = {c: None for c in feats}
    rows = [
        {**base, "id": "1", "issue_d": "Dec-2015", "loan_status": "Fully Paid", "term": " 36 months",
         "emp_length": "10+ years", "int_rate": "13.56%", "grade": " B "},
        {**base, "id": "2", "issue_d": "Jan-2016", "loan_status": "Charged Off", "term": " 60 months",
         "emp_length": "< 1 year", "int_rate": "9.5"},
        {**base, "id": "3", "issue_d": "Jan-2016", "loan_status": "Charged Off", "term": " 60 months",
         "emp_length": "n/a"},
        {**base, "id": "x9", "issue_d": "Jan-2016", "loan_status": "Fully Paid"},        # non-numeric id -> dropped
        {**base, "id": "5", "issue_d": "Jan-2016", "loan_status": "Current"},           # no outcome -> dropped
        {**base, "id": "6", "issue_d": "garbage", "loan_status": "Fully Paid"},         # bad date -> dropped
    ]
    raw = rows_df(spark, rows, ", ".join(f"{c} string" for c in sorted({*feats, "id", "issue_d", "loan_status"})))
    out = {r["loan_id"]: r for r in clean(raw, feats).collect()}
    assert set(out) == {1, 2, 3}
    assert out[1]["term"] == 36 and out[1]["emp_length"] == 10.0 and abs(out[1]["int_rate"] - 13.56) < 1e-9
    assert out[1]["grade"] == "B" and out[1]["target"] == 0 and out[1]["issue_month"] == "2015-12"
    assert out[2]["emp_length"] == 0.0 and out[2]["target"] == 1
    assert out[3]["emp_length"] is None


def test_clean_fails_loudly_on_missing_source_column(spark, cfg):
    raw = rows_df(spark, [("1", "Jan-2016", "Fully Paid")], "id string, issue_d string, loan_status string")
    with pytest.raises(ValueError, match="missing expected columns"):
        clean(raw, cfg.features())


def test_loans_ddl_contains_partition_and_properties(cfg):
    ddl = loans_ddl(cfg)
    assert "PARTITIONED BY (months(issue_d))" in ddl
    assert "'format-version' = '2'" in ddl
    assert "ingest_batch STRING" in ddl and "target INT" in ddl
    for f in cfg.features():
        assert f in ddl
