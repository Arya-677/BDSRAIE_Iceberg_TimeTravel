"""The project must work with the LendingClub extract the team actually has.

The header below is copied from the sample the team pasted (72 columns; the FIRST header cell is blank and holds the loan id,
`member_id` is empty, issue_d looks like 'Jun-17', there are no fico columns, and the paste was tab-separated). The key values of
its 7 rows are reproduced; other cells are left blank like most of them are in the real file."""
import csv
import gzip
from datetime import date

import pytest

from ttml.ingest import load_clean_frame, read_source_csv, resolve_id_column, sniff_delimiter
from ttml.schema import parse_issue_d

HEADER = ["", "member_id", "loan_amnt", "funded_amnt", "funded_amnt_inv", "term", "int_rate", "installment", "grade", "sub_grade",
          "emp_title", "emp_length", "home_ownership", "annual_inc", "verification_status", "issue_d", "loan_status", "pymnt_plan",
          "desc", "purpose", "title", "zip_code", "addr_state", "dti", "delinq_2yrs", "earliest_cr_line", "inq_last_6mths",
          "mths_since_last_delinq", "mths_since_last_record", "open_acc", "pub_rec", "revol_bal", "revol_util", "total_acc",
          "initial_list_status", "out_prncp", "out_prncp_inv", "total_pymnt", "total_pymnt_inv", "total_rec_prncp", "total_rec_int",
          "total_rec_late_fee", "recoveries", "collection_recovery_fee", "last_pymnt_d", "last_pymnt_amnt", "next_pymnt_d",
          "last_credit_pull_d", "collections_12_mths_ex_med", "mths_since_last_major_derog", "policy_code", "application_type",
          "annual_inc_joint", "dti_joint", "verification_status_joint", "acc_now_delinq", "tot_coll_amt", "tot_cur_bal",
          "open_acc_6m", "open_il_12m", "open_il_24m", "mths_since_rcnt_il", "total_bal_il", "il_util", "open_rv_12m", "open_rv_24m",
          "max_bal_bc", "all_util", "total_rev_hi_lim", "inq_fi", "total_cu_tl", "inq_last_12m"]

# (id, loan_amnt, term, int_rate, installment, grade, emp_length, home, annual_inc, verification, purpose, dti, delinq, inq6m,
#  open_acc, pub_rec, revol_bal, revol_util, total_acc)  -- values taken from the pasted rows
ROWS = [
    ("112435993", "2300", "36 months", "12.62", "77.08", "C", "", "OWN", "10000", "Not Verified", "credit_card", "21.61", "0", "1", "4", "0", "3911", "55.1", "6"),
    ("112290210", "16000", "60 months", "12.62", "360.95", "C", "10+ years", "MORTGAGE", "94000", "Not Verified", "debt_consolidation", "25.61", "0", "0", "9", "0", "33752", "105.8", "26"),
    ("112436985", "6025", "36 months", "15.05", "209.01", "C", "7 years", "MORTGAGE", "46350", "Not Verified", "home_improvement", "8.88", "0", "0", "11", "0", "12425", "44.9", "27"),
    ("112439006", "20400", "36 months", "9.44", "652.91", "B", "10+ years", "RENT", "44000", "Source Verified", "car", "27.06", "0", "1", "15", "0", "8769", "18.7", "19"),
    ("112438929", "13000", "36 months", "11.99", "431.73", "B", "10+ years", "MORTGAGE", "85000", "Source Verified", "debt_consolidation", "6.79", "1", "0", "5", "0", "6866", "88", "24"),
    ("112230200", "12000", "36 months", "9.44", "384.06", "B", "10+ years", "MORTGAGE", "70000", "Not Verified", "debt_consolidation", "18.34", "0", "0", "14", "1", "15025", "58.2", "30"),
    ("112210041", "6000", "36 months", "10.42", "194.79", "B", "7 years", "MORTGAGE", "37107.19", "Source Verified", "medical", "30.63", "0", "0", "10", "1", "9332", "59.1", "28"),
]
NAMES = ["id", "loan_amnt", "term", "int_rate", "installment", "grade", "emp_length", "home_ownership", "annual_inc",
         "verification_status", "purpose", "dti", "delinq_2yrs", "inq_last_6mths", "open_acc", "pub_rec", "revol_bal", "revol_util", "total_acc"]


def write(path, statuses, sep="\t", issue="Jun-17", first_header="", opener=open):
    with opener(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=sep)
        w.writerow([first_header or ""] + HEADER[1:])
        for r, st in zip(ROWS, statuses):
            d = dict(zip(NAMES, r))
            d.update(issue_d=issue, loan_status=st, member_id="", funded_amnt=d["loan_amnt"], funded_amnt_inv=d["loan_amnt"],
                     emp_title="x", title="t", zip_code="148xx", addr_state="NY", earliest_cr_line="Sep-85", total_pymnt="382.99",
                     recoveries="0", last_pymnt_d="Dec-17", pymnt_plan="n", policy_code="1", application_type="Individual")
            w.writerow([d["id"]] + [d.get(c, "") for c in HEADER[1:]])
    return str(path)


ALL_CURRENT = ["Current"] * 7
MIXED = ["Fully Paid", "Charged Off", "Fully Paid", "Fully Paid", "Charged Off", "Late (31-120 days)", "Current"]


def test_header_matches_the_pasted_sample():
    assert len(HEADER) == 72 and HEADER[0] == "" and HEADER[1] == "member_id" and "fico_range_low" not in HEADER


def test_sample_loads_without_error_and_all_current_loans_have_no_outcome(spark, cfg, tmp_path):
    p = write(tmp_path / "s.tsv", ALL_CURRENT)
    raw = read_source_csv(spark, p, cfg.features())
    assert raw.count() == 7 and "id" in raw.columns                       # blank first header resolved to the id
    assert load_clean_frame(spark, cfg, p).count() == 0                   # 'Current' = no final outcome (documented in the data card)


def test_sample_values_are_parsed_correctly(spark, cfg, tmp_path):
    p = write(tmp_path / "s.tsv", MIXED)
    rows = {r["loan_id"]: r for r in load_clean_frame(spark, cfg, p).collect()}
    assert set(rows) == {112435993, 112290210, 112436985, 112439006, 112438929}       # late + current rows dropped
    a, b, c = rows[112435993], rows[112290210], rows[112436985]
    assert a["issue_d"] == date(2017, 6, 1) and a["issue_month"] == "2017-06"          # 'Jun-17' -> 2017-06
    assert (a["target"], b["target"], c["target"]) == (0, 1, 0)
    assert a["term"] == 36 and b["term"] == 60 and a["int_rate"] == 12.62 and a["installment"] == 77.08
    assert a["emp_length"] is None and b["emp_length"] == 10.0 and c["emp_length"] == 7.0       # blank / '10+ years' / '7 years'
    assert a["home_ownership"] == "OWN" and a["verification_status"] == "Not Verified" and a["purpose"] == "credit_card"
    assert c["annual_inc"] == 46350.0 and c["inq_last_6mths"] == 0.0 and c["revol_util"] == 44.9 and b["revol_util"] == 105.8


def test_only_allowlisted_origination_columns_reach_the_table(spark, cfg, tmp_path):
    cols = load_clean_frame(spark, cfg, write(tmp_path / "s.tsv", MIXED)).columns
    assert cols == ["loan_id", "issue_d", "issue_month", *cfg.features(), "target"]
    for leaked in ("total_pymnt", "recoveries", "last_pymnt_d", "out_prncp", "loan_status", "addr_state", "zip_code", "emp_title"):
        assert leaked not in cols


def _key(frame):
    return sorted(tuple(r) for r in frame.collect())


def test_tab_comma_and_gzip_versions_give_identical_data(spark, cfg, tmp_path):
    tsv = _key(load_clean_frame(spark, cfg, write(tmp_path / "a.tsv", MIXED)))
    csv_ = _key(load_clean_frame(spark, cfg, write(tmp_path / "a.csv", MIXED, sep=",")))
    gz = _key(load_clean_frame(spark, cfg, write(tmp_path / "a.csv.gz", MIXED, sep=",", opener=gzip.open)))
    assert tsv == csv_ == gz and len(tsv) == 5


def test_four_digit_years_and_an_id_header_also_work(spark, cfg, tmp_path):
    short = _key(load_clean_frame(spark, cfg, write(tmp_path / "a.tsv", MIXED, issue="Jun-17")))
    long_ = _key(load_clean_frame(spark, cfg, write(tmp_path / "b.csv", MIXED, sep=",", issue="Jun-2017", first_header="id")))
    assert short == long_


def test_missing_id_column_is_explained(spark, cfg, tmp_path):
    p = tmp_path / "noid.csv"
    p.write_text("member_id,loan_amnt,issue_d,loan_status\n,1000,Jun-17,Fully Paid\n", encoding="utf-8")
    with pytest.raises(ValueError, match="no loan-id column"):
        read_source_csv(spark, str(p), cfg.features())


def test_missing_feature_column_is_explained(spark, cfg, tmp_path):
    p = tmp_path / "nofeat.csv"
    p.write_text("id,loan_amnt,issue_d,loan_status\n1,1000,Jun-17,Fully Paid\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing expected columns"):
        read_source_csv(spark, str(p), cfg.features())


@pytest.mark.parametrize("raw,expected", [("Jun-17", date(2017, 6, 1)), ("Jun-2017", date(2017, 6, 1)), ("Dec-99", date(1999, 12, 1)),
                                           ("Jan-08", date(2008, 1, 1)), ("Jan-2008", date(2008, 1, 1)), ("garbage", None),
                                           ("Jun-1", None), ("", None), ("Jun-17-01", None), ("Xyz-17", None)])
def test_issue_date_formats(spark, raw, expected):
    from conftest import rows_df
    out = rows_df(spark, [(raw,)], "s string").select(parse_issue_d(__import__("pyspark.sql.functions", fromlist=["col"]).col("s")).alias("d")).first()[0]
    assert out == expected


def test_delimiter_and_id_helpers(tmp_path):
    for name, text, sep in (("a.tsv", "a\tb\tc\n1\t2\t3\n", "\t"), ("b.csv", "a,b,c\n", ","), ("c.csv", "a;b;c\n", ";"),
                            ("d.csv", "\ufeffid,b\n", ","), ("e.csv", "noseparator\n", ",")):
        (tmp_path / name).write_text(text, encoding="utf-8")
        assert sniff_delimiter(str(tmp_path / name)) == sep
    with gzip.open(tmp_path / "g.csv.gz", "wt", encoding="utf-8") as f:
        f.write("x\ty\n")
    assert sniff_delimiter(str(tmp_path / "g.csv.gz")) == "\t"
    assert resolve_id_column(["_c0", "member_id"]) == "_c0" and resolve_id_column(["id", "_c0"]) == "id" and resolve_id_column(["a"]) is None


def test_synthetic_default_style_mirrors_the_real_extract(spark, cfg, tmp_path):
    from ttml.data_gen import write_csv
    p = write_csv(str(tmp_path / "s.csv"), start="2015-01", end="2015-02", rows_per_month=60, seed=1)
    header = open(p, encoding="utf-8").readline().rstrip("\n").split(",")
    assert header[0] == "" and header[1] == "member_id" and "fico_range_low" not in header
    assert "Jan-15" in open(p, encoding="utf-8").read()
    df = load_clean_frame(spark, cfg, p)
    assert df.count() > 80 and sorted(r[0] for r in df.select("issue_month").distinct().collect()) == ["2015-01", "2015-02"]
