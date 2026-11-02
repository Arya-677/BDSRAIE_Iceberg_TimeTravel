"""Regression tests for defects found in the audit: path handling, identifier safety, CSV edge cases,
timezone handling, and SQL syntax of every statement we generate (parsed by Spark itself)."""
from datetime import timezone

import pandas as pd
import pytest

from ttml import registry
from ttml.config import load_config
from ttml.ingest import load_clean_frame, read_source_csv
from ttml.registry import INGEST_LOG_FIELDS, REGISTRY_FIELDS, ROLLBACK_FIELDS, EVENT_FIELDS, _ddl, validate_identifier
from ttml.rollback import flag_outlier_batches
from ttml.schema import loans_ddl
from ttml.timeutil import from_micros, utc_naive, utc_now


# ---- paths: nothing may depend on the current working directory ---------------------------------
def test_config_paths_resolve_against_project_root_not_cwd(cfg, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    c = load_config(cfg.root / "conf" / "pipeline.yaml")
    assert c.features() and c.features_sha256() == cfg.features_sha256()
    assert c.path("artifacts").is_absolute() and str(c.path("artifacts")).startswith(str(cfg.root))


def test_absolute_paths_are_left_alone(cfg, tmp_path):
    assert cfg.path(tmp_path) == tmp_path


# ---- identifier safety (names go into SQL and Iceberg tag names) ---------------------------------
@pytest.mark.parametrize("bad", ["", "a b", "x'; DROP TABLE t; --", "../etc", "v1\n", "-leading", "x" * 65, None])
def test_unsafe_identifiers_rejected(bad):
    with pytest.raises(ValueError, match="invalid"):
        validate_identifier(bad)


@pytest.mark.parametrize("ok", ["v1", "credit-lgbm-v3", "2015-02", "2015-02-bad", "v1.0_rc1"])
def test_safe_identifiers_accepted(ok):
    assert validate_identifier(ok) == ok


# ---- CSV edge cases ------------------------------------------------------------------------------
def _write(tmp_path, header, rows):
    p = tmp_path / "x.csv"
    p.write_text(header + "\n" + "\n".join(rows) + "\n")
    return str(p)


def test_missing_column_gives_friendly_error_not_spark_exception(spark, cfg, tmp_path):
    p = _write(tmp_path, "id,issue_d,loan_status", ["1,Jan-2016,Fully Paid"])
    with pytest.raises(ValueError, match="missing expected columns"):
        read_source_csv(spark, p, cfg.features())


def test_quoted_multiline_text_field_does_not_corrupt_rows(spark, cfg, tmp_path):
    feats = cfg.features()
    cols = ["id", "issue_d", "loan_status", "desc", *feats]

    def row(i, desc):
        vals = {c: "" for c in cols}
        vals.update(id=str(i), issue_d="Jan-2016", loan_status="Fully Paid", desc=desc, loan_amnt="1000", term=" 36 months",
                    int_rate="10.5", grade="A", emp_length="2 years", home_ownership="RENT", annual_inc="50000",
                    verification_status="Verified", purpose="car", dti="10", delinq_2yrs="0", fico_range_low="700",
                    fico_range_high="704", open_acc="5", pub_rec="0", revol_bal="100", revol_util="10", total_acc="9")
        return ",".join('"' + vals[c].replace('"', '""') + '"' for c in cols)

    p = _write(tmp_path, ",".join(cols), [row(1, "line one\nline two, with comma"), row(2, 'he said "hi"'), row(3, "plain")])
    out = load_clean_frame(spark, cfg, p).collect()
    assert sorted(r["loan_id"] for r in out) == [1, 2, 3]
    assert all(r["int_rate"] == 10.5 and r["grade"] == "A" for r in out)


# ---- outlier audit must ignore slow real-world drift ---------------------------------------------
def test_outlier_audit_ignores_slow_drift_but_catches_gross_errors():
    n = 36
    s = pd.DataFrame({"batch": [f"b{i:02d}" for i in range(n)], "rows": 1000,
                      "label_rate": [0.12 + 0.08 * i / (n - 1) for i in range(n)],       # 12% -> 20% over 3 years
                      "int_rate_mean": [12.0 + 1.5 * i / (n - 1) for i in range(n)],     # 12.0 -> 13.5
                      "int_rate_max": 28.0, "dup_ids": 0})
    assert flag_outlier_batches(s).empty
    s.loc[20, "int_rate_mean"] = 1300.0
    assert flag_outlier_batches(s)["batch"].tolist() == ["b20"]


# ---- time handling -------------------------------------------------------------------------------
def test_timestamps_sent_to_spark_are_timezone_aware_utc():
    assert registry._now().tzinfo is not None and utc_now().utcoffset().total_seconds() == 0
    assert utc_naive().tzinfo is None


def test_from_micros_is_exact_and_aware():
    d = from_micros(1_700_000_000_123_456)
    assert d.tzinfo == timezone.utc and d.microsecond == 123_456 and d.year == 2023




# ---- every generated CREATE/UPDATE statement must at least PARSE in Spark ------------------------
def _parse(spark, sql):
    spark._jsparkSession.sessionState().sqlParser().parsePlan(sql)     # raises ParseException on bad syntax


def test_all_generated_ddl_parses(spark, cfg):
    _parse(spark, loans_ddl(cfg))
    _parse(spark, loans_ddl(cfg, table="lake.exp.e2_x", write_mode="merge-on-read"))
    for t, f in [(cfg.registry_table, REGISTRY_FIELDS), (cfg.status_events_table, EVENT_FIELDS),
                 (cfg.ingest_log_table, INGEST_LOG_FIELDS), (cfg.rollback_log_table, ROLLBACK_FIELDS)]:
        _parse(spark, _ddl(t, f))


def test_update_statement_used_by_experiments_parses(spark):
    _parse(spark, "UPDATE lake.exp.t SET target = 1 - target WHERE issue_d >= DATE '2015-01-01' "
                  "AND issue_d < DATE '2015-02-01' AND pmod(hash(loan_id, 3), 10000) < 100")


def test_loans_ddl_write_mode_override(cfg):
    assert "'write.update.mode' = 'merge-on-read'" in loans_ddl(cfg, write_mode="merge-on-read")
    assert "lake.exp.zz" in loans_ddl(cfg, table="lake.exp.zz")


def test_registry_schema_lists_are_consistent():
    names = [n for n, _ in REGISTRY_FIELDS]
    assert len(names) == len(set(names)) and "snapshot_id" in names
    assert set(registry.REQUIRED_ON_REGISTER) <= set(names)
