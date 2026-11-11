from ttml.config import allowlist_sha256, load_allowlist
from ttml.data_gen import generate, months_between

LEAKAGE_AND_PROXIES = {"total_pymnt", "recoveries", "last_pymnt_d", "loan_status", "addr_state", "zip_code",
                       "out_prncp", "total_rec_prncp", "last_credit_pull_d"}


def test_allowlist_has_no_leakage_or_geographic_proxy(cfg):
    assert not (set(cfg.features()) & LEAKAGE_AND_PROXIES)


def test_allowlist_hash_ignores_comments_and_blank_lines(tmp_path):
    a = tmp_path / "a.txt"; b = tmp_path / "b.txt"
    a.write_text("x\ny\n")
    b.write_text("# comment\n\nx\n  y  \n# trailing\n")
    assert load_allowlist(a) == load_allowlist(b) == ["x", "y"]
    assert allowlist_sha256(a) == allowlist_sha256(b)


def test_allowlist_hash_changes_when_features_change(tmp_path):
    a = tmp_path / "a.txt"; b = tmp_path / "b.txt"
    a.write_text("x\ny\n"); b.write_text("x\nz\n")
    assert allowlist_sha256(a) != allowlist_sha256(b)


def test_months_between_inclusive():
    assert months_between("2015-11", "2016-02") == ["2015-11", "2015-12", "2016-01", "2016-02"]


def test_generate_is_deterministic_and_has_leakage_columns():
    a = generate("2015-01", "2015-02", 50, seed=1)
    b = generate("2015-01", "2015-02", 50, seed=1)
    assert a.equals(b)
    assert {"total_pymnt", "recoveries", "last_pymnt_d"} <= set(a.columns)   # present in raw, must be dropped later
    assert a["id"].is_unique
