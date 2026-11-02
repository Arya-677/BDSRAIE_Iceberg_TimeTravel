"""Report plumbing only. The numbers below are test fixtures, NOT results; real reports read reports/*.json|csv|xml."""
from pathlib import Path

import pandas as pd

from ttml.audit_report import build_markdown, load_junit, md_to_pdf

FAKE = {"v1": {"snapshot_id": 1}, "head_after_more_months": 2,
        "exact_retrain": {"L1_same_data": True, "L2_identical_predictions": True, "L3_identical_model_file": False, "n_probe": 10,
                          "max_abs_pred_diff": 0.0, "fingerprint_a": {"rows": 1}, "fingerprint_b": {"rows": 1}},
        "negative_control": {"differs": True}}


def test_report_without_evidence_says_not_run_and_invents_nothing():
    md = build_markdown(None, None, {}, None)
    assert md.count("NOT RUN") >= 4 and "PASS" not in md.replace("NOT RUN", "")


def test_report_reflects_fail_results_honestly():
    md = build_markdown(FAKE, None, {}, "card")
    assert "| L3 | identical model file hash | FAIL |" in md and "| L1 |" in md
    assert "TEAM TO COMPLETE" in md


def test_junit_mapping_and_skip_handling(tmp_path):
    x = tmp_path / "j.xml"
    x.write_text('<testsuites><testsuite><properties><property name="ttml_iceberg_mode" value="real"/></properties>'
                 '<testcase classname="t" name="test_exact_retrain_x"/>'
                 '<testcase classname="t" name="test_rollback_y"><skipped/></testcase>'
                 '<testcase classname="t" name="test_negative_control_z"><failure/></testcase></testsuite></testsuites>')
    md = build_markdown(None, None, load_junit(x), None)
    assert "| T2 | Exact retrain from the registry after the table has changed | PASS |" in md
    assert "SKIPPED" in md and "| FAIL |" in md


def test_storage_table_rendering_and_pdf(tmp_path):
    st = pd.DataFrame([{"exp": "E1", "mode": "copy-on-write", "update_rate": 0.0, "snapshots": 12, "live_bytes": 2048,
                        "retained_bytes": 2048, "metadata_bytes": 4096, "overhead_ratio": 1.0}])
    md = build_markdown(FAKE, st, {}, None)
    assert "2.0 KiB" in md and "1.000" in md
    out = tmp_path / "r.pdf"
    md_to_pdf(md, str(out))
    assert out.exists() and out.stat().st_size > 2000


def _junit(tmp_path, mode):
    x = tmp_path / "j.xml"
    prop = f'<properties><property name="ttml_iceberg_mode" value="{mode}"/></properties>' if mode else ""
    x.write_text(f'<testsuites><testsuite>{prop}<testcase classname="t" name="test_exact_retrain_x"/></testsuite></testsuites>')
    return load_junit(x)


def test_simulator_results_are_never_presented_as_evidence(tmp_path):
    md = build_markdown(None, None, _junit(tmp_path, "sim"), None)
    assert "SIMULATOR ONLY" in md and "WARNING" in md
    assert "| T2 | Exact retrain from the registry after the table has changed | PASS |" not in md


def test_real_iceberg_results_are_plain_pass_without_warning(tmp_path):
    md = build_markdown(None, None, _junit(tmp_path, "real"), None)
    assert "| T2 | Exact retrain from the registry after the table has changed | PASS |" in md and "WARNING" not in md


def test_unrecorded_engine_is_flagged(tmp_path):
    md = build_markdown(None, None, _junit(tmp_path, None), None)
    assert "engine not recorded" in md


def test_demo_run_on_simulator_triggers_warning():
    md = build_markdown({**FAKE, "engine": "SIMULATOR"}, None, {}, None)
    assert "WARNING" in md
