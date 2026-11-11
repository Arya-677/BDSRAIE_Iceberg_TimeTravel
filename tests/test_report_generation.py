"""Report plumbing only. The numbers below are test fixtures, NOT results; real reports read reports/*.json|csv|xml."""
from pathlib import Path

import pandas as pd
import pytest

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


def test_synthetic_demo_is_flagged_in_the_report_and_real_dataset_is_named():
    syn = build_markdown({**FAKE, "dataset": {"kind": "synthetic", "path": "/x/demo_synthetic.csv", "sha256": "ab" * 32}}, None, {}, None)
    assert "SYNTHETIC data" in syn
    real = build_markdown({**FAKE, "months": ["2013-01", "2014-02"], "dataset": {"kind": "file", "path": "/d/accepted.csv", "sha256": "cd" * 32,
                                                                              "rows_in_demo_months": 123456}}, None, {}, None)
    assert "SYNTHETIC data" not in real and "accepted.csv" in real and "123456" in real


def test_rollback_section_uses_the_actual_poisoned_batch_name():
    res = {**FAKE, "poisoned_batch": "2014-03", "audit_flagged": [{"batch": "2014-03", "why": "x"}, {"batch": "2013-07", "why": "y"}],
           "rollback": {"models_quarantined": ["v3"], "from": 2, "rolled_back_to": 1, "fingerprint_matches_logged_good_state": True}}
    md = build_markdown(res, None, {}, None)
    assert "2014-03: `int_rate`" in md and "| Post-hoc audit flagged the bad batch | PASS |" in md and "2013-07" in md


def _snap_frame():
    from test_snapshot_storage import WITH_UPDATE
    from ttml.snapshot_storage import derive
    return derive(WITH_UPDATE)


def test_report_has_a_snapshot_storage_section_with_metrics_and_cross_check():
    res = {**FAKE, "snapshot_storage": {"reconciliation": {"cumulative_from_summaries": 2400, "retained_from_all_files": 2400, "difference_bytes": 0}}}
    md = build_markdown(res, None, {}, None, snap_storage=_snap_frame())
    assert "### 6.1 Storage difference between snapshots" in md
    assert "| Snapshots | 3 |" in md and "History overhead (kept / current table) | 1.20x" in md and "overwrite" in md
    assert "Cross-check: the per-commit summaries add up to 2.34 KiB" in md


def test_report_says_not_run_when_snapshot_storage_is_missing():
    md = build_markdown(None, None, {}, None)
    assert "### 6.1 Storage difference between snapshots" in md and "reports/storage/snapshot_storage.csv` not found" in md


def test_report_only_embeds_storage_charts_that_exist(tmp_path):
    (tmp_path / "storage").mkdir()
    (tmp_path / "storage" / "snapshot_storage_growth.png").write_bytes(b"x")
    md = build_markdown(None, None, {}, None, reports_dir=str(tmp_path), snap_storage=_snap_frame())
    assert "storage/snapshot_storage_growth.png" in md and "snapshot_storage_changes.png" not in md


def test_pdf_renders_third_level_headings_instead_of_showing_hashes(tmp_path):
    import subprocess
    md = "# Title\n\n### Sub heading here\n\nbody text\n"
    out = tmp_path / "h.pdf"
    md_to_pdf(md, str(out))
    try:
        text = subprocess.run(["pdftotext", str(out), "-"], capture_output=True, text=True).stdout
    except FileNotFoundError:
        pytest.skip("pdftotext not installed")
    assert "Sub heading here" in text and "###" not in text
