"""Runs the whole evidence story (src/ttml/demo.py) and asserts every headline claim the audit report makes.
    TTML_ICEBERG=sim pytest tests/test_demo_iceberg.py     (logic only)     TTML_ICEBERG=1 ... (authoritative)"""
import json

import pytest

from ttml.demo import run_demo

pytestmark = pytest.mark.iceberg


def test_demo_end_to_end_makes_every_claim_the_report_relies_on(iceberg_spark, cfg, tmp_path):
    R = run_demo(iceberg_spark, cfg, out_dir=str(tmp_path), rows_per_month=300)

    e = R["exact_retrain"]
    assert e["L1_same_data"] and e["L2_identical_predictions"] and e["L3_identical_model_file"], e
    assert R["v1_vs_v2_differ"] and R["negative_control"]["differs"]

    assert [x["batch"] for x in R["audit_flagged"]] == ["2016-01"]
    rb = R["rollback"]
    assert rb["fingerprint_matches_logged_good_state"] and rb["head_equals_pre_incident_fingerprint"]
    assert rb["bad_snapshot_still_listed"] and rb["models_quarantined"] == ["v3"]
    assert R["status_v3"] == "quarantined"

    r = R["retention"]
    assert r["guard"] == "passed" and r["v1_snapshot_survived"] and r["untagged_old_snapshot_expired"]
    assert r["guard_problems_now"] == [] and r["snapshots_after"] < r["snapshots_before"]

    e2 = R["exact_retrain_after_retention"]
    assert e2["L1_same_data"] and e2["L2_identical_predictions"] and e2["L3_identical_model_file"], e2

    saved = json.loads((tmp_path / "demo_results.json").read_text())
    assert saved["exact_retrain"]["L1_same_data"] is True
