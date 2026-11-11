"""Runs the whole evidence story (src/ttml/demo.py) and asserts every headline claim the audit report makes.
    TTML_ICEBERG=sim pytest tests/test_demo_iceberg.py     (logic only)     TTML_ICEBERG=1 ... (authoritative)"""
import json
from pathlib import Path

import pytest

from ttml.demo import run_demo

pytestmark = pytest.mark.iceberg


def test_demo_end_to_end_makes_every_claim_the_report_relies_on(iceberg_spark, cfg, tmp_path):
    R = run_demo(iceberg_spark, cfg, out_dir=str(tmp_path), rows_per_month=300)

    e = R["exact_retrain"]
    assert e["L1_same_data"] and e["L2_identical_predictions"] and e["L3_identical_model_file"], e
    assert R["v1_vs_v2_differ"] and R["negative_control"]["differs"]

    assert R["poisoned_batch"] == "2016-01" and R["dataset"]["kind"] == "synthetic"
    assert "2016-01" in [x["batch"] for x in R["audit_flagged"]]
    rb = R["rollback"]
    assert rb["fingerprint_matches_logged_good_state"] and rb["head_equals_pre_incident_fingerprint"]
    assert rb["bad_snapshot_still_listed"] and rb["models_quarantined"] == ["v3"]
    assert R["status_v3"] == "quarantined"

    r = R["retention"]
    assert r["guard"] == "passed" and r["v1_snapshot_survived"] and r["untagged_old_snapshot_expired"]
    assert r["guard_problems_now"] == [] and r["snapshots_after"] < r["snapshots_before"]

    e2 = R["exact_retrain_after_retention"]
    assert e2["L1_same_data"] and e2["L2_identical_predictions"] and e2["L3_identical_model_file"], e2

    ss = R["snapshot_storage"]
    assert ss["snapshots"] >= 14 and all(Path(f).exists() for f in ss["files"]), ss        # per-snapshot storage captured before retention
    saved = json.loads((tmp_path / "demo_results.json").read_text())
    assert saved["exact_retrain"]["L1_same_data"] is True


def test_demo_runs_on_a_provided_csv_with_another_start_month(iceberg_spark, cfg, tmp_path):
    from ttml.data_gen import write_csv
    p = write_csv(str(tmp_path / "mine.csv"), start="2013-03", end="2014-04", rows_per_month=300, seed=3)
    R = run_demo(iceberg_spark, cfg, out_dir=str(tmp_path), csv=p, start="2013-03")
    assert R["dataset"]["kind"] == "file" and R["months"][0] == "2013-03" and R["poisoned_batch"] == "2014-03"
    e = R["exact_retrain"]
    assert e["L1_same_data"] and e["L2_identical_predictions"] and e["L3_identical_model_file"], e
    assert R["rollback"]["fingerprint_matches_logged_good_state"] and R["rollback"]["models_quarantined"] == ["v3"]


def test_demo_explains_when_the_csv_lacks_the_needed_months(iceberg_spark, cfg, tmp_path):
    from ttml.data_gen import write_csv
    p = write_csv(str(tmp_path / "short.csv"), start="2015-01", end="2015-05", rows_per_month=300, seed=3)
    with pytest.raises(ValueError, match="14 consecutive months"):
        run_demo(iceberg_spark, cfg, out_dir=str(tmp_path), csv=p, start="2015-01")
