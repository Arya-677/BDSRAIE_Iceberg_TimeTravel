"""The reproducibility suite T1-T6 plus rollback, retention and storage tests.
    TTML_ICEBERG=1   pytest -m iceberg -q --junitxml=reports/junit.xml    (real Iceberg: authoritative)
    TTML_ICEBERG=sim pytest -m iceberg -q                                  (simulator: logic only; storage tests skip)
Test names contain the keywords the audit report maps to controls (registry_binds, exact_retrain, time_travel,
rollback, retention, negative_control)."""
import json
from datetime import timedelta
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from ttml import registry
from ttml.faults import make_poisoned
from ttml.ingest import ingest_csv, ingest_month, load_clean_frame, sha256_file
from ttml.retention import RetentionGuardError, check_guard, promote, run_retention
from ttml.retrain import EvidenceLost, compare_models, retrain
from ttml.rollback import audit_batches, rollback_workflow
from ttml.snapshots import (current_snapshot_id, fingerprint, list_refs, list_snapshots, read_at_snapshot,
                            snapshot_exists)
from ttml.storage_metrics import measure
from ttml.tables import drop_all, init_tables
from ttml.timeutil import utc_naive
from ttml.train import train

pytestmark = pytest.mark.iceberg


@pytest.fixture()
def env(iceberg_spark, cfg, synthetic_csv8, tmp_path):
    s = iceberg_spark
    drop_all(s, cfg)
    init_tables(s, cfg)
    ids = ingest_csv(s, cfg, synthetic_csv8, "2015-01", "2015-06")                # 6 snapshots
    ov = {"train_end": "2015-04-30", "params": {"n_estimators": 30}, "artifacts_dir": str(tmp_path / "art")}
    return s, ids, ov, synthetic_csv8, str(tmp_path / "art")


def more(s, cfg, csv, months):
    df = load_clean_frame(s, cfg, csv).cache()
    return [ingest_month(s, cfg, df.filter(F.col("issue_month") == m), m, csv, sha256_file(csv)) for m in months]


# T1
def test_registry_binds_snapshot_to_every_model(env, cfg):
    s, ids, ov, csv, art = env
    head = current_snapshot_id(s, cfg.loans_table)
    train(s, cfg, "v1", overrides=ov, trained_by="t")
    row = registry.get_model(s, cfg, "v1")
    assert row["snapshot_id"] == head == ids[-1]
    tags = {r["name"]: r["snapshot_id"] for r in list_refs(s, cfg.loans_table) if r["type"] == "TAG"}
    assert tags["model-v1"] == head
    card = json.loads((Path(row["artifact_uri"]).parent / "model_card.json").read_text())
    assert card["snapshot_id"] == head and card["data_fingerprint"]["hash"] == row["data_fingerprint"]["hash"]
    assert row["artifact_sha256"] and row["feature_list_sha256"] == cfg.features_sha256()
    assert registry.current_status(s, cfg, "v1") == "candidate"


# T2
def test_exact_retrain_after_table_moves(env, cfg):
    s, ids, ov, csv, art = env
    train(s, cfg, "v1", overrides=ov)
    more(s, cfg, csv, ["2015-07", "2015-08"])                                      # head moves on
    assert current_snapshot_id(s, cfg.loans_table) != ids[-1]
    retrain(s, cfg, "v1", "v1-retrain", extra={"artifacts_dir": art})
    c = compare_models(s, cfg, "v1", "v1-retrain")
    assert c["L1_same_data"], c
    assert c["L2_identical_predictions"], c
    assert c["L3_identical_model_file"], c        # if this alone fails for a benign reason, normalise the file; never weaken L1/L2


# T3
def test_time_travel_returns_old_data(env, cfg):
    s, ids, ov, csv, art = env
    t = cfg.loans_table
    fp3, n3 = fingerprint(read_at_snapshot(s, t, ids[2])), read_at_snapshot(s, t, ids[2]).count()
    more(s, cfg, csv, ["2015-07"])
    assert fingerprint(read_at_snapshot(s, t, ids[2])) == fp3
    assert n3 < s.table(t).count()


# T4
def test_rollback_restores_state_and_quarantines_tainted_models(env, cfg):
    s, ids, ov, csv, art = env
    t = cfg.loans_table
    train(s, cfg, "v-good", overrides=ov)
    pre = fingerprint(s.table(t))
    df = load_clean_frame(s, cfg, csv).cache()
    bad_sid = ingest_month(s, cfg, make_poisoned(df.filter("issue_month = '2015-07'")), "2015-07", csv, "x", bypass_gate=True)
    more(s, cfg, csv, ["2015-08"])
    train(s, cfg, "v-bad", overrides=ov)
    flagged = audit_batches(s, cfg)
    assert flagged["batch"].tolist() == ["2015-07"]
    out = rollback_workflow(s, cfg, int(flagged.iloc[0]["snapshot_id"]), "2015-07", "test incident", actor="t")
    assert fingerprint(s.table(t)) == pre
    assert out["fingerprint_matches_logged_good_state"] is True
    assert out["models_quarantined"] == ["v-bad"]
    assert registry.current_status(s, cfg, "v-bad") == "quarantined"
    assert registry.current_status(s, cfg, "v-good") == "candidate"
    assert snapshot_exists(s, t, bad_sid)                                           # history is not erased
    assert s.table(cfg.rollback_log_table).count() == 1
    assert more(s, cfg, csv, ["2015-07"])[0] is not None                           # corrected batch can be re-ingested


# T5
def test_retention_preserves_evidence(env, cfg):
    s, ids, ov, csv, art = env
    t = cfg.loans_table
    train(s, cfg, "v1", overrides=ov)
    promote(s, cfg, "v1", "approved", "t", "ok")
    more(s, cfg, csv, ["2015-07", "2015-08"])
    out = run_retention(s, cfg, older_than=utc_naive() + timedelta(days=1), skip_orphans=True, retain_last=1)
    assert out["guard"] == "passed" and out["guard_after"] == "passed"
    assert snapshot_exists(s, t, ids[-1])                                           # tagged evidence survives
    assert not snapshot_exists(s, t, ids[1])                                        # untagged old snapshot expired
    retrain(s, cfg, "v1", "v1-after", extra={"artifacts_dir": art})
    assert compare_models(s, cfg, "v1", "v1-after")["L2_identical_predictions"]


def test_retention_guard_blocks_unsafe_cleanup(env, cfg):
    s, ids, ov, csv, art = env
    t = cfg.loans_table
    train(s, cfg, "v1", overrides=ov)
    s.sql(f"ALTER TABLE {t} DROP TAG `model-v1`")                                  # someone removes the protection
    assert check_guard(s, cfg)
    n = len(list_snapshots(s, t))
    with pytest.raises(RetentionGuardError):
        run_retention(s, cfg, older_than=utc_naive() + timedelta(days=1), skip_orphans=True, retain_last=1)
    assert len(list_snapshots(s, t)) == n                                           # nothing was expired


def test_evidence_lost_error_when_snapshot_expired(env, cfg):
    s, ids, ov, csv, art = env
    train(s, cfg, "v1", overrides=ov, tag=False)                                    # unprotected on purpose
    more(s, cfg, csv, ["2015-07"])
    s.sql(f"CALL lake.system.expire_snapshots(table => 'credit.loans', older_than => TIMESTAMP '"
          f"{(utc_naive() + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')}', retain_last => 1)")
    with pytest.raises(EvidenceLost):
        retrain(s, cfg, "v1", "v1-late", extra={"artifacts_dir": art})


# T6
def test_negative_control_unpinned_head_differs(env, cfg):
    """Shows the exact-retrain comparison CAN fail. Two situations:
    (a) later months appended: the table fingerprint differs, but a model trained only on EARLIER months can still
        be identical (its training window did not change). Fingerprint difference is what proves the table moved.
    (b) a late backfill inside the training window: now the unpinned model really differs from the pinned one."""
    s, ids, ov, csv, art = env
    train(s, cfg, "v1", overrides=ov)
    more(s, cfg, csv, ["2015-07", "2015-08"])
    train(s, cfg, "v-head-a", overrides=ov)                                         # unpinned -> today's head
    a = compare_models(s, cfg, "v1", "v-head-a")
    assert not a["L1_same_data"]
    assert registry.get_model(s, cfg, "v1")["data_fingerprint"] != registry.get_model(s, cfg, "v-head-a")["data_fingerprint"]

    df = load_clean_frame(s, cfg, csv).cache()
    late = df.filter("issue_month = '2015-02'").withColumn("loan_id", F.col("loan_id") + 10**9)
    ingest_month(s, cfg, late, "2015-02-late", csv, "x")                           # new loans arriving for an old month
    train(s, cfg, "v-head-b", overrides=ov)
    b = compare_models(s, cfg, "v1", "v-head-b")
    assert not b["L1_same_data"] and not b["L2_identical_predictions"] and not b["L3_identical_model_file"]
    assert b["artifact_sha_a"] != b["artifact_sha_b"]
    # ...while the pinned retrain of v1 is still exact:
    retrain(s, cfg, "v1", "v1-pinned", extra={"artifacts_dir": art})
    assert compare_models(s, cfg, "v1", "v1-pinned")["L3_identical_model_file"]


# storage
def test_storage_measure_append_only_has_no_history_overhead(real_iceberg_spark, env, cfg):
    s, ids, ov, csv, art = env
    m = measure(s, cfg.loans_table)
    assert m["snapshots"] == 6 and m["live_bytes"] > 0
    assert m["retained_bytes"] == m["live_bytes"] and m["overhead_ratio"] == 1.0


def test_storage_measure_copy_on_write_update_creates_overhead(real_iceberg_spark, env, cfg):
    s, ids, ov, csv, art = env
    t = cfg.loans_table
    s.sql(f"UPDATE {t} SET target = 1 - target WHERE issue_d < DATE '2015-02-01' AND pmod(hash(loan_id), 100) < 5")
    m = measure(s, t)
    assert m["overhead_ratio"] > 1.0 and m["retained_bytes"] > m["live_bytes"]


def test_storage_experiments_run_end_to_end_small(real_iceberg_spark, cfg, tmp_path):
    from ttml.storage_experiments import run_all
    res = run_all(real_iceberg_spark, cfg, str(tmp_path), months=4, rows_per_month=200, e1_checkpoints=[2, 4])
    assert {"E1", "E2", "E3", "E4", "E5"} <= set(res.exp)
    assert (res.overhead_ratio.dropna() >= 1.0).all()
    assert (tmp_path / "storage_results.csv").exists()
