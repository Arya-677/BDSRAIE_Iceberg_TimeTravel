"""End-to-end Iceberg tests. Run with:  TTML_ICEBERG=1 pytest -m iceberg -q   (or =sim for the logic simulator)"""
import pytest

from ttml import registry
from ttml.ingest import BatchRejected, ingest_csv, ingest_month, load_clean_frame, sha256_file
from ttml.snapshots import current_snapshot_id, fingerprint, list_snapshots, read_at_snapshot, snapshot_exists
from ttml.tables import drop_all, init_tables

pytestmark = pytest.mark.iceberg


@pytest.fixture()
def loaded(iceberg_spark, cfg, synthetic_csv):
    s = iceberg_spark
    drop_all(s, cfg)
    init_tables(s, cfg)
    ids = ingest_csv(s, cfg, synthetic_csv, "2015-01", "2015-04")
    return s, ids


def test_one_snapshot_per_month(loaded, cfg):
    s, ids = loaded
    assert len(ids) == 4 and len(set(ids)) == 4
    assert len(list_snapshots(s, cfg.loans_table)) == 4
    assert current_snapshot_id(s, cfg.loans_table) == ids[-1]


def test_ingest_log_matches_snapshots_and_fingerprints(loaded, cfg):
    s, ids = loaded
    log = {r["snapshot_id_after"]: r for r in s.table(cfg.ingest_log_table).collect()}
    assert set(log) == set(ids)
    for sid in ids:                                           # time travel returns exactly what was logged
        fp = fingerprint(read_at_snapshot(s, cfg.loans_table, sid))
        assert (fp["rows"], fp["hash"]) == (log[sid]["table_rows_after"], log[sid]["table_hash_after"])


def test_time_travel_row_counts_grow(loaded, cfg):
    s, ids = loaded
    counts = [read_at_snapshot(s, cfg.loans_table, i).count() for i in ids]
    assert counts == sorted(counts) and len(set(counts)) == 4


def test_rerun_is_idempotent(loaded, cfg, synthetic_csv):
    s, ids = loaded
    again = ingest_csv(s, cfg, synthetic_csv, "2015-01", "2015-04")
    assert again == [] and len(list_snapshots(s, cfg.loans_table)) == 4


def test_gate_rejects_bad_batch_and_bypass_is_logged(loaded, cfg, synthetic_csv):
    s, ids = loaded
    good = load_clean_frame(s, cfg, synthetic_csv)
    bad = good.filter("issue_month = '2015-02'").withColumn("loan_id", good.loan_id + 10**9) \
              .withColumn("int_rate", good.int_rate * 100)          # shifted units -> out of range
    before = current_snapshot_id(s, cfg.loans_table)
    with pytest.raises(BatchRejected):
        ingest_month(s, cfg, bad, "2015-02-bad", synthetic_csv, sha256_file(synthetic_csv))
    assert current_snapshot_id(s, cfg.loans_table) == before    # nothing written
    sid = ingest_month(s, cfg, bad, "2015-02-bad", synthetic_csv, sha256_file(synthetic_csv), bypass_gate=True)
    assert snapshot_exists(s, cfg.loans_table, sid)
    row = s.table(cfg.ingest_log_table).filter("batch_id = '2015-02-bad'").first()
    assert row["gate_bypassed"] is True


def test_registry_is_append_only_and_requires_lineage(loaded, cfg):
    s, ids = loaded
    fp = fingerprint(read_at_snapshot(s, cfg.loans_table, ids[-1]))
    full = dict(model_version="m1", table_name=cfg.loans_table, snapshot_id=ids[-1], data_fingerprint=fp,
                feature_list_sha256=cfg.features_sha256(), params_json={"seed": 1}, git_commit="abc123", trained_by="test")
    with pytest.raises(ValueError, match="missing required lineage"):
        registry.register_model(s, cfg, **{**full, "snapshot_id": None})
    registry.register_model(s, cfg, **full)
    with pytest.raises(ValueError, match="already exists"):
        registry.register_model(s, cfg, **full)
    r = registry.get_model(s, cfg, "m1")
    assert r["snapshot_id"] == ids[-1] and r["data_fingerprint"]["rows"] == fp["rows"]
    assert registry.current_status(s, cfg, "m1") == "candidate"
    registry.log_status(s, cfg, "m1", "quarantined", "test", "tester")
    assert registry.current_status(s, cfg, "m1") == "quarantined"
