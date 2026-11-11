"""End-to-end evidence run. Writes reports/demo_results.json, which the audit report is generated from.

Story: ingest -> train v1 (pinned) -> ingest more -> train v2 -> exact retrain of v1 (L1/L2/L3) -> negative control ->
bad month slips through -> v3 trained on tainted data -> audit detects -> quarantine + rollback + proof ->
re-ingest corrected data -> clean model v4 -> approve v1 (tier-1 tag) -> retention (v1 evidence survives) -> retrain v1 again.
"""
from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pandas as pd

from pyspark.sql import functions as F

from . import registry, snapshot_storage
from .data_gen import write_csv
from .faults import make_poisoned
from .ingest import ingest_month, load_clean_frame, sha256_file
from .retention import check_guard, promote, run_retention
from .retrain import compare_models, retrain
from .rollback import audit_batches, rollback_workflow
from .snapshots import (current_snapshot_id, fingerprint, list_snapshots, read_at_snapshot, snapshot_exists)
from .storage_metrics import measure
from .tables import drop_all, init_tables
from .timeutil import utc_naive
from .train import train

SMALL = {"params": {"n_estimators": 60}}      # fast demo model; real runs use conf/pipeline.yaml


def _end_of(month: str) -> str:
    return pd.Period(month, freq="M").end_time.strftime("%Y-%m-%d")


def run_demo(spark, cfg, out_dir: str = "reports", rows_per_month: int = 800, csv: str | None = None,
             start: str = "2015-01") -> dict:
    """The evidence story on 14 consecutive months starting at `start`.
    csv=None: synthetic data is generated. csv=<path>: YOUR file (e.g. LendingClub) is used; all 14 months must exist in it."""
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    months = [str(pd.Period(start, freq="M") + i) for i in range(14)]
    poisoned = months[12]
    R: dict = {"started": utc_naive().isoformat(timespec="seconds") + "Z",
               "engine": "SIMULATOR" if getattr(spark, "is_sim", False) else "iceberg",
               "months": months, "poisoned_batch": poisoned}
    T = cfg.loans_table
    drop_all(spark, cfg)
    init_tables(spark, cfg)

    if csv is None:
        csv = write_csv(str(out / "demo_synthetic.csv"), start=months[0], end=months[-1], rows_per_month=rows_per_month, seed=11)
        R["dataset"] = {"kind": "synthetic", "path": csv}
    else:
        R["dataset"] = {"kind": "file", "path": str(csv)}
    sha = sha256_file(csv)
    R["dataset"]["sha256"] = sha
    df = load_clean_frame(spark, cfg, csv).cache()
    present = {r[0] for r in df.select("issue_month").distinct().collect()}
    missing = [m for m in months if m not in present]
    if missing:
        raise ValueError(f"the demo needs 14 consecutive months starting {start} but {csv} has no usable rows for {missing}. "
                         f"Pick another --start (months present: {sorted(present)[:6]} ...).")
    R["dataset"]["rows_in_demo_months"] = df.filter(F.col("issue_month").isin(months)).count()
    t_first, t_second = _end_of(months[5]), _end_of(months[11])       # train windows: first 6 months / first 12 months
    ov = lambda **k: {**SMALL, "train_end": t_first, **k}  # noqa: E731
    month = lambda m: df.filter(F.col("issue_month") == m)      # noqa: E731
    sid_of = {}

    # 1. months 1-9, train v1
    for m in months[:9]:
        sid_of[m] = ingest_month(spark, cfg, month(m), m, csv, sha)
    R["ingest_first_9_months"] = {"snapshots": len(list_snapshots(spark, T)), "head": current_snapshot_id(spark, T)}
    R["v1"] = train(spark, cfg, "v1", overrides=ov(), trained_by="demo")

    # 2. months 10-12, train v2 on the moved head
    for m in months[9:12]:
        sid_of[m] = ingest_month(spark, cfg, month(m), m, csv, sha)
    R["v2"] = train(spark, cfg, "v2", overrides=ov(), trained_by="demo")
    R["v1_vs_v2_differ"] = R["v1"]["fingerprint"] != R["v2"]["fingerprint"]

    # 3. exact retrain of v1 although the table has moved on
    R["head_after_more_months"] = current_snapshot_id(spark, T)
    retrain(spark, cfg, "v1", "v1-retrain", trained_by="demo")
    R["exact_retrain"] = compare_models(spark, cfg, "v1", "v1-retrain")

    # 4. negative control: same params on today's head, no pin -> must differ
    R["negative_control"] = {"fingerprint_head": fingerprint(read_at_snapshot(spark, T, current_snapshot_id(spark, T))),
                             "fingerprint_v1": R["v1"]["fingerprint"]}
    R["negative_control"]["differs"] = R["negative_control"]["fingerprint_head"] != R["negative_control"]["fingerprint_v1"]

    # 5. a bad month slips through the gate (bypass simulates the failure), then a clean month
    good_head = current_snapshot_id(spark, T)
    good_fp = fingerprint(read_at_snapshot(spark, T, good_head))
    bad_sid = ingest_month(spark, cfg, make_poisoned(month(poisoned)), poisoned, csv, sha, bypass_gate=True)
    ingest_month(spark, cfg, month(months[13]), months[13], csv, sha)
    R["v3_tainted"] = train(spark, cfg, "v3", overrides=ov(train_end=t_second), trained_by="demo")

    # 6. detect, contain, roll back, prove
    flagged = audit_batches(spark, cfg)
    R["audit_flagged"] = flagged.to_dict("records")
    hit = flagged[flagged["batch"] == poisoned]
    if hit.empty:
        raise RuntimeError(f"the post-hoc audit did not flag the poisoned batch {poisoned}; it flagged: {flagged['batch'].tolist()}")
    R["audit_other_flags"] = [x for x in flagged["batch"].tolist() if x != poisoned]    # possible false positives on real data
    R["rollback"] = rollback_workflow(spark, cfg, int(hit.iloc[0]["snapshot_id"]), poisoned,
                                      "int_rate x100 and flipped labels", actor="demo")
    R["rollback"]["head_equals_pre_incident_fingerprint"] = fingerprint(spark.table(T)) == good_fp
    R["rollback"]["bad_snapshot_still_listed"] = snapshot_exists(spark, T, bad_sid)
    R["status_v3"] = registry.current_status(spark, cfg, "v3")

    # 7. repair: corrected months, clean model
    for m in months[12:]:
        ingest_month(spark, cfg, month(m), m, csv, sha)
    R["v4_clean"] = train(spark, cfg, "v4", overrides=ov(train_end=t_second), trained_by="demo")

    # 7b. storage difference between snapshots, captured BEFORE retention expires any of them
    try:
        ssum, sdf = snapshot_storage.write_report(spark, T, str(out / "storage"), prefix="snapshot_storage", title=T)
        try:                                      # cross-check Iceberg's per-commit summaries against its file listing
            ret = measure(spark, T)["retained_bytes"]
            ssum["reconciliation"] = {"cumulative_from_summaries": ssum["cumulative_retained_bytes"], "retained_from_all_files": ret,
                                      "difference_bytes": ssum["cumulative_retained_bytes"] - ret}
        except Exception as e:
            ssum["reconciliation"] = {"unavailable": f"{type(e).__name__}"}
        R["snapshot_storage"] = ssum
    except Exception as e:
        R["snapshot_storage"] = {"unavailable": f"{type(e).__name__}: {str(e)[:200]}"}

    # 8. approve v1 -> tier-1 retention; run retention; evidence must survive
    promote(spark, cfg, "v1", "approved", "demo-approver", "passed validation")
    before = list_snapshots(spark, T)
    far = utc_naive() + timedelta(days=1)
    R["retention"] = run_retention(spark, cfg, older_than=far, skip_orphans=True, retain_last=1)
    after = list_snapshots(spark, T)
    v1_sid = R["v1"]["snapshot_id"]
    R["retention"].update({"snapshots_before": len(before), "snapshots_after": len(after),
                           "v1_snapshot_survived": snapshot_exists(spark, T, v1_sid),
                           "untagged_old_snapshot_expired": not snapshot_exists(spark, T, sid_of[months[1]]),
                           "guard_problems_now": check_guard(spark, cfg)})

    # 9. retrain v1 AFTER retention: still exact
    retrain(spark, cfg, "v1", "v1-retrain-after-retention", trained_by="demo")
    R["exact_retrain_after_retention"] = compare_models(spark, cfg, "v1", "v1-retrain-after-retention")

    try:
        R["storage_snapshot"] = measure(spark, T)
    except Exception as e:                       # e.g. non-local filesystem or metadata tables unavailable
        R["storage_snapshot"] = {"unavailable": f"{type(e).__name__}: {str(e)[:200]}"}
    R["registry_rows"] = [{k: (str(v) if not isinstance(v, (int, float, bool, type(None))) else v) for k, v in r.asDict(recursive=True).items()
                           if k in ("model_version", "snapshot_id", "git_commit", "artifact_sha256", "data_fingerprint", "seed")}
                          for r in spark.table(cfg.registry_table).orderBy("created_at").collect()]
    R["status_events"] = [r.asDict() for r in spark.table(cfg.status_events_table).orderBy("event_time").collect()]
    R["finished"] = utc_naive().isoformat(timespec="seconds") + "Z"
    df.unpersist()
    (out / "demo_results.json").write_text(json.dumps(R, indent=2, default=str), encoding="utf-8")
    return R
