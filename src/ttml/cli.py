"""Command line entry point:  python -m ttml.cli <command>"""
from __future__ import annotations

import argparse

from .config import load_config


def main(argv=None):
    p = argparse.ArgumentParser(prog="ttml")
    p.add_argument("--config", default="conf/pipeline.yaml")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("gen-data", help="write a synthetic LendingClub-shaped CSV")
    g.add_argument("--out", default="data/raw/synthetic_lc.csv")
    g.add_argument("--start", default="2015-01")
    g.add_argument("--end", default="2015-12")
    g.add_argument("--rows-per-month", type=int, default=500)
    g.add_argument("--seed", type=int, default=42)

    sub.add_parser("init", help="create namespaces and tables")

    i = sub.add_parser("ingest", help="load a CSV month by month (one snapshot per month)")
    i.add_argument("--csv")
    i.add_argument("--start")
    i.add_argument("--end")
    i.add_argument("--max-months", type=int)
    i.add_argument("--skip-rejected", action="store_true", help="skip months the ingest gate rejects instead of stopping")

    t = sub.add_parser("train", help="train a model on a pinned snapshot and register it")
    t.add_argument("version")
    t.add_argument("--snapshot-id", type=int, help="default: current head, pinned once")
    t.add_argument("--train-end", help="override training.train_end (YYYY-MM-DD)")
    r = sub.add_parser("retrain", help="rebuild a model from its registry row only")
    r.add_argument("version"); r.add_argument("new_version")
    cm = sub.add_parser("compare", help="L1/L2/L3 comparison of two registered models")
    cm.add_argument("a"); cm.add_argument("b")
    pr = sub.add_parser("promote", help="set a model status (approved/deployed extend snapshot retention to tier 1)")
    pr.add_argument("version"); pr.add_argument("status"); pr.add_argument("--reason", default="manual"); pr.add_argument("--actor", default="cli")
    sub.add_parser("audit", help="post-hoc data-quality audit of loaded batches")
    rb = sub.add_parser("rollback", help="contain + roll back to just before a bad batch")
    rb.add_argument("--bad-batch", required=True); rb.add_argument("--reason", default="data quality incident")
    rt = sub.add_parser("retention", help="run tiered retention (guard first)")
    rt.add_argument("--dry-run", action="store_true"); rt.add_argument("--retain-last", type=int)
    sub.add_parser("storage", help="storage overhead of the loans table right now")
    ex = sub.add_parser("experiments", help="run storage experiments E1-E5")
    ex.add_argument("--quick", action="store_true"); ex.add_argument("--months", type=int, default=24)
    ex.add_argument("--rows-per-month", type=int, default=2000); ex.add_argument("--out", default="reports/storage")
    dm = sub.add_parser("demo", help="end-to-end evidence run -> reports/demo_results.json")
    dm.add_argument("--rows-per-month", type=int, default=800, help="synthetic data only")
    dm.add_argument("--csv", help="use YOUR file (e.g. LendingClub accepted loans) instead of synthetic data")
    dm.add_argument("--start", default=None, help="first of 14 consecutive months to use (default 2015-01 synthetic / data.start_month for --csv)")
    ss = sub.add_parser("snapshot-storage", help="storage of every snapshot: CSV + graphs (growth, per-commit changes)")
    ss.add_argument("--table", help="default: the loans table"); ss.add_argument("--out", default="reports/storage")
    sd = sub.add_parser("snapshot-diff", help="storage difference between two snapshot ids")
    sd.add_argument("a", type=int); sd.add_argument("b", type=int); sd.add_argument("--table", help="default: the loans table")
    sub.add_parser("report", help="build reports/audit_readiness_report.md/.pdf from evidence files (no Spark needed)")

    sub.add_parser("snapshots", help="list snapshots of the loans table")
    sub.add_parser("fingerprint", help="print the content fingerprint of the current table state")

    a = p.parse_args(argv)
    cfg = load_config(a.config)

    if a.cmd == "gen-data":
        from .data_gen import write_csv
        out = cfg.path(a.out); out.parent.mkdir(parents=True, exist_ok=True)
        print("wrote", write_csv(str(out), start=a.start, end=a.end, rows_per_month=a.rows_per_month, seed=a.seed))
        return

    if a.cmd == "report":
        from .audit_report import generate
        print("wrote", *generate(str(cfg.path("reports"))))
        return

    from .spark_session import get_spark
    spark = get_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    if a.cmd == "init":
        from .tables import init_tables
        init_tables(spark, cfg)
        print("tables ready")
    elif a.cmd == "ingest":
        from .ingest import ingest_csv
        ids = ingest_csv(spark, cfg, str(cfg.path(a.csv or cfg.raw["data"]["source_csv"])), a.start, a.end, a.max_months, skip_rejected=a.skip_rejected)
        print(f"{len(ids)} snapshots created")
    elif a.cmd == "train":
        from .train import train
        ov = {"train_end": a.train_end} if a.train_end else None
        print(train(spark, cfg, a.version, snapshot_id=a.snapshot_id, overrides=ov))
    elif a.cmd == "retrain":
        from .retrain import retrain
        print(retrain(spark, cfg, a.version, a.new_version))
    elif a.cmd == "compare":
        from .retrain import compare_models
        import json; print(json.dumps(compare_models(spark, cfg, a.a, a.b), indent=2, default=str))
    elif a.cmd == "promote":
        from .retention import promote
        promote(spark, cfg, a.version, a.status, a.actor, a.reason); print("ok")
    elif a.cmd == "audit":
        from .rollback import audit_batches
        f = audit_batches(spark, cfg); print(f.to_string(index=False) if len(f) else "no batches flagged")
    elif a.cmd == "rollback":
        from .rollback import rollback_workflow, snapshot_for_batch
        print(rollback_workflow(spark, cfg, snapshot_for_batch(spark, cfg, a.bad_batch), a.bad_batch, a.reason))
    elif a.cmd == "retention":
        from .retention import run_retention
        print(run_retention(spark, cfg, dry_run=a.dry_run, retain_last=a.retain_last))
    elif a.cmd == "storage":
        from .storage_metrics import measure
        import json; print(json.dumps(measure(spark, cfg.loans_table), indent=2))
    elif a.cmd == "experiments":
        from .storage_experiments import run_all
        print(run_all(spark, cfg, str(cfg.path(a.out)), months=a.months, rows_per_month=a.rows_per_month, quick=a.quick).to_string())
    elif a.cmd == "demo":
        from .demo import run_demo
        R = run_demo(spark, cfg, out_dir=str(cfg.path("reports")), rows_per_month=a.rows_per_month,
                     csv=str(cfg.path(a.csv)) if a.csv else None,
                     start=a.start or (cfg.raw["data"]["start_month"] if a.csv else "2015-01"))
        e = R["exact_retrain"]
        print("exact retrain  L1:", e["L1_same_data"], " L2:", e["L2_identical_predictions"], " L3:", e["L3_identical_model_file"])
        print("rollback verified:", R["rollback"]["fingerprint_matches_logged_good_state"], " models quarantined:", R["rollback"]["models_quarantined"])
        print("wrote reports/demo_results.json")
    elif a.cmd == "snapshot-storage":
        from .snapshot_storage import fmt_bytes, write_report
        s, _ = write_report(spark, a.table or cfg.loans_table, str(cfg.path(a.out)))
        print(f"\n{s['snapshots']} snapshots. Table: {fmt_bytes(s['first_table_bytes'])} at the first snapshot, {fmt_bytes(s['last_table_bytes'])} at the last.")
        print(f"Storage actually kept for all of them: {fmt_bytes(s['cumulative_retained_bytes'])} "
              f"({s['history_overhead_ratio']:.2f}x the current table).")
        print(f"If every snapshot were a full copy: {fmt_bytes(s['full_copy_cumulative_bytes'])} (sharing saves {s['savings_factor']:.1f}x).")
        print(f"Average share of a snapshot reused from the previous one: {s['avg_shared_pct_after_first']:.1f}%. Commit types: {s['operations']}")
        print("Files:"); [print("  ", f) for f in s["files"]]
    elif a.cmd == "snapshot-diff":
        import json
        from .snapshot_storage import derive, diff_between, read_summaries
        print(json.dumps(diff_between(derive(read_summaries(spark, a.table or cfg.loans_table)), a.a, a.b), indent=2))
    elif a.cmd == "snapshots":
        from .snapshots import list_snapshots
        print(list_snapshots(spark, cfg.loans_table).to_string(index=False))
    elif a.cmd == "fingerprint":
        from .snapshots import current_snapshot_id, fingerprint, read_at_snapshot
        sid = current_snapshot_id(spark, cfg.loans_table)
        print("snapshot", sid, fingerprint(read_at_snapshot(spark, cfg.loans_table, sid)))


if __name__ == "__main__":
    main()
