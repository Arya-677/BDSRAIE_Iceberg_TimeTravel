# Time Travel for Reproducible ML (Apache Iceberg)

Credit-default training pipeline where **every model version is bound to the Iceberg snapshot it was trained on**,
with exact retraining, rollback, storage-overhead measurement, a retention policy and an audit-readiness report.

## Windows / macOS notes (read this if tests fail with "Python worker exited unexpectedly")
Spark can crash when it launches a Python worker process (`EOFException ... pyspark.daemon`). This project is built so
that **nothing needs a Python worker**: audit rows are built by the JVM (`src/ttml/rows.py`, never `createDataFrame`), there
are no UDFs, and the test suite runs with a deliberately missing worker executable to enforce that on every platform.
If you still see worker errors, check:
- You are running inside the project's virtualenv. Spark is pointed at the current interpreter automatically
  (`PYSPARK_PYTHON`); on Windows the default `python3` is often missing or the Microsoft Store alias.
- Java 11 or 17 is installed and `JAVA_HOME` is set (`java -version`).
- For writing Iceberg tables on native Windows, Hadoop usually needs `winutils.exe` / `hadoop.dll` (`HADOOP_HOME`). The
  easiest route is **WSL2** (Ubuntu) or Docker, where none of this applies.
- Use `python scripts/run_demo.py` instead of the `.sh` script on Windows.
- Contributors: never call `spark.createDataFrame(rows)`; use `ttml.rows.frame_from_rows` (or `rows_df` in tests).
  `tests/test_rows_and_workers.py` fails if you do. Pass timezone-aware datetimes only; naive ones are rejected.

## What is implemented
| Piece | Module |
|---|---|
| Config, feature allow-list (hash recorded per model) | `config.py`, `conf/` |
| Cleaning, target, table DDL | `schema.py` |
| Ingest gate (validation) + monthly ingest, one snapshot per month, ingest log | `validation.py`, `ingest.py` |
| Snapshot helpers, order-independent data fingerprint, tags, rollback, lineage | `snapshots.py` |
| Append-only model registry, status events, ingest log, rollback log | `registry.py` |
| Deterministic features and training; pins snapshot, registers, tags | `features.py`, `train.py` |
| Exact retrain from a registry row + L1/L2/L3 comparison | `retrain.py` |
| Bad-batch injection, post-hoc audit, quarantine + rollback + proof | `faults.py`, `rollback.py` |
| Tiered retention + safety guard | `retention.py` |
| Storage overhead measurement and experiments E1-E5 | `storage_metrics.py`, `storage_experiments.py` |
| End-to-end evidence run | `demo.py` |
| Audit-readiness report (MD + PDF) from evidence files | `audit_report.py` |

## Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"           # installs the package and the `ttml` command (same as `ttml`)
java -version                     # Java 11 or 17 (21 works with Spark 3.5.3)
pip freeze > environment.lock     # hashed into every registry row
```
All relative paths in `conf/pipeline.yaml` resolve against the project root, so commands work from any directory.
Edit `conf/pipeline.yaml`: set `catalog.iceberg_version` to a release that has
`iceberg-spark-runtime-3.5_2.12` on Maven Central (needs internet on first run; the default 1.5.2 is a placeholder to verify).

## Run everything (synthetic data, ~minutes)
```bash
bash scripts/run_demo.sh        # (or: python scripts/run_demo.py on Windows) (real Iceberg) demo -> storage experiments -> pytest (junit) -> reports/audit_readiness_report.pdf
```
Or step by step:
```bash
ttml demo                         # reports/demo_results.json  (retrain L1/L2/L3, rollback, retention)
ttml experiments --quick          # reports/storage/*.csv, *.png   (drop --quick for 96-snapshot E1)
TTML_ICEBERG=1 pytest -q --junitxml=reports/junit.xml
ttml report                       # reports/audit_readiness_report.{md,pdf}
```

## Real data
1. Download the LendingClub accepted-loans CSV from Kaggle (check the licence), put it in `data/raw/`, set `data.source_csv`.
2. Compare column names with `conf/features_allowlist.txt` (cleaning fails loudly if a column is missing).
3. `ttml init && ttml ingest --start 2012-01 --end 2016-12`
4. `ttml train v1` (pins the current snapshot; split from `training.train_end`).
5. Fill in `reports/data_card.md`, rerun the report.

## Everyday commands
```bash
ttml train v2 [--snapshot-id N]     ttml retrain v1 v1-again     ttml compare v1 v1-again
ttml promote v1 approved            ttml audit                   ttml rollback --bad-batch 2016-01
ttml retention [--dry-run]          ttml storage                 ttml snapshots / fingerprint
```
(`ttml` is installed by `pip install -e .`; `python -m ttml.cli` is equivalent)

## Tests
```bash
pytest -q                           # no Iceberg needed: cleaning, validation, fingerprint, deterministic training, lineage, report
TTML_ICEBERG=1 pytest -m iceberg -q # end-to-end T1-T6 + rollback, retention guard, storage (needs the Iceberg jar)
```
| Control | Test (in `tests/test_reproducibility_iceberg.py`) |
|---|---|
| T1 registry binds snapshot | `test_registry_binds_snapshot_to_every_model` |
| T2 exact retrain after table moves | `test_exact_retrain_after_table_moves` |
| T3 time travel | `test_time_travel_returns_old_data` |
| T4 rollback + quarantine | `test_rollback_restores_state_and_quarantines_tainted_models` |
| T5 retention keeps evidence / guard blocks unsafe cleanup | `test_retention_preserves_evidence`, `test_retention_guard_blocks_unsafe_cleanup` |
| T6 negative control | `test_negative_control_unpinned_head_differs` |

## Things you must still do / verify
- Run the Iceberg tests on a machine with internet (or a pre-downloaded jar). Parts of the Iceberg SQL (tag syntax, metadata table names such as `all_files`, procedure arguments) depend on the Iceberg version you pin; fix any mismatch the first run reveals.
- Complete `reports/data_card.md`, and the "TEAM TO COMPLETE" sections of the report (storage interpretation, erasure policy).
- `training.release_retention_days: 2555` is a placeholder; confirm the real retention requirement.
- Reproducibility is claimed for one environment (same library versions and CPU architecture). Say so in the report.
- Storage-experiment updates are synthetic. Run on a larger dataset than the defaults before drawing conclusions.
# BDSRAIE_Iceberg_TimeTravel
