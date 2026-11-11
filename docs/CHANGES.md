# Changes in 0.6.0 (storage by snapshot + the team's real dataset)

New: storage difference between snapshots (requested by the professor)
- `ttml snapshot-storage`: for EVERY snapshot of the loans table: size as of that snapshot, bytes added / removed by that commit, share
  reused from the previous snapshot, storage actually kept (shared files stored once), and the cost if every snapshot were a full copy.
  Writes `reports/storage/snapshot_storage.csv` + `snapshot_storage_growth.png` + `snapshot_storage_changes.png`.
  Source: Iceberg's own per-commit snapshot summaries (`total-files-size`, `added-files-size`, `removed-files-size`), so no data is scanned.
- `ttml snapshot-diff A B`: storage difference between any two snapshots.
- The demo captures it BEFORE retention expires snapshots; the audit report has a new section 6.1 with the table and graphs, plus a
  cross-check of the per-commit summaries against Iceberg's file listing (reported honestly, including any difference).
- The storage experiments also write per-snapshot reports and `snapshot_storage_compare.png` (append-only vs copy-on-write vs
  merge-on-read, 5% monthly corrections).
- `bash run.sh storage` (menu option 6) generates the graphs for the data you loaded.

Fixed: compatibility with the team's LendingClub extract (checked against a reconstruction of the pasted sample; tests/test_user_dataset_sample.py)
- `fico_range_low/high` were REQUIRED features but that extract has no credit-score columns: removed; added `installment`, `inq_last_6mths`.
- `issue_d` like `Jun-17` (2-digit year, as Excel writes it) was not parsed, so every row would have been dropped. Now both `Jun-17` and `Jun-2017`
  work (regex + rebuilt 4-digit year; feeding '17' to a 'yyyy' pattern would raise SparkUpgradeException).
- The loan id sits in a first column with a BLANK header (and `member_id` is empty): the id column is now auto-detected
  (`id`, `loan_id`, blank first column, `Unnamed: 0`).
- Tab-separated, semicolon, pipe, gzip and UTF-8-BOM files are detected automatically. Missing columns / no id column give a plain-English error.
- `ttml ingest --skip-rejected`: recent months with few resolved loans can fail the ingest gate; they can now be skipped instead of stopping the load.
- The synthetic generator now mirrors that extract (blank id header, `Jun-17` dates, no fico columns) so the tests exercise the same shape.

Also (redone after a sandbox reset): `ttml demo --csv FILE --start YYYY-MM` runs the whole evidence story on YOUR file (any 14 consecutive
months), `bash run.sh` option 5, automatic local git commit so model records carry a real commit (not `no-git`), Spark driver memory 4g
(`spark.driver_memory` in conf/pipeline.yaml) for the full-size file, and the report names the dataset used and warns when it was synthetic.

# Changes in 0.5.0 (first real-Iceberg run on a Mac)

Reported from the first run on real Iceberg (macOS, Java 11): 105 passed, 5 failed. All 5 had one cause:
`AttributeError: 'NoneType' object has no attribute 'total_seconds'` inside `DataFrame.toPandas()`.

Root cause: PySpark 3.5.x localises timestamp columns with `tz_localize("tzlocal()")`; with pandas >= 3.1 this crashes whenever the
machine timezone is not UTC and TZ is unset (every Mac). CI containers run in UTC, so it was invisible there. Reproduced here by
setting the container's system timezone to IST with pandas 3.1.0 (the 2 tests I tried failed identically before the fix).

Fixed
- `ttml/frames.py::to_pandas`: collects timestamps as exact epoch microseconds and builds UTC-aware pandas times; `toPandas()` is no
  longer used anywhere (a test forbids it, and another proves the helper never calls it).
- `pandas<3` pinned (PySpark 3.5 predates pandas 3); `run.sh` detects an old pandas 3 environment and reinstalls.

Verified here under an IST system timezone with TZ unset: pandas 3.1.0 plain 100 passed + simulator 115 passed (3 real-only
skipped); pandas 2.3.3 plain 100 passed + simulator 115 passed. Real Iceberg on your Mac is the remaining check.

# Changes in 0.4.0 (easy Mac runner)

- `run.sh` + `START_HERE.md`: one command (`bash run.sh`) checks Java/Python, offers to install Java via Homebrew, creates the
  virtualenv, installs everything, runs the tests/demo/report, and logs to `logs/last_run.log`. Java detection accepts 11/17/21
  and rejects Java 8, 22+ and the macOS "no Java installed" stub (tested with fake executables).
- Bug found by testing in a FRESH virtualenv: Python 3.12 has no `distutils`, which PySpark 3.5 imports (`toPandas`); it was only
  present in my sandbox by accident. `setuptools` is now a dependency and the runner verifies it.
- Spark binds to 127.0.0.1 (driver host/bindAddress) to avoid hostname/VPN problems on laptops.

# Changes in 0.3.0 (Windows / Python-worker fix)

Reported: plain-Spark tests (fingerprint, schema_clean, validation) failed on the first version: 10 failures, all from
`spark.createDataFrame(list_of_rows)` needing a Python worker process, which crashes on some Windows setups.

Root cause was in the product, not only the tests: the model registry, status events, ingest log and rollback log all
wrote their rows through `createDataFrame`, so the real pipeline would have failed at its first audit write.

Fixed
- `ttml/rows.py` (`frame_from_rows`): JVM-only row construction via typed SQL literals (strings escaped, NaN/inf, structs, maps,
  arrays, nulls). Rejects naive datetimes (the old IST +5.5h class of bug can no longer be reintroduced). All four audit tables use it.
- Enforced invariant: the test session sets a deliberately missing Python worker executable, so any code path that needs a
  worker fails on EVERY platform. A test proves the guard is live; another statically forbids `createDataFrame(` in src/.
- Spark workers are pinned to the current interpreter (`PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON`, setdefault).
- Windows newline bug: model files were written in text mode (\n -> \r\n), so the registered SHA-256 would not match the file on disk.
  Now written as bytes; tested.
- All text file I/O uses UTF-8 (Windows default is cp1252); removed a Linux-only `time.tzset` test; `scripts/run_demo.py` added.
- Test helper `rows_df` (conftest) now delegates to `frame_from_rows` (your workaround is kept, but in one tested place).

Verified here (Linux, worker deliberately missing): plain suite 92 passed / 18 Iceberg tests skipped; simulator suite 107 passed / 3 real-only storage tests skipped.
Not verified: native Windows (no Windows machine available here) and the real Iceberg runtime.

# Changes in 0.2.0 (audit pass)

Fixed
- Timezone: datetimes sent to Spark are timezone-aware UTC (naive ones were reinterpreted in local time, e.g. IST = +5.5h).
- Paths resolve against the project root, not the working directory; stored artifact paths are absolute; `ttml` console script.
- CSV reader handles quoted multi-line fields (real LendingClub text columns); missing columns raise a clear ValueError.
- Batch outlier audit: generous floors so slow real-world drift is not flagged; gross errors still are.
- model_version / batch_id validated (SQL + tag-name safety; regex uses fullmatch).
- train(): tag the snapshot BEFORE registering the model; single-class split raises a clear error.
- git_dirty ignores untracked files; optional `training.require_clean_git` gate on approve/deploy.
- compare_models: both models predict on the same probe rows (crashed when snapshots differed).
- rollback: by batch id without relying on the audit; refuses snapshots not in current history.
- Report: integer formatting, image paths, and simulator results are labelled "NOT evidence".
- ingest: cache released on failure; empty batch handled before gate; optional per-month table fingerprint.

Added
- tests/iceberg_sim.py: simulator of the Iceberg subset used (logic tests only), TTML_ICEBERG=sim.
- tests for paths, identifier safety, CSV edge cases, drift, timezone round-trip, SQL syntax (parsed by Spark), demo end-to-end.

Verified here: plain suite 68 passed (18 Iceberg tests skipped); simulator suite 83 passed (3 real-only storage tests skipped).
NOT verified: anything against the real Iceberg runtime (TTML_ICEBERG=1). Not built: Home Credit adapter, MLflow mirror, cold-archive tier.
