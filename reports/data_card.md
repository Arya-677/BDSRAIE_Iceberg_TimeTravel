### Data card: LendingClub accepted loans (TEAM TO COMPLETE every field marked TODO)

- **Source:** Kaggle "Lending Club" accepted-loans file. TODO: exact dataset URL, version/date downloaded, file SHA-256 (the ingest log records the SHA-256 of the file actually loaded).
- **Licence / access terms:** TODO: copy from the Kaggle dataset page at download time.
- **Rows used:** TODO: months loaded (see `data.start_month`/`end_month` in `conf/pipeline.yaml`), rows after cleaning (see `ingest_log`).
- **Target:** `loan_status` Fully Paid -> 0, Charged Off -> 1 (including the "Does not meet the credit policy" variants). Loans still current, late, in grace period or defaulted-but-not-charged-off have no final outcome and are dropped.
- **Features used:** the allow-list in `conf/features_allowlist.txt` (hash recorded in every registry row). Origination-time fields only.
- **Excluded on purpose:** post-origination fields (total payments, recoveries, last payment date, last credit pull, outstanding principal) because they leak the outcome; `addr_state` and `zip_code` as geographic proxies.
- **Known limitations:** historical data from one lender, selection bias (only approved loans), label timing (recent loans are mostly unresolved), not validated for real lending decisions. TODO: add anything you found.
- **Sensitive data:** do not attempt to re-identify borrowers.
- **Development runs** may use synthetic data from `ttml gen-data`, which is not statistically realistic. Say which data each reported result used.
