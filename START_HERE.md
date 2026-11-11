# Start here (Mac)

1. Open **Terminal** (press Cmd+Space, type `Terminal`, press Enter).
2. Type `cd ` (with a space), drag the unzipped **iceberg-ml-timetravel** folder into the Terminal window, press Enter.
3. Type this and press Enter:

       bash run.sh

4. Choose **1** the first time (installs everything and runs a quick check, about 5-10 minutes).
   When that says `[OK] Quick check passed`, run `bash run.sh` again and choose **3** (the full run).

If anything stops with `[STOPPED]`, send me the file `logs/last_run.log` (in the same folder).

Shortcuts: `bash run.sh check` | `bash run.sh full` | `bash run.sh mydata /path/to/file.csv` | `bash run.sh storage`
Results: `reports/audit_readiness_report.pdf` (opens automatically at the end of option 3).

## Using your real dataset (needed for the final submission)
Your LendingClub extract works as it is: the loan id may sit in a first column with a blank header, `issue_d` may look like
`Jun-17` or `Jun-2017`, and the file may be comma- or tab-separated, plain or `.gz`. It does not need the credit-score columns.
1. Save the file anywhere (for example your Downloads folder). Check and note its licence.
2. Run `bash run.sh`, choose **5**, drag the file into the Terminal window, press Enter, then Enter again for the first month
   (the story needs 14 consecutive months that have loans with a final outcome, e.g. 2013-01 to 2014-02).
3. It runs training, exact retrain, bad batch, rollback and retention on YOUR data and builds the PDF.
4. Fill in every TODO in `reports/data_card.md` and the sections marked TEAM TO COMPLETE in the report.

## Storage difference between snapshots (for your professor)
The report (option 3 or 5) has a section **6.1 Storage difference between snapshots** with graphs and a table. To get the graphs for
the data you loaded yourself, run option 4 and then option **6** (or `bash run.sh storage`). Files are in `reports/storage/`:
- `snapshot_storage_growth.png`: table size per snapshot, storage actually kept, and what full copies would cost
- `snapshot_storage_changes.png`: bytes added/removed by each snapshot and how much is shared with the previous one
- `snapshot_storage.csv`: the numbers. Compare two snapshots: `ttml snapshot-diff <id-a> <id-b>`
- `snapshot_storage_compare.png` (from the experiments): append-only vs copy-on-write vs merge-on-read
