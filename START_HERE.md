# Start here (Mac)

1. Open **Terminal** (press Cmd+Space, type `Terminal`, press Enter).
2. Type `cd ` (with a space), drag the unzipped **iceberg-ml-timetravel** folder into the Terminal window, press Enter.
3. Type this and press Enter:

       bash run.sh

4. Choose **1** the first time (installs everything and runs a quick check, about 5-10 minutes).
   When that says `[OK] Quick check passed`, run `bash run.sh` again and choose **3** (the full run).

If anything stops with `[STOPPED]`, send me the file `logs/last_run.log` (in the same folder).

Shortcuts: `bash run.sh check` | `bash run.sh full` | `bash run.sh data /path/to/file.csv`
Results: `reports/audit_readiness_report.pdf` (opens automatically at the end of option 3).
