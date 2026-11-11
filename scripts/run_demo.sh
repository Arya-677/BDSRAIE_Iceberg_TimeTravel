#!/usr/bin/env bash
# Full evidence run: demo story -> storage experiments -> tests -> audit-readiness report.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf warehouse artifacts
python -m ttml.cli demo
python -m ttml.cli experiments --quick          # drop --quick for the full run (96 snapshots in E1)
TTML_ICEBERG=1 python -m pytest -q --junitxml=reports/junit.xml
python -m ttml.cli report
echo "Report: reports/audit_readiness_report.pdf"
