"""Cross-platform equivalent of scripts/run_demo.sh (works in Windows cmd/PowerShell, no bash needed).

    python scripts/run_demo.py

Runs against the REAL Iceberg runtime (TTML_ICEBERG=1): demo -> storage experiments -> tests -> audit report."""
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
env = {**os.environ, "TTML_ICEBERG": "1"}
for d in ("warehouse", "artifacts"):
    shutil.rmtree(d, ignore_errors=True)
steps = [
    [sys.executable, "-m", "ttml.cli", "demo"],
    [sys.executable, "-m", "ttml.cli", "experiments", "--quick"],     # drop --quick for the full 96-snapshot E1
    [sys.executable, "-m", "pytest", "-q", "--junitxml=reports/junit.xml"],
    [sys.executable, "-m", "ttml.cli", "report"],
]
for cmd in steps:
    print(">", " ".join(cmd[1:]), flush=True)
    if subprocess.call(cmd, env=env) != 0:
        sys.exit(f"step failed: {' '.join(cmd[1:])}")
print("Report: reports/audit_readiness_report.pdf")
