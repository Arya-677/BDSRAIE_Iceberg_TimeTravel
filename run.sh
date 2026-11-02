#!/bin/bash
# ---------------------------------------------------------------------------------------------
#  Easy runner for macOS (also works on Linux / WSL).      Usage:   bash run.sh
#  It checks Java + Python, installs everything, runs the tests/demo, and logs to logs/last_run.log
# ---------------------------------------------------------------------------------------------
set -u
cd "$(dirname "$0")" || exit 1
ROOT="$(pwd)"
mkdir -p logs reports
LOG="logs/last_run.log"

say()  { printf "\n\033[1m==> %s\033[0m\n" "$*"; }
ok()   { printf "\033[32m[OK] %s\033[0m\n" "$*"; }
warn() { printf "\033[33m[!] %s\033[0m\n" "$*"; }
die()  { printf "\n\033[31m[STOPPED] %s\033[0m\n" "$*"
         printf "    Log file: %s/%s\n    If you need help, send me that file (or paste its last 40 lines).\n" "$ROOT" "$LOG"; exit 1; }

# run a command, show output, and also append it to the log. returns the command's own exit code.
run() { "$@" 2>&1 | tee -a "$LOG"; return "${PIPESTATUS[0]}"; }

# ---------------------------------------------------------------- Java (needs 11, 17 or 21)
java_major() {            # $1 = path to a java executable; prints the major version, or nothing
  local ver major
  ver="$("$1" -version 2>&1 | awk -F '"' '/version/ {print $2; exit}')"
  [ -n "$ver" ] || return 0
  major="${ver%%.*}"
  if [ "$major" = "1" ]; then major="$(echo "$ver" | cut -d. -f2)"; fi
  echo "$major"
}

java_ok() { case "$(java_major "$1")" in 11|17|21) return 0 ;; *) return 1 ;; esac; }

find_java() {             # sets JAVA_HOME if a suitable Java is found; returns 1 otherwise
  local cand home v
  if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ] && java_ok "$JAVA_HOME/bin/java"; then return 0; fi
  if [ -x /usr/libexec/java_home ]; then
    for v in 17 21 11; do
      home="$(/usr/libexec/java_home -v "$v" 2>/dev/null)" || home=""
      if [ -n "$home" ] && [ -x "$home/bin/java" ] && java_ok "$home/bin/java"; then export JAVA_HOME="$home"; return 0; fi
    done
  fi
  for cand in /opt/homebrew/opt/openjdk@17 /usr/local/opt/openjdk@17 /opt/homebrew/opt/openjdk@21 /usr/local/opt/openjdk@21 \
              /opt/homebrew/opt/openjdk@11 /usr/local/opt/openjdk@11 /opt/homebrew/opt/openjdk /usr/local/opt/openjdk; do
    if [ -x "$cand/bin/java" ] && java_ok "$cand/bin/java"; then export JAVA_HOME="$cand"; return 0; fi
  done
  if command -v java >/dev/null 2>&1 && java_ok "$(command -v java)"; then
    home="$(dirname "$(dirname "$(command -v java)")")"; export JAVA_HOME="$home"; return 0
  fi
  return 1
}

ensure_java() {
  say "Checking Java"
  if find_java; then export PATH="$JAVA_HOME/bin:$PATH"; ok "Java found ($JAVA_HOME)"; return 0; fi
  warn "No suitable Java (11, 17 or 21) found. Spark needs it."
  if command -v brew >/dev/null 2>&1; then
    printf "Install Java 17 now with Homebrew? (takes ~1 minute) [y/N] "; read -r ans
    if [ "$ans" = "y" ] || [ "$ans" = "Y" ]; then
      run brew install openjdk@17 || die "Homebrew could not install Java. Try:  brew install openjdk@17"
      find_java && { export PATH="$JAVA_HOME/bin:$PATH"; ok "Java installed ($JAVA_HOME)"; return 0; }
      die "Java installed but not found. Close and reopen Terminal, then run:  bash run.sh"
    fi
  fi
  die "Please install Java 17, then run this script again.
      Easiest: install Homebrew from https://brew.sh then run:  brew install openjdk@17
      Or download 'Temurin 17 (macOS)' from https://adoptium.net and install it."
}

# ---------------------------------------------------------------- Python (needs 3.10 - 3.12)
PY=""
find_python() {
  local p
  for p in python3.12 python3.11 python3.10 python3; do
    if command -v "$p" >/dev/null 2>&1 && "$p" -c 'import sys; sys.exit(0 if (3,10) <= sys.version_info[:2] <= (3,12) else 1)' 2>/dev/null; then
      PY="$(command -v "$p")"; return 0
    fi
  done
  return 1
}

# ---------------------------------------------------------------- environment setup
setup() {
  ensure_java
  say "Checking Python"
  if [ -x .venv/bin/python ] && .venv/bin/python -c 'import ttml, pyspark, lightgbm, reportlab, distutils, pandas; assert int(pandas.__version__.split(".")[0]) < 3' >/dev/null 2>&1; then
    ok "Environment already set up"; . .venv/bin/activate; return 0
  fi
  find_python || die "Need Python 3.10, 3.11 or 3.12 (your Mac's built-in Python is too old).
      Easiest: brew install python@3.12      (needs Homebrew: https://brew.sh)
      Or download Python 3.12 from https://www.python.org/downloads/macos/   then run:  bash run.sh"
  ok "Using $PY ($("$PY" --version 2>&1))"
  say "Creating the virtual environment and installing packages (first time only, 3-8 minutes)"
  [ -x .venv/bin/python ] || run "$PY" -m venv .venv || die "Could not create the virtual environment."
  . .venv/bin/activate
  run python -m pip install --upgrade pip || die "pip upgrade failed (check your internet connection)."
  run pip install -e ".[dev]" || die "Package install failed (check your internet connection)."
  pip freeze > environment.lock
  ok "Setup complete"
}

export SPARK_LOCAL_IP=127.0.0.1       # avoids Spark hostname / VPN networking problems on macs
export PYTHONUNBUFFERED=1

# ---------------------------------------------------------------- steps
step_check() {
  setup
  say "Quick check: plain tests (no Iceberg needed, about 1-2 minutes)"
  run python -m pytest -q -p no:cacheprovider
  local rc=$?
  if [ $rc -eq 0 ]; then ok "Quick check passed. Next:  bash run.sh full"; else die "Some quick-check tests failed."; fi
}

iceberg_version() { python -c "import yaml;print(yaml.safe_load(open('conf/pipeline.yaml'))['catalog']['iceberg_version'])"; }

check_jar() {
  local v url code
  v="$(iceberg_version)"
  url="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/$v/iceberg-spark-runtime-3.5_2.12-$v.jar"
  code="$(curl -sIL -m 25 -o /dev/null -w '%{http_code}' "$url" 2>/dev/null)"
  if [ "$code" = "200" ]; then ok "Iceberg $v is downloadable"; return 0; fi
  warn "Could not confirm Iceberg $v (HTTP '$code'). Either you are offline, or that version does not exist."
  warn "If offline: connect and retry. If the version is wrong: edit conf/pipeline.yaml (catalog.iceberg_version),"
  warn "e.g. 1.5.2 or 1.6.1, see https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/"
  return 1
}

step_real() {
  setup
  say "Real Iceberg tests (first run downloads the Iceberg jar, a few minutes)"
  check_jar || die "Fix the Iceberg download problem above, then run again."
  mkdir -p reports
  TTML_ICEBERG=1 run python -m pytest -q -p no:cacheprovider --junitxml=reports/junit.xml
  local rc=$?
  if [ $rc -eq 0 ]; then ok "Real Iceberg tests PASSED"; else die "Some real-Iceberg tests failed (this is expected to need a few fixes the first time). Send me the log."; fi
}

step_full() {
  step_real
  say "Running the end-to-end demo (train, exact retrain, bad batch, rollback, retention)"
  run python -m ttml.cli demo || die "The demo failed."
  say "Running the storage experiments (quick version)"
  run python -m ttml.cli experiments --quick || die "The storage experiments failed."
  say "Building the audit-readiness report"
  run python -m ttml.cli report || die "The report build failed."
  ok "ALL DONE.  Report: $ROOT/reports/audit_readiness_report.pdf"
  if command -v open >/dev/null 2>&1; then open reports/audit_readiness_report.pdf 2>/dev/null; fi
}

step_data() {   # bash run.sh data /path/to/lendingclub.csv [start-month] [end-month]
  local csv="${1:-}" start="${2:-2012-01}" end="${3:-2013-12}"
  [ -n "$csv" ] && [ -f "$csv" ] || die "Usage: bash run.sh data /full/path/to/accepted_loans.csv [2012-01] [2013-12]"
  setup
  check_jar || die "Fix the Iceberg download problem above, then run again."
  say "Loading $csv ($start to $end), one snapshot per month"
  run python -m ttml.cli init || die "init failed"
  python - "$csv" <<'PY' || die "Could not point the config at your CSV."
import sys, pathlib
p = pathlib.Path("conf/pipeline.yaml"); s = p.read_text(encoding="utf-8")
import re
s = re.sub(r"source_csv:.*", "source_csv: " + sys.argv[1], s, count=1)
p.write_text(s, encoding="utf-8")
PY
  run python -m ttml.cli ingest --start "$start" --end "$end" || die "ingest failed (the message above says which column or batch)."
  run python -m ttml.cli snapshots
  say "Training v1 on the pinned snapshot, then retraining it exactly"
  run python -m ttml.cli train v1 --train-end "${end}-28" || die "training failed"
  run python -m ttml.cli retrain v1 v1-again || die "retrain failed"
  run python -m ttml.cli compare v1 v1-again
  ok "Done. 'L1/L2/L3 true' above means the retrained model is identical."
}

menu() {
  cat <<'M'

  What do you want to do?
    1) Quick check             (installs everything, runs the basic tests: start here)
    2) Real Iceberg tests      (downloads Iceberg, runs the full test suite)
    3) FULL RUN                (tests + demo + experiments + audit report PDF)
    4) Use my own LendingClub CSV
    5) Quit
M
  printf "  Type 1-5 and press Enter: "; read -r c
  case "$c" in
    1) step_check ;;
    2) step_real ;;
    3) step_full ;;
    4) printf "  Drag your CSV file into this window, then press Enter: "; read -r f; f="${f%\"}"; f="${f#\"}"; f="${f//\\ / }"; step_data "$f" ;;
    *) exit 0 ;;
  esac
}

[ "${RUN_SH_SOURCE_ONLY:-0}" = "1" ] && return 0 2>/dev/null
: > "$LOG"
case "${1:-menu}" in
  check) step_check ;;
  real)  step_real ;;
  full)  step_full ;;
  data)  shift; step_data "$@" ;;
  setup) setup ;;
  menu|*) menu ;;
esac
