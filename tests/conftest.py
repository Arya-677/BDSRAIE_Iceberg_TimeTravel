import os
import shutil
from pathlib import Path

import pytest

from ttml.config import load_config

ROOT = Path(__file__).resolve().parents[1]

# ENFORCED INVARIANT: nothing in this project may need a Spark Python worker process (it crashes on some Windows/macOS
# setups, see src/ttml/rows.py). We point Spark at a deliberately missing worker executable, so any code path that
# secretly needs one fails the tests on EVERY platform, not only on the machines where workers are broken.
# Set TTML_ALLOW_PYTHON_WORKERS=1 to disable the guard (e.g. when debugging).
if os.environ.get("TTML_ALLOW_PYTHON_WORKERS") != "1":
    os.environ["PYSPARK_PYTHON"] = "ttml-python-workers-are-forbidden-in-tests"


def rows_df(spark, rows, ddl):
    """DataFrame from local Python rows, JVM-only (no Python worker). Use this instead of spark.createDataFrame."""
    from ttml.rows import frame_from_rows
    return frame_from_rows(spark, rows, ddl)


@pytest.fixture(scope="session")
def cfg():
    return load_config(ROOT / "conf" / "pipeline.yaml")


@pytest.fixture(scope="session")
def spark_env(tmp_path_factory, cfg):
    """One Spark session for the whole run.  TTML_ICEBERG selects the mode:

      (unset)  plain local Spark. Cleaning/validation/fingerprint/training tests run; Iceberg tests skip.
      sim      plain Spark + tests/iceberg_sim.py, a SIMULATOR of the Iceberg subset we use. Exercises orchestration
               logic where the Iceberg jar is unavailable. NOT proof of real Iceberg behaviour.
      1        the real Iceberg runtime (needs the jar: internet on first run). The authoritative mode.
    Yields (plain_session, iceberg_session_or_None, mode).
    """
    mode = os.environ.get("TTML_ICEBERG", "")
    if mode == "1":
        from ttml.spark_session import get_spark
        wh = tmp_path_factory.mktemp("warehouse")
        s = get_spark(cfg, warehouse=str(wh), app_name="ttml-tests")
        s.sparkContext.setLogLevel("ERROR")
        yield s, s, "real"
        s.stop()
        shutil.rmtree(wh, ignore_errors=True)
    else:
        from ttml.spark_session import get_plain_spark
        s = get_plain_spark()
        s.sparkContext.setLogLevel("ERROR")
        if mode == "sim":
            from iceberg_sim import SimSpark
            yield s, SimSpark(s), "sim"
        else:
            yield s, None, "plain"
        s.stop()


@pytest.fixture(scope="session", autouse=True)
def _record_engine_mode(record_testsuite_property, spark_env):
    """Written into junit.xml so the audit report can tell real-Iceberg results from simulator results."""
    record_testsuite_property("ttml_iceberg_mode", spark_env[2])


@pytest.fixture(scope="session")
def spark(spark_env):
    return spark_env[0]


@pytest.fixture()
def iceberg_spark(spark_env):
    if spark_env[1] is None:
        pytest.skip("set TTML_ICEBERG=sim (simulator) or TTML_ICEBERG=1 (real Iceberg jar) to run Iceberg tests")
    return spark_env[1]


@pytest.fixture()
def real_iceberg_spark(spark_env):
    if spark_env[2] != "real":
        pytest.skip("needs the real Iceberg runtime (TTML_ICEBERG=1): file sizes, UPDATE and metadata tables are not simulated")
    return spark_env[1]


@pytest.fixture(scope="session")
def synthetic_csv(tmp_path_factory):
    from ttml.data_gen import write_csv
    p = tmp_path_factory.mktemp("raw") / "synthetic_lc.csv"
    write_csv(str(p), start="2015-01", end="2015-04", rows_per_month=400, seed=7)
    return str(p)


@pytest.fixture(scope="session")
def synthetic_csv8(tmp_path_factory):
    from ttml.data_gen import write_csv
    p = tmp_path_factory.mktemp("raw8") / "synthetic_lc8.csv"
    write_csv(str(p), start="2015-01", end="2015-08", rows_per_month=500, seed=5)
    return str(p)
