"""Source-to-table schema: parsing LendingClub-style columns, target definition, table DDL.

<<<<<<< HEAD
Column names/format follow the commonly used LendingClub extracts: issue_d like 'Dec-2015' OR 'Dec-15' (Excel shortens it),
term like ' 36 months', emp_length like '10+ years'. Verify against your download; adjust only this module if it differs.
=======
Column names/format follow the commonly used Kaggle LendingClub file
(issue_d like 'Dec-2015', term like ' 36 months', emp_length like '10+ years').
Verify against your download; adjust only this module if it differs.
>>>>>>> origin/main
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import Config

# loan_status values mapped to the binary target. Anything else (Current, Late, In Grace
# Period, Default, ...) has no final outcome yet and is dropped -- document this in the data card.
GOOD = ["Fully Paid", "Does not meet the credit policy. Status:Fully Paid"]
BAD = ["Charged Off", "Does not meet the credit policy. Status:Charged Off"]

NUMERIC_COLS = [
<<<<<<< HEAD
    "loan_amnt", "int_rate", "installment", "annual_inc", "dti", "delinq_2yrs", "inq_last_6mths", "fico_range_low",
=======
    "loan_amnt", "int_rate", "annual_inc", "dti", "delinq_2yrs", "fico_range_low",
>>>>>>> origin/main
    "fico_range_high", "open_acc", "pub_rec", "revol_bal", "revol_util", "total_acc",
]
STRING_COLS = ["grade", "home_ownership", "verification_status", "purpose"]


<<<<<<< HEAD
def parse_issue_d(col):
    """'Jun-2017' or 'Jun-17' -> DATE 2017-06-01 (null if it does not look like Mon-YY / Mon-YYYY).

    The month and year are extracted with a regex and a 4-digit year is rebuilt BEFORE calling to_date: feeding '17' to a
    'yyyy' pattern makes Spark raise SparkUpgradeException (new parser rejects it, legacy parser would accept). Two-digit
    years use the POSIX pivot (00-68 -> 20xx, 69-99 -> 19xx); LendingClub issue dates are 2007-2018 so this is unambiguous."""
    pat = r"^([A-Za-z]{3})-(\d{2}|\d{4})$"
    mon, yr = F.regexp_extract(col, pat, 1), F.regexp_extract(col, pat, 2)
    yr4 = F.when(F.length(yr) == 2, F.concat(F.when(yr.cast("int") <= 68, F.lit("20")).otherwise(F.lit("19")), yr)).otherwise(yr)
    return F.when(F.length(mon) == 3, F.to_date(F.concat(F.lit("01-"), mon, F.lit("-"), yr4), "dd-MMM-yyyy"))


=======
>>>>>>> origin/main
def required_source_columns(features: list[str]) -> list[str]:
    """Raw columns we need to read. Everything else in the 150-column file is ignored on purpose."""
    base = {"id", "issue_d", "loan_status"}
    return sorted(base | set(features))


def clean(df: DataFrame, features: list[str]) -> DataFrame:
    """Raw string columns -> typed, origination-time-only frame with target and issue_month.

    Output columns: loan_id, issue_d, issue_month ('YYYY-MM'), <features>, target
    Rows without a numeric id, a parseable issue_d, or a final outcome are dropped.
    """
    missing = [c for c in required_source_columns(features) if c not in df.columns]
    if missing:
        raise ValueError(f"source file is missing expected columns: {missing}")

    s = lambda c: F.trim(F.col(c).cast("string"))  # noqa: E731

    out = df.select(
        F.when(s("id").rlike(r"^\d+$"), s("id").cast("bigint")).alias("loan_id"),
<<<<<<< HEAD
        parse_issue_d(s("issue_d")).alias("issue_d"),
=======
        F.to_date(F.concat(F.lit("01-"), s("issue_d")), "dd-MMM-yyyy").alias("issue_d"),
>>>>>>> origin/main
        s("loan_status").alias("_status"),
        *[F.col(c) for c in features],
    )

    # --- feature parsing -----------------------------------------------------
    if "term" in features:
        out = out.withColumn("term", F.regexp_extract(F.col("term").cast("string"), r"(\d+)", 1).cast("int"))
    if "emp_length" in features:
        e = F.trim(F.col("emp_length").cast("string"))
        out = out.withColumn(
            "emp_length",
            F.when(e.startswith("<"), F.lit(0.0))
            .when(e.startswith("10+"), F.lit(10.0))
            .otherwise(F.regexp_extract(e, r"(\d+)", 1).cast("double")),   # null if 'n/a' / missing
        )
    for c in NUMERIC_COLS:
        if c in features:
            # tolerate '13.56%' style values as well as plain numbers
            out = out.withColumn(c, F.regexp_replace(F.col(c).cast("string"), "%", "").cast("double"))
    for c in STRING_COLS:
        if c in features:
            out = out.withColumn(c, F.trim(F.col(c).cast("string")))

    # --- target --------------------------------------------------------------
    out = out.withColumn(
        "target",
        F.when(F.col("_status").isin(BAD), F.lit(1)).when(F.col("_status").isin(GOOD), F.lit(0)).cast("int"),
    ).drop("_status")

    out = (
        out.filter(F.col("loan_id").isNotNull() & F.col("issue_d").isNotNull() & F.col("target").isNotNull())
        .withColumn("issue_month", F.date_format("issue_d", "yyyy-MM"))
    )
    return out.select("loan_id", "issue_d", "issue_month", *features, "target")


def loans_ddl(cfg: Config, table: str | None = None, write_mode: str | None = None) -> str:
    """CREATE TABLE for the Iceberg loans table. Column list is derived from the allow-list so
    feature changes are explicit and reviewable. Types mirror clean()."""
    features = cfg.features()
    type_of = {"term": "INT", "emp_length": "DOUBLE"}
    cols = ["loan_id BIGINT", "issue_d DATE"]
    for f in features:
        t = type_of.get(f) or ("DOUBLE" if f in NUMERIC_COLS else "STRING")
        cols.append(f"{f} {t}")
    cols += ["target INT", "ingest_batch STRING"]
    p = cfg.raw["table_properties"]
    mode = write_mode or p["write_mode"]
    props = {
        "format-version": "2",
        "write.delete.mode": mode,
        "write.update.mode": mode,
        "write.merge.mode": mode,
        "history.expire.max-snapshot-age-ms": str(p["expire_max_snapshot_age_ms"]),
        "history.expire.min-snapshots-to-keep": str(p["expire_min_snapshots_to_keep"]),
    }
    props_sql = ",\n  ".join(f"'{k}' = '{v}'" for k, v in props.items())
    return (
        f"CREATE TABLE IF NOT EXISTS {table or cfg.loans_table} (\n  " + ",\n  ".join(cols) + "\n)\n"
        "USING iceberg\nPARTITIONED BY (months(issue_d))\n"
        f"TBLPROPERTIES (\n  {props_sql}\n)"
    )
