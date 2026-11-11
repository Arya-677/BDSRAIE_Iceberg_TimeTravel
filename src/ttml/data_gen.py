"""Synthetic LendingClub-shaped data so you can build and test before downloading the real file.

Mimics the *format* of the Kaggle file (string issue_d 'Dec-2015', term ' 36 months', emp_length
'10+ years', a trailing footer row) including post-origination "leakage" columns that the
feature allow-list must exclude. It is NOT a statistical replica of real loans.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
GRADES = list("ABCDEFG")
PURPOSES = ["debt_consolidation", "credit_card", "home_improvement", "major_purchase", "small_business", "other"]
HOME = ["RENT", "MORTGAGE", "OWN"]
VERIF = ["Verified", "Source Verified", "Not Verified"]
EMP = ["< 1 year", "1 year", "2 years", "3 years", "5 years", "8 years", "10+ years", None]


def months_between(start: str, end: str) -> list[str]:
    s = pd.Period(start, freq="M")
    e = pd.Period(end, freq="M")
    return [str(p) for p in pd.period_range(s, e, freq="M")]


def generate(start: str = "2015-01", end: str = "2015-12", rows_per_month: int = 500, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    frames = []
    next_id = 10_000_000
    for m in months_between(start, end):
        n = rows_per_month
        year, mon = int(m[:4]), int(m[5:])
        grade_idx = rng.integers(0, 7, n)
        int_rate = np.clip(6 + 3.2 * grade_idx + rng.normal(0, 1.2, n), 5.3, 31.0).round(2)
        dti = np.clip(rng.normal(18, 8, n), 0, 60).round(2)
        annual_inc = np.exp(rng.normal(11.1, 0.5, n)).round(0)
        loan_amnt = (rng.integers(1, 40, n) * 1000).astype(float)
        installment = np.round(loan_amnt * (int_rate / 1200) / (1 - (1 + int_rate / 1200) ** -36), 2)
        fico = rng.integers(660, 820, n)
        logit = -3.0 + 0.12 * (int_rate - 12) + 0.025 * (dti - 18) - 0.01 * (fico - 700) + rng.normal(0, 0.5, n)
        p = 1 / (1 + np.exp(-logit))
        bad = rng.random(n) < p
        status = np.where(bad, "Charged Off", "Fully Paid")
        # ~8% still open -> must be dropped (no final outcome)
        status = np.where(rng.random(n) < 0.08, "Current", status)
        df = pd.DataFrame({
            "id": np.arange(next_id, next_id + n).astype(str),
            "loan_amnt": loan_amnt,
            "installment": installment,
            "inq_last_6mths": rng.poisson(0.7, n).astype(float),
            "term": rng.choice([" 36 months", " 60 months"], n, p=[0.75, 0.25]),
            "int_rate": int_rate,
            "grade": [GRADES[i] for i in grade_idx],
            "emp_length": rng.choice(EMP, n),
            "home_ownership": rng.choice(HOME, n),
            "annual_inc": annual_inc,
            "verification_status": rng.choice(VERIF, n),
            "issue_d": f"{MONTH_ABBR[mon - 1]}-{year}",
            "loan_status": status,
            "purpose": rng.choice(PURPOSES, n),
            "addr_state": rng.choice(["CA", "NY", "TX", "FL", "IL"], n),
            "dti": dti,
            "delinq_2yrs": rng.poisson(0.3, n).astype(float),
            "fico_range_low": fico.astype(float),
            "fico_range_high": (fico + 4).astype(float),
            "open_acc": rng.integers(2, 30, n).astype(float),
            "pub_rec": rng.poisson(0.15, n).astype(float),
            "revol_bal": np.round(np.exp(rng.normal(9.2, 1.0, n)), 0),
            "revol_util": np.clip(rng.normal(50, 25, n), 0, 120).round(1),
            "total_acc": rng.integers(3, 60, n).astype(float),
            # ---- post-origination LEAKAGE columns (must never be features) ----
            "total_pymnt": np.where(bad, loan_amnt * rng.uniform(0.1, 0.7, n), loan_amnt * rng.uniform(1.0, 1.2, n)).round(2),
            "recoveries": np.where(bad, loan_amnt * rng.uniform(0, 0.1, n), 0.0).round(2),
            "last_pymnt_d": "Jan-2019",
        })
        # a few messy values, as in real exports
        df.loc[df.sample(frac=0.01, random_state=int(rng.integers(0, 10**6))).index, "emp_length"] = "n/a"
        frames.append(df)
        next_id += n
    out = pd.concat(frames, ignore_index=True)
    return out


def _short_year(issue_d: str) -> str:
    return issue_d[:4] + issue_d[6:]          # 'Jun-2017' -> 'Jun-17'


def write_csv(path: str, style: str = "sample", **kw) -> str:
    """Write a synthetic LendingClub-shaped file.

    style='sample' (default) mirrors the real extract this project is checked against: the loan id sits in a FIRST COLUMN WITH A
    BLANK HEADER, an empty `member_id` column follows, issue_d has a 2-digit year ('Jun-17'), and there are no fico columns.
    style='classic' uses an 'id' header and 4-digit years ('Jun-2017') like the older Kaggle files."""
    df = generate(**kw)
    if style == "sample":
        df = df.drop(columns=["fico_range_low", "fico_range_high"])
        df["issue_d"] = df["issue_d"].map(_short_year)
        df.insert(1, "member_id", "")
        df["funded_amnt"] = df["loan_amnt"]
        df.to_csv(path, index=False, header=[""] + list(df.columns[1:]))
    elif style == "classic":
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"unknown style {style!r}")
    # Real LendingClub CSVs end with free-text footer lines; clean() must tolerate them.
    with open(path, "a", encoding="utf-8", newline="") as f:
        f.write('\nTotal amount funded in policy code 1: 123456789\n')
    return path
