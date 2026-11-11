"""Deterministic feature matrix. Encodings use FIXED vocabularies, never vocabularies learned from the
data, so the same rows always produce the same numbers regardless of what else is in the table."""
from __future__ import annotations

import hashlib

import numpy as np
import pandas as pd

VOCAB = {
    "grade": list("ABCDEFG"),
    "home_ownership": ["RENT", "MORTGAGE", "OWN", "OTHER", "ANY", "NONE"],
    "verification_status": ["Not Verified", "Source Verified", "Verified"],
    "purpose": ["debt_consolidation", "credit_card", "home_improvement", "major_purchase", "small_business",
                "car", "medical", "moving", "vacation", "house", "wedding", "renewable_energy", "educational", "other"],
}


def build_matrix(pdf: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    """Return float64 DataFrame with columns in `features` order. Unknown categories -> NaN."""
    out = {}
    for f in features:
        col = pdf[f]
        if f in VOCAB:
            s = col.astype("string").str.strip()
            s = s.where(s.isin(VOCAB[f])).astype(object)                      # unknown -> missing
            codes = pd.Categorical(s, categories=VOCAB[f]).codes.astype("float64")
            codes[codes < 0] = np.nan
            out[f] = codes
        else:
            out[f] = pd.to_numeric(col, errors="coerce").astype("float64").to_numpy()
    return pd.DataFrame(out, index=pdf.index)[features]


def array_sha256(a: np.ndarray) -> str:
    return hashlib.sha256(np.ascontiguousarray(np.asarray(a, dtype="float64")).tobytes()).hexdigest()
