"""Training logic without Iceberg: the determinism the exact-retrain guarantee rests on."""
import json

import pytest
from pyspark.sql import functions as F

from ttml.config import names_sha256
from ttml.features import VOCAB, build_matrix
from ttml.ingest import load_clean_frame
from ttml.retrain import settings_from_row
from ttml.train import KEEP_COLS, merged_settings, prepare_frame, train_from_frame

OV = {"train_end": "2015-04-30", "params": {"n_estimators": 30}}


@pytest.fixture(scope="module")
def frame(spark, cfg, tmp_path_factory):
    from ttml.data_gen import write_csv
    p = tmp_path_factory.mktemp("d") / "x.csv"
    write_csv(str(p), start="2015-01", end="2015-06", rows_per_month=500, seed=3)
    return load_clean_frame(spark, cfg, str(p)).drop("issue_month").cache()


def test_deterministic_training_same_data_any_row_order(frame, cfg):
    s = merged_settings(cfg, OV)
    a = train_from_frame(frame, s)
    b = train_from_frame(frame.orderBy(F.rand(99)).repartition(7), s)       # shuffled + repartitioned input
    assert a["fingerprint"] == b["fingerprint"]
    assert a["metrics"]["x_train_sha256"] == b["metrics"]["x_train_sha256"]
    assert a["metrics"]["test_pred_sha256"] == b["metrics"]["test_pred_sha256"]
    assert a["model_sha256"] == b["model_sha256"]


def test_deterministic_training_model_learns_something(frame, cfg):
    m = train_from_frame(frame, merged_settings(cfg, OV))["metrics"]
    assert m["auc"] > 0.55 and m["rows_train"] > 0 and m["rows_test"] > 0


def test_deterministic_training_detects_different_data(frame, cfg):
    s = merged_settings(cfg, OV)
    a = train_from_frame(frame, s)
    fewer = frame.filter(F.pmod(F.col("loan_id"), F.lit(10)) != 0)
    b = train_from_frame(fewer, s)
    assert a["fingerprint"] != b["fingerprint"] and a["model_sha256"] != b["model_sha256"]


def test_deterministic_training_params_matter(frame, cfg):
    a = train_from_frame(frame, merged_settings(cfg, OV))
    b = train_from_frame(frame, merged_settings(cfg, {**OV, "params": {"n_estimators": 31}}))
    assert a["model_sha256"] != b["model_sha256"]


def test_split_that_leaves_no_test_rows_raises(frame, cfg):
    with pytest.raises(ValueError, match="adjust the split"):
        train_from_frame(frame, merged_settings(cfg, {**OV, "train_end": "2099-01-01"}))


def test_training_frame_contains_only_allowlisted_columns(frame, cfg):
    f = prepare_frame(frame, cfg.features())
    assert f.columns == [*KEEP_COLS, *cfg.features()]
    assert "total_pymnt" not in f.columns and "recoveries" not in f.columns


def test_feature_encoding_is_data_independent():
    import pandas as pd
    a = build_matrix(pd.DataFrame({"grade": ["B", "A"], "purpose": ["car", "weird"]}), ["grade", "purpose"])
    b = build_matrix(pd.DataFrame({"grade": ["A", "B", "G"], "purpose": ["car", "car", "car"]}), ["grade", "purpose"])
    assert a.loc[0, "grade"] == VOCAB["grade"].index("B") and b.loc[0, "grade"] == 0.0
    assert a["purpose"].isna().tolist() == [False, True]                       # unknown category -> NaN, not a new code


def test_settings_from_registry_row_roundtrip_and_consistency(cfg):
    feats = cfg.features()
    row = {"params_json": json.dumps({"lgbm": {"n_estimators": 5}, "features": feats, "train_end": "2015-04-30", "seed": 7}),
           "feature_list_sha256": names_sha256(feats), "data_filter": "1 = 1"}
    s = settings_from_row(row)
    assert s["features"] == feats and s["seed"] == 7 and s["params"] == {"n_estimators": 5}
    row["feature_list_sha256"] = "tampered"
    with pytest.raises(ValueError, match="inconsistent"):
        settings_from_row(row)
