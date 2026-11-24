"""Unit test (thuần Python) cho chia dữ liệu theo thời gian, purge, point-in-time của baseline, nhóm feature và cấu hình.
  python -m pytest -q jobs/tests/ml/test_ml_splits_leakage.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ml import feature_sets as fs  # noqa: E402
from ml.metrics import classification_report, mae, rmse, traffic_state  # noqa: E402
from ml.mlconfig import load_config, parse_yaml_subset  # noqa: E402
from ml.splits import (eval_mask, final_fold, inner_early_stopping_split, spatial_holdout_links, train_mask,  # noqa: E402
                       walk_forward_folds)

CFG = load_config()
H, A = CFG["problem"]["horizon_minutes"], CFG["problem"]["availability_lag_minutes"]


def frame(start="2025-01-01", end="2026-01-01", freq="5min"):
    o = pd.date_range(start, end, freq=freq, tz="UTC", inclusive="left")
    return pd.DataFrame({"origin_utc": o, "target_window_start_utc": o + pd.Timedelta(minutes=H - 5),
                         "target_window_end_utc": o + pd.Timedelta(minutes=H)})


# ------------------------------------------------------------------ cấu hình
def test_yaml_subset_parser_matches_pyyaml():
    yaml = pytest.importorskip("yaml")
    text = (Path(__file__).resolve().parents[3] / "conf" / "ml" / "forecast.yaml").read_text(encoding="utf-8")
    assert parse_yaml_subset(text) == yaml.safe_load(text)


# ------------------------------------------------------------------ temporal split + purge
def test_folds_are_ordered_and_inside_dev():
    folds = walk_forward_folds(CFG["split"])
    dev_end = pd.Timestamp(CFG["split"]["dev_end"], tz="UTC")
    assert [f.eval_start for f in folds] == sorted(f.eval_start for f in folds)
    for f in folds:
        assert f.train_start < f.boundary == f.eval_start < f.eval_end <= dev_end


def test_final_test_never_in_dev_folds():
    ft = final_fold(CFG["split"])
    for f in walk_forward_folds(CFG["split"]) + walk_forward_folds(CFG["split"], "sliding"):
        assert f.eval_end <= ft.eval_start


@pytest.mark.parametrize("mode", ["expanding", "sliding"])
def test_purge_no_train_label_inside_or_after_validation(mode):
    df = frame()
    for f in walk_forward_folds(CFG["split"], mode) + [final_fold(CFG["split"])]:
        tr, ev = df[train_mask(df, f, H, A)], df[eval_mask(df, f)]
        # nhãn train đã quan sát được (target_end + A) trước ranh giới -> không cửa sổ target nào chạm tập đánh giá
        assert (tr["target_window_end_utc"] + pd.Timedelta(minutes=A) <= f.boundary).all()
        assert (tr["target_window_end_utc"] <= ev["origin_utc"].min()).all()
        assert not set(tr["origin_utc"]) & set(ev["origin_utc"])
        # khoảng trống = đúng H + A: gốc train cuối = ranh giới - (H + A)
        assert tr["origin_utc"].max() == f.boundary - pd.Timedelta(minutes=H + A)
        assert (ev["target_window_end_utc"] <= f.eval_end).all()


def test_sliding_window_length():
    for f in walk_forward_folds(CFG["split"], "sliding"):
        assert f.boundary - f.train_start == pd.Timedelta(days=CFG["split"]["sliding_train_days"])


def test_inner_early_stopping_is_purged_and_after_fit():
    f = walk_forward_folds(CFG["split"])[0]
    df = frame()
    tr = df[train_mask(df, f, H, A)].reset_index(drop=True)
    m_fit, m_es = inner_early_stopping_split(tr, 7, H, A)
    b = tr.loc[m_es, "origin_utc"].min()
    assert (tr.loc[m_fit, "target_window_end_utc"] + pd.Timedelta(minutes=A) <= b).all()
    assert tr.loc[m_fit, "origin_utc"].max() < b and not (m_fit & m_es).any()


def test_spatial_holdout_is_stratified_and_disjoint():
    links = pd.DataFrame({"link_id": range(20), "borough": ["A"] * 10 + ["B"] * 10})
    hold = spatial_holdout_links(links, 0.2, 42)
    assert len(hold) == 4 and len(hold & set(range(10))) == 2
    assert hold == spatial_holdout_links(links, 0.2, 42)          # tái lập theo seed


# ------------------------------------------------------------------ point-in-time của free-flow
def test_fold_baseline_uses_only_matching_as_of():
    base = pd.DataFrame({"link_id": [1, 1, 2], "free_flow_speed": [40.0, 50.0, None],
                         "as_of_utc": pd.to_datetime(["2025-05-01", "2025-10-01", "2025-05-01"], utc=True)})
    df = pd.DataFrame({"link_id": [1, 2], "speed_last_known_60": [20.0, 30.0], "roll_mean_60": [20.0, 30.0],
                       "speed_same_target_last_week": [40.0, 30.0]})
    out = fs.add_fold_features(df.copy(), base, pd.Timestamp("2025-05-01", tz="UTC"))
    assert out["free_flow_speed"].tolist()[0] == 40.0 and np.isnan(out["free_flow_speed"].tolist()[1])
    assert out["ratio_last_ff"].iloc[0] == pytest.approx(0.5) and np.isnan(out["ratio_last_ff"].iloc[1])
    out10 = fs.add_fold_features(df.copy(), base, pd.Timestamp("2025-10-01", tz="UTC"))
    assert out10["free_flow_speed"].iloc[0] == 50.0


def test_baseline_as_of_dates_cover_every_fold_boundary():
    as_of = {pd.Timestamp(x, tz="UTC") for x in CFG["baseline"]["as_of_dates"]}
    for f in walk_forward_folds(CFG["split"]) + walk_forward_folds(CFG["split"], "sliding") + [final_fold(CFG["split"])]:
        assert f.boundary in as_of


def test_unseen_link_gets_negative_code():
    df = pd.DataFrame({"link_id": [10, 99], "borough": ["Queens", "Atlantis"]})
    out = fs.add_static_codes(df, {10: 0})
    assert out["link_code"].tolist() == [0, -1] and out["borough_code"].tolist() == [fs.BOROUGHS.index("Queens"), -1]


# ------------------------------------------------------------------ nhóm feature / leakage theo tên
def test_feature_groups_exclude_eval_only_and_targets():
    for name, feats in fs.GROUPS.items():
        assert not set(feats) & set(fs.EVAL_ONLY), name
        assert not [f for f in feats if f.startswith(("target_", "eval_"))], name
        assert len(feats) == len(set(feats)), name
    assert not [f for f in fs.GROUPS["all"] if "traffic_state" in f]       # không dùng trạng thái suy từ tương lai


def test_weather_sensitivity_sets():
    assert fs.weather_set(60) == ["w_" + c for c in fs.WEATHER_CORE]
    assert fs.weather_set(0)[0].startswith("w0_") and fs.weather_set(180)[0].startswith("w180_")


def test_calendar_pandas_handles_dst():
    ts = pd.Series(pd.to_datetime(["2025-03-09 06:55", "2025-03-09 07:05", "2025-11-02 05:30", "2025-11-02 06:30"], utc=True))
    cal = fs.calendar_pandas(ts)
    assert cal["local_hour"].tolist() == [1, 3, 1, 1]                   # 01:55 EST -> 03:05 EDT; 01:30 EDT và 01:30 EST
    assert cal["day_of_week"].tolist() == [6, 6, 6, 6]


# ------------------------------------------------------------------ metric
def test_metrics_and_state_rule():
    assert mae([1, 2], [2, 4]) == 1.5 and rmse([0, 0], [3, 4]) == pytest.approx(np.sqrt(12.5))
    st = traffic_state([10, 30, 45, np.nan, 20], [50, 50, 50, 50, np.nan])
    assert st.tolist() == ["CONGESTED", "SLOW", "FREE_FLOW", "NO_VALID_DATA", "NO_BASELINE"]
    rep = classification_report(["SLOW", "SLOW", "CONGESTED"], ["SLOW", "FREE_FLOW", "CONGESTED"])
    assert rep["per_class"]["SLOW"]["recall"] == 0.5 and rep["per_class"]["CONGESTED"]["f1"] == 1.0


def test_build_matrix_equals_add_fold_features():
    """Train (build_matrix, numpy) và inference (add_fold_features, pandas) phải cho cùng giá trị feature."""
    rng = np.random.default_rng(0)
    n = 50
    df = pd.DataFrame({"link_id": rng.choice([1, 2, 3], n), "speed_last_known_60": rng.uniform(5, 60, n).astype("float32"),
                       "roll_mean_60": rng.uniform(5, 60, n).astype("float32"),
                       "speed_same_target_last_week": np.where(rng.random(n) < 0.2, np.nan, rng.uniform(5, 60, n)).astype("float32"),
                       "speed_last": rng.uniform(5, 60, n).astype("float32")})
    base = pd.DataFrame({"link_id": [1, 2, 3], "free_flow_speed": [40.0, None, 0.0],
                         "as_of_utc": pd.to_datetime(["2025-10-01"] * 3, utc=True)})
    as_of = pd.Timestamp("2025-10-01", tz="UTC")
    feats = ["speed_last"] + fs.SPATIAL_FOLD
    a = fs.build_matrix(df, np.arange(n), feats, fs.ff_map(base, as_of))
    b = fs.add_fold_features(df.copy(), base, as_of)[feats].to_numpy(np.float32)
    assert np.allclose(a, b, equal_nan=True, rtol=1e-6)
