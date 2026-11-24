"""Integration test cho model đã train: serialization + bộ chấm điểm numpy (dùng lúc inference) khớp LightGBM.
Thư mục model: biến ML_MODEL_DIR, hoặc phiên bản đang phục vụ theo models/ml/current.json (bỏ qua test nếu chưa có model).
  python -m pytest -q jobs/tests/ml/test_ml_model_io.py
"""
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "jobs"))

from ml.lgbm_numpy import NumpyBooster, model_file  # noqa: E402

def _serving_model_dir() -> Path:
    cur = ROOT / "models" / "ml" / "current.json"
    if cur.exists():
        return ROOT / "models" / "ml" / json.loads(cur.read_text())["model_version"]
    return ROOT / "models" / "ml" / "current"


MODEL_DIR = Path(os.getenv("ML_MODEL_DIR") or _serving_model_dir())
pytestmark = pytest.mark.skipif(not model_file(MODEL_DIR).exists(), reason=f"chưa có model tại {MODEL_DIR}")


def test_metadata_complete():
    meta = json.loads((MODEL_DIR / "metadata.json").read_text())
    for k in ("model_version", "features", "categorical", "link_index", "baseline_as_of_utc", "train_boundary",
              "horizon_minutes", "availability_lag_minutes", "weather_lag_minutes", "feature_version", "config",
              "training", "lightgbm_version", "created_at"):
        assert k in meta, k
    assert meta["training"]["best_iteration"] > 0 and meta["training"]["train_seconds"] > 0
    assert meta["config"]["seed"] == 42
    assert pd.Timestamp(meta["train_boundary"]) == pd.Timestamp(meta["config"]["split"]["test_start"], tz="UTC")


def test_numpy_scorer_matches_lightgbm_fixture():
    fx = pd.read_parquet(MODEL_DIR / "fixture.parquet")
    nb = NumpyBooster.load(str(model_file(MODEL_DIR)))
    meta = json.loads((MODEL_DIR / "metadata.json").read_text())
    assert nb.feature_names == meta["features"]
    pred = nb.predict(fx[meta["features"]].values)
    assert np.max(np.abs(pred - fx["_pred"].values)) < 1e-6


def test_model_txt_roundtrip_if_lightgbm_available():
    lgb = pytest.importorskip("lightgbm")
    fx = pd.read_parquet(MODEL_DIR / "fixture.parquet")
    meta = json.loads((MODEL_DIR / "metadata.json").read_text())
    b = lgb.Booster(model_file=str(MODEL_DIR / "model.txt"))
    assert np.max(np.abs(b.predict(fx[meta["features"]]) - fx["_pred"].values)) < 1e-9


def test_gz_copy_matches_model_json():
    """Bản nén trong repo phải giống hệt model.json (chỉ kiểm khi có cả hai, tức trên máy đã train/phục vụ)."""
    import gzip
    plain, gz = MODEL_DIR / "model.json", MODEL_DIR / "model.json.gz"
    if not (plain.exists() and gz.exists()):
        pytest.skip("cần cả model.json và model.json.gz")
    assert gzip.decompress(gz.read_bytes()) == plain.read_bytes()


def test_numpy_scorer_handles_unseen_link_and_missing_values():
    nb = NumpyBooster.load(str(model_file(MODEL_DIR)))
    meta = json.loads((MODEL_DIR / "metadata.json").read_text())
    X = np.full((3, len(meta["features"])), np.nan)
    X[:, meta["features"].index("link_code")] = [-1, 0, 10_000]
    p = nb.predict(X)
    assert np.isfinite(p).all()
