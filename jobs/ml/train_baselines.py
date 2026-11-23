"""Model 0 (Persistence) và Model 1 (Seasonal Naive) trên mọi fold walk-forward + final test.

  python jobs/ml/train_baselines.py --data <export_dir>/features_v1 --baseline <export_dir>/baseline_v1 --out runs/v1

Tập đánh giá CHUNG cho mọi mô hình (eval_frame): target có giá trị VÀ có tốc độ hợp lệ trong 60' trước data_cutoff
(speed_last_known_60) - không có quan sát gần đây thì persistence không xác định. Số dòng bị loại được thống kê.
  - Persistence: tốc độ hợp lệ gần nhất trong 60' (speed_last_known_60).
  - Seasonal naive: tốc độ cùng cửa sổ target tuần trước; thiếu -> dùng persistence (độ phủ thuần được báo cáo riêng).
"""
import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from ml.feature_sets import add_fold_features, load_features
from ml.metrics import mae, rmse
from ml.mlconfig import load_config
from ml.splits import eval_mask, final_fold, walk_forward_folds

FRAME_COLS = ["link_id", "origin_utc", "target_window_start_utc", "borough", "local_hour", "is_peak", "is_weekend",
              "eval_precip_target_mm", "free_flow_speed", "speed_last_known_60", "speed_same_target_last_week"]


def eval_frame(df: pd.DataFrame, fold, baseline: pd.DataFrame, target="target_speed_15m") -> tuple[pd.DataFrame, dict]:
    m = eval_mask(df, fold)
    sub = df.loc[m]
    stats = {"rows_in_period": int(m.sum()), "no_target": int(sub[target].isna().sum()),
             "no_recent_data": int((sub[target].notna() & sub["speed_last_known_60"].isna()).sum())}
    sub = sub[sub[target].notna() & sub["speed_last_known_60"].notna()]
    sub = add_fold_features(sub.copy(), baseline, fold.boundary)        # free-flow as_of = ranh giới fold
    fr = sub[FRAME_COLS].copy()
    fr["y"] = sub[target].astype("float64")
    stats["eval_rows"] = int(len(fr))
    return fr.reset_index(drop=True), stats


def baseline_preds(fr: pd.DataFrame) -> pd.DataFrame:
    seasonal = fr["speed_same_target_last_week"]
    return pd.DataFrame({"link_id": fr["link_id"], "origin_utc": fr["origin_utc"],
                         "persistence": fr["speed_last_known_60"].astype("float64"),
                         "seasonal_naive": seasonal.fillna(fr["speed_last_known_60"]).astype("float64"),
                         "seasonal_available": seasonal.notna()})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    cfg = load_config()
    out = Path(args.out)
    (out / "frames").mkdir(parents=True, exist_ok=True)
    (out / "preds").mkdir(parents=True, exist_ok=True)
    baseline = pd.read_parquet(args.baseline)
    cols = sorted(set(FRAME_COLS + ["target_speed_15m", "target_window_end_utc", "roll_mean_60"]) - {"free_flow_speed"})
    df = load_features(args.data, cfg["split"]["dev_start"], cfg["split"]["test_end"], columns=cols)
    summary = {}
    for fold in walk_forward_folds(cfg["split"]) + [final_fold(cfg["split"])]:
        t0 = time.time()
        fr, st = eval_frame(df, fold, baseline)
        bp = baseline_preds(fr)
        fr.to_parquet(out / "frames" / f"{fold.name}.parquet", index=False)
        bp.to_parquet(out / "preds" / f"{fold.name}__baselines.parquet", index=False)
        summary[fold.name] = {**st, "persistence": {"mae": mae(fr.y, bp.persistence), "rmse": rmse(fr.y, bp.persistence)},
                              "seasonal_naive": {"mae": mae(fr.y, bp.seasonal_naive), "rmse": rmse(fr.y, bp.seasonal_naive),
                                                 "pure_coverage": float(bp.seasonal_available.mean()),
                                                 "mae_pure_rows": mae(fr.y[bp.seasonal_available], bp.seasonal_naive[bp.seasonal_available])},
                              "seconds": round(time.time() - t0, 1)}
        print(fold.name, json.dumps(summary[fold.name]), flush=True)
    (out / "baselines_summary.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    main()
