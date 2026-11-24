"""Model 2 - LightGBM hồi quy tốc độ 15'. Chạy trên máy train (CPU), đọc Parquet export của build_dataset.py.

  python jobs/ml/train_lightgbm.py --data .../features_v1 --baseline .../baseline_v1 --out runs/v1 \
      --experiments tune,walk_forward,sliding,ablation,weighting,spatial,weather_lag,avail_lag,final

Nguyên tắc (docs/ml/03_validation_and_leakage.md):
  - Chỉ chia theo thời gian; purge mẫu train có nhãn chưa quan sát được tại ranh giới (splits.train_mask).
  - Early stopping trên 7 ngày CUỐI của tập train (purge), không phải trên tập đánh giá.
  - Tuning chỉ trên `tuning_folds` fold dev cuối; final test chỉ chạy một lần với cấu hình đã chọn trước đó.
  - Free-flow và tỷ lệ tốc độ/free-flow lấy từ baseline as_of = ranh giới của fold.
"""
import argparse
import itertools
import json
import platform
import time
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from ml.feature_sets import (CATEGORICAL, GROUPS, LOAD_COLS, SPATIAL, TEMPORAL, TRAFFIC, add_static_codes,
                             build_matrix, calendar_pandas, ff_map, load_features, weather_set)
from ml.metrics import mae, rmse, traffic_state
from ml.mlconfig import load_config
from ml.splits import (eval_mask, final_fold, inner_early_stopping_split, spatial_holdout_links, train_mask,
                       walk_forward_folds)
from ml.train_baselines import eval_frame

ES_DAYS = 7


class Ctx:
    def __init__(self, cfg, df, baseline, out, link_index):
        self.cfg, self.df, self.baseline, self.out, self.link_index = cfg, df, baseline, out, link_index
        p = cfg["problem"]
        self.H, self.A = p["horizon_minutes"], p["availability_lag_minutes"]
        self.log = []


def lgb_params(cfg, **over):
    c = cfg["lightgbm"]
    p = {"objective": c["objective"], "learning_rate": c["learning_rate"], "num_leaves": c["num_leaves"],
         "min_child_samples": c["min_child_samples"], "feature_fraction": c["feature_fraction"],
         "bagging_fraction": c["bagging_fraction"], "bagging_freq": c["bagging_freq"], "lambda_l2": c["lambda_l2"],
         "max_bin": c["max_bin"], "num_threads": c["num_threads"], "seed": cfg["seed"], "deterministic": True,
         "force_row_wise": True, "verbose": -1, "metric": "l1"}
    p.update(over)
    return p


class TrainSet:
    """Chỉ số dòng train (fit / early-stopping) trên ctx.df; ma trận feature dựng thẳng bằng numpy float32 từng cột
    (pandas .loc trên ~6,5 triệu dòng tạo bản sao trung gian ~3 GB, vượt RAM máy train)."""

    def __init__(self, ctx, fold, stride=1, target="target_speed_15m", avail=None, links=None):
        avail = ctx.A if avail is None else avail
        df = ctx.df
        m = train_mask(df, fold, ctx.H, avail) & df[target].notna()
        if stride > 1:
            m &= (df["origin_utc"].dt.minute % (5 * stride) == 0)
        if links is not None:
            m &= df["link_id"].isin(links)
        idx = np.flatnonzero(m.values)
        sub = pd.DataFrame({"origin_utc": df["origin_utc"].values[idx], "target_window_end_utc": df["target_window_end_utc"].values[idx]})
        m_fit, m_es = inner_early_stopping_split(sub, ES_DAYS, ctx.H, avail)   # 7 ngày cuối, purge
        self.idx_fit, self.idx_es = idx[m_fit.values], idx[m_es.values]
        self.ctx, self.target, self.ff_map = ctx, target, ff_map(ctx.baseline, fold.boundary)

    def X(self, idx, feats):
        return build_matrix(self.ctx.df, idx, feats, self.ff_map)

    def y(self, idx):
        return self.ctx.df[self.target].to_numpy(np.float32)[idx]


def fit(ctx, ts: TrainSet, feats: list[str], params: dict, weight=None):
    """Train với early stopping trên ES_DAYS ngày cuối của train (purge). weight: theo thứ tự ts.idx_fit."""
    cats = [c for c in CATEGORICAL if c in feats]
    ds_params = {k: params[k] for k in ("max_bin", "min_child_samples", "seed", "num_threads", "verbose") if k in params}
    ds_params["min_data_in_leaf"] = ds_params.pop("min_child_samples", 20)
    dfit = lgb.Dataset(ts.X(ts.idx_fit, feats), ts.y(ts.idx_fit), weight=weight, feature_name=feats,
                       categorical_feature=cats, params=ds_params, free_raw_data=True).construct()
    des = lgb.Dataset(ts.X(ts.idx_es, feats), ts.y(ts.idx_es), feature_name=feats, categorical_feature=cats,
                      reference=dfit, params=ds_params, free_raw_data=True).construct()
    t0 = time.time()
    booster = lgb.train(params, dfit, num_boost_round=ctx.cfg["lightgbm"]["n_estimators"], valid_sets=[des],
                        callbacks=[lgb.early_stopping(ctx.cfg["lightgbm"]["early_stopping_rounds"], verbose=False)])
    info = {"best_iteration": int(booster.best_iteration), "es_mae": float(booster.best_score["valid_0"]["l1"]),
            "n_fit": int(len(ts.idx_fit)), "n_es": int(len(ts.idx_es)), "train_seconds": round(time.time() - t0, 1),
            "features": feats, "params": {k: v for k, v in params.items() if k != "verbose"}}
    del dfit, des
    return booster, info


def pred_of(b, X: "EvalSet", feats):
    return b.predict(X.matrix(feats), num_iteration=b.best_iteration)


def train_frame(ctx, fold, stride=1, target="target_speed_15m", avail=None, links=None) -> TrainSet:
    return TrainSet(ctx, fold, stride, target, avail, links)


class EvalSet:
    """Dòng đánh giá (cùng thứ tự với eval_frame) + ma trận feature dựng theo yêu cầu."""

    def __init__(self, ctx, fold, target):
        m = eval_mask(ctx.df, fold) & ctx.df[target].notna() & ctx.df["speed_last_known_60"].notna()
        self.idx = np.flatnonzero(m.values)
        self.df, self.ffm, self.override = ctx.df, ff_map(ctx.baseline, fold.boundary), {}

    def matrix(self, feats):
        X = build_matrix(self.df, self.idx, feats, self.ffm)
        for c, v in self.override.items():
            if c in feats:
                X[:, feats.index(c)] = v
        return X

    @property
    def link_id(self):
        return self.df["link_id"].to_numpy()[self.idx]


def eval_set(ctx, fold, target="target_speed_15m"):
    fr, st = eval_frame(ctx.df, fold, ctx.baseline, target)
    X = EvalSet(ctx, fold, target)
    assert len(X.idx) == len(fr) and (ctx.df["origin_utc"].to_numpy()[X.idx] == fr["origin_utc"].to_numpy()).all()
    return X, fr, st


def save_preds(ctx, fold_name, model_name, fr, pred):
    d = ctx.out / "preds"
    d.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"link_id": fr["link_id"], "origin_utc": fr["origin_utc"], model_name: pred.astype("float64")}) \
        .to_parquet(d / f"{fold_name}__{model_name}.parquet", index=False)


def record(ctx, exp, fold, model, fr, pred, info=None, extra=None):
    row = {"experiment": exp, "fold": fold, "model": model, "n": int(len(fr)), "mae": mae(fr.y, pred), "rmse": rmse(fr.y, pred),
           **(extra or {})}
    if info:
        row.update({k: info[k] for k in ("best_iteration", "es_mae", "n_fit", "train_seconds")})
    ctx.log.append(row)
    print(json.dumps(row), flush=True)
    (ctx.out / "lgbm_results.json").write_text(json.dumps(ctx.log, indent=1))


def best_params(ctx):
    p = ctx.out / "tuning.json"
    if p.exists():
        return json.loads(p.read_text())["best"]
    c = ctx.cfg["lightgbm"]
    return {"num_leaves": c["num_leaves"], "min_child_samples": c["min_child_samples"], "learning_rate": c["learning_rate"]}


ALL = GROUPS["all"]
WEATHER_EXTRA = [f"w{l}_{c}" for l in (0, 180) for c in ("temp_c", "rh_pct", "wind_kmh", "precip_1h_mm", "precip_3h_mm")]


def exp_tune(ctx):
    g = ctx.cfg["lightgbm"]["grid"]
    folds = walk_forward_folds(ctx.cfg["split"])[-ctx.cfg["split"]["tuning_folds"]:]
    stride = ctx.cfg["lightgbm"]["tuning_train_stride"]
    data = [(f, train_frame(ctx, f, stride), eval_set(ctx, f)) for f in folds]
    res = []
    for nl, mcs, lr in itertools.product(g["num_leaves"], g["min_child_samples"], g["learning_rate"]):
        over = {"num_leaves": nl, "min_child_samples": mcs, "learning_rate": lr}
        maes = []
        for f, tr, (X, fr, _) in data:
            b, info = fit(ctx, tr, ALL, lgb_params(ctx.cfg, **over))
            pred = pred_of(b, X, ALL)
            maes.append(mae(fr.y, pred))
            record(ctx, "tune", f.name, f"lgbm_nl{nl}_mcs{mcs}_lr{lr}", fr, pred, info)
        res.append({**over, "mean_mae": float(np.mean(maes)), "fold_mae": maes})
    best = min(res, key=lambda r: r["mean_mae"])
    (ctx.out / "tuning.json").write_text(json.dumps({"grid": res, "best": {k: best[k] for k in ("num_leaves", "min_child_samples", "learning_rate")},
                                                      "folds": [f.name for f in folds], "train_stride": stride}, indent=1))


def exp_walk_forward(ctx, mode="expanding"):
    bp = best_params(ctx)
    for f in walk_forward_folds(ctx.cfg["split"], mode):
        tr = train_frame(ctx, f)
        b, info = fit(ctx, tr, ALL, lgb_params(ctx.cfg, **bp))
        del tr
        X, fr, st = eval_set(ctx, f)
        pred = pred_of(b, X, ALL)
        name = "lightgbm" if mode == "expanding" else "lightgbm_sliding"
        save_preds(ctx, f.name if mode == "expanding" else f.name.replace("sliding", "expanding"), name, fr, pred)
        record(ctx, "walk_forward" if mode == "expanding" else "sliding", f.name, name, fr, pred, info)
        if mode == "expanding":
            imp = pd.DataFrame({"feature": b.feature_name(), "gain": b.feature_importance("gain"),
                                "split": b.feature_importance("split")})
            imp.to_csv(ctx.out / f"importance_{f.name}.csv", index=False)


def last_fold(ctx):
    return walk_forward_folds(ctx.cfg["split"])[-1]


def exp_ablation(ctx):
    f, bp, stride = last_fold(ctx), best_params(ctx), ctx.cfg["lightgbm"]["tuning_train_stride"]
    tr, (X, fr, _) = train_frame(ctx, f, stride), eval_set(ctx, f)
    for gname, feats in GROUPS.items():
        b, info = fit(ctx, tr, feats, lgb_params(ctx.cfg, **bp))
        pred = pred_of(b, X, feats)
        save_preds(ctx, f.name, f"ablation_{gname}", fr, pred)
        record(ctx, "ablation", f.name, gname, fr, pred, info)


def class_weights(tr: TrainSet, cfg):
    ff = pd.Series(tr.ctx.df["link_id"].to_numpy()[tr.idx_fit]).map(tr.ff_map).to_numpy(float)
    st = traffic_state(tr.y(tr.idx_fit), ff, cfg["state"]["congested_lt"], cfg["state"]["slow_lt"])
    s = pd.Series(st)
    known = s.isin(["FREE_FLOW", "SLOW", "CONGESTED"])
    cnt = s[known].value_counts()
    w = pd.Series(1.0, index=s.index)
    for c, n in cnt.items():
        w[s == c] = known.sum() / (len(cnt) * n)
    return w.clip(upper=10).values, cnt.to_dict()


def exp_weighting(ctx):
    f, bp, stride = last_fold(ctx), best_params(ctx), ctx.cfg["lightgbm"]["tuning_train_stride"]
    tr, (X, fr, _) = train_frame(ctx, f, stride), eval_set(ctx, f)
    w, cnt = class_weights(tr, ctx.cfg)
    for name, weight in [("unweighted", None), ("class_weighted", w)]:
        b, info = fit(ctx, tr, ALL, lgb_params(ctx.cfg, **bp), weight=weight)
        pred = pred_of(b, X, ALL)
        save_preds(ctx, f.name, f"weighting_{name}", fr, pred)
        record(ctx, "weighting", f.name, name, fr, pred, info, {"train_state_counts": cnt})


def exp_spatial(ctx):
    f, bp, stride = last_fold(ctx), best_params(ctx), ctx.cfg["lightgbm"]["tuning_train_stride"]
    links = ctx.df[["link_id", "borough"]].drop_duplicates()
    hold = spatial_holdout_links(links, 0.2, ctx.cfg["seed"])
    seen = set(links.link_id) - hold
    (ctx.out / "spatial_holdout_links.json").write_text(json.dumps(sorted(int(x) for x in hold)))
    tr = train_frame(ctx, f, stride, links=seen)
    X, fr, _ = eval_set(ctx, f)
    no_id = [c for c in ALL if c != "link_code"]                 # link chưa từng gặp: không dựa vào mã link
    for name, feats in [("with_link_id", ALL), ("no_link_id", no_id)]:
        b, info = fit(ctx, tr, feats, lgb_params(ctx.cfg, **bp))
        X.override = {"link_code": np.where(np.isin(X.link_id, list(hold)), -1,
                                            ctx.df["link_code"].to_numpy()[X.idx]).astype(np.float32)}   # link lạ -> -1 như inference
        pred = pred_of(b, X, feats)
        X.override = {}
        for grp, mask in [("seen_links", fr.link_id.isin(seen)), ("holdout_links", fr.link_id.isin(hold))]:
            record(ctx, "spatial", f.name, f"{name}__{grp}", fr[mask.values], pred[mask.values], info,
                   {"holdout_links": len(hold)})


def exp_weather_lag(ctx):
    f, bp, stride = last_fold(ctx), best_params(ctx), ctx.cfg["lightgbm"]["tuning_train_stride"]
    tr, (X, fr, _) = train_frame(ctx, f, stride), eval_set(ctx, f)
    base = TRAFFIC + TEMPORAL + SPATIAL
    runs = [("no_weather", base)] + [(f"weather_lag_{l}min", base + weather_set(l))
                                     for l in sorted([ctx.cfg["problem"]["weather_lag_minutes"]] + ctx.cfg["problem"]["weather_sensitivity_lags"])]
    for name, feats in runs:
        b, info = fit(ctx, tr, feats, lgb_params(ctx.cfg, **bp))
        pred = pred_of(b, X, feats)
        record(ctx, "weather_lag", f.name, name, fr, pred, info)


AVAIL_COLS = ["origin_utc", "target_window_start_utc", "target_window_end_utc", "target_speed_15m",
              "speed_same_target_last_week", "local_hour", "minute_of_day", "day_of_week", "is_weekend", "is_holiday",
              "is_peak", "tod_sin", "tod_cos", "dow_sin", "dow_cos"]


def shift_for_avail(df: pd.DataFrame, a: int, H: int) -> None:
    """Sửa TẠI CHỖ cho availability lag a (tiết kiệm RAM; exp_avail_lag sao lưu và khôi phục các cột này):
    feature giữ nguyên (gắn với cửa sổ có sẵn cuối L), gốc / target / lịch dịch theo a."""
    df["origin_utc"] = df["data_cutoff_utc"] + pd.Timedelta(minutes=a)
    df["target_window_start_utc"] = df["origin_utc"] + pd.Timedelta(minutes=H - 5)
    df["target_window_end_utc"] = df["origin_utc"] + pd.Timedelta(minutes=H)
    if a != 10:
        df["target_speed_15m"] = df[f"target_speed_15m_a{a}"]
        df["speed_same_target_last_week"] = df[f"speed_same_target_last_week_a{a}"]
    cal = calendar_pandas(df["target_window_start_utc"])
    for c in cal:
        df[c] = cal[c].values.astype(df[c].dtype)


def exp_avail_lag(ctx):
    f, bp, stride = last_fold(ctx), best_params(ctx), ctx.cfg["lightgbm"]["tuning_train_stride"]
    feats = [c for c in ALL if c != "speed_same_target_yesterday"]   # bản 'hôm qua' chỉ có cho A mặc định
    backup = {c: ctx.df[c].copy() for c in AVAIL_COLS}
    try:
        for a in sorted(ctx.cfg["problem"]["sensitivity_lags"] + [ctx.A]):
            shift_for_avail(ctx.df, a, ctx.H)
            tr = train_frame(ctx, f, stride, avail=a)
            X, fr, _ = eval_set(ctx, f)
            b, info = fit(ctx, tr, feats, lgb_params(ctx.cfg, **bp))
            del tr
            pred = pred_of(b, X, feats)
            record(ctx, "avail_lag", f.name, f"lightgbm_A{a}", fr, pred, info)
            record(ctx, "avail_lag", f.name, f"persistence_A{a}", fr, fr["speed_last_known_60"].values)
            record(ctx, "avail_lag", f.name, f"seasonal_naive_A{a}", fr,
                   fr["speed_same_target_last_week"].fillna(fr["speed_last_known_60"]).values)
            for c in AVAIL_COLS:
                ctx.df[c] = backup[c]
    finally:
        for c in AVAIL_COLS:
            ctx.df[c] = backup[c]


def exp_final(ctx):
    f, bp = final_fold(ctx.cfg["split"]), best_params(ctx)
    tr = train_frame(ctx, f)
    b, info = fit(ctx, tr, ALL, lgb_params(ctx.cfg, **bp))
    del tr
    X, fr, st = eval_set(ctx, f)
    pred = pred_of(b, X, ALL)
    save_preds(ctx, f.name, "lightgbm", fr, pred)
    record(ctx, "final", f.name, "lightgbm", fr, pred, info, st)
    mdir = ctx.out / "model"
    mdir.mkdir(exist_ok=True)
    version = f"lgbm-{ctx.cfg['version']}-{time.strftime('%Y%m%d%H%M%S')}"
    b.save_model(str(mdir / "model.txt"), num_iteration=b.best_iteration)
    dump = b.dump_model(num_iteration=b.best_iteration)
    (mdir / "model.json").write_text(json.dumps(dump))
    # fixture nhỏ để test bộ chấm điểm numpy khớp Booster.predict (chạy được cả khi máy không có lightgbm)
    rs = np.random.default_rng(0).choice(len(X.idx), size=min(2000, len(X.idx)), replace=False)
    fx = pd.DataFrame(X.matrix(ALL)[rs].astype("float64"), columns=ALL)
    fx["_pred"] = b.predict(fx[ALL].to_numpy(), num_iteration=b.best_iteration)
    fx.to_parquet(mdir / "fixture.parquet", index=False)
    meta = {"model_version": version, "features": ALL, "categorical": CATEGORICAL, "link_index": {str(k): v for k, v in ctx.link_index.items()},
            "baseline_as_of_utc": str(f.boundary), "train_start": str(f.train_start), "train_boundary": str(f.boundary),
            "horizon_minutes": ctx.H, "availability_lag_minutes": ctx.A,
            "weather_lag_minutes": ctx.cfg["problem"]["weather_lag_minutes"], "feature_version": ctx.cfg["version"],
            "config": ctx.cfg, "training": info, "lightgbm_version": lgb.__version__, "python": platform.python_version(),
            "host": platform.node(), "dataset_rows": int(len(ctx.df)), "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (mdir / "metadata.json").write_text(json.dumps(meta, indent=1, default=str))


EXPERIMENTS = {"tune": exp_tune, "walk_forward": exp_walk_forward, "sliding": lambda c: exp_walk_forward(c, "sliding"),
               "ablation": exp_ablation, "weighting": exp_weighting, "spatial": exp_spatial, "weather_lag": exp_weather_lag,
               "avail_lag": exp_avail_lag, "final": exp_final}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--experiments", default=",".join(EXPERIMENTS))
    ap.add_argument("--max-month", default=None, help="smoke test: chỉ nạp tới tháng này")
    ap.add_argument("--smoke", action="store_true", help="smoke test: 1 fold, 1 cấu hình, 30 cây")
    args = ap.parse_args()
    cfg = load_config()
    if args.smoke:
        cfg["lightgbm"]["n_estimators"] = 30
        cfg["split"]["val_months"] = cfg["split"]["val_months"][-1:]
        cfg["split"]["tuning_folds"] = 1
        cfg["lightgbm"]["grid"] = {k: v[:1] for k, v in cfg["lightgbm"]["grid"].items()}
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    exps = args.experiments.split(",")
    # tiết kiệm RAM: chỉ nạp tháng test khi chạy 'final'; chỉ nạp cột độ nhạy khi chạy thí nghiệm độ nhạy
    end = args.max_month or (cfg["split"]["test_end"] if "final" in exps else cfg["split"]["dev_end"])
    cols = [c for c in LOAD_COLS if not (c.startswith(("w0_", "w180_")) or c.endswith(("_a0", "_a5")))]
    if "weather_lag" in exps or "avail_lag" in exps:
        cols = LOAD_COLS
    cols = [c for c in cols if c not in ("last_window_start_utc",)]
    df = load_features(args.data, cfg["split"]["dev_start"], end, columns=cols, require_any_target=True)
    # dòng không có target nào đã bị bỏ lúc nạp (thống kê số dòng này nằm ở baselines_summary.json)
    link_index = {int(l): i for i, l in enumerate(sorted(df["link_id"].unique()))}
    df = add_static_codes(df, link_index)
    print(json.dumps({"loaded_rows": len(df), "mem_gb": round(df.memory_usage(deep=True).sum() / 1e9, 2),
                      "load_seconds": round(time.time() - t0, 1)}), flush=True)
    ctx = Ctx(cfg, df, pd.read_parquet(args.baseline), out, link_index)
    old = out / "lgbm_results.json"
    if old.exists():
        ctx.log = json.loads(old.read_text())
    for e in exps:
        t1 = time.time()
        ctx.log = [r for r in ctx.log if r["experiment"] != {"walk_forward": "walk_forward"}.get(e, e)]
        EXPERIMENTS[e](ctx)
        print(json.dumps({"experiment_done": e, "seconds": round(time.time() - t1, 1)}), flush=True)


if __name__ == "__main__":
    main()
