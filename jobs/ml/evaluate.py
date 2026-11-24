"""Tổng hợp kết quả thực nghiệm từ thư mục run (frames/, preds/, lgbm_results.json, baselines_summary.json) -> reports/ml/.

  python jobs/ml/evaluate.py --run runs/v1 --out reports/ml

Sinh: metrics.json, các bảng CSV, biểu đồ PNG (actual vs predicted, MAE theo giờ / borough / trạng thái, theo fold,
feature importance), confusion matrix trạng thái suy ra từ tốc độ dự báo. Không tạo số liệu nào không có trong file kết quả.
"""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from ml.metrics import STATES, classification_report, grouped_mae, mae, rmse, traffic_state  # noqa: E402

MODELS = ["persistence", "seasonal_naive", "lightgbm"]
LABEL = {"persistence": "Persistence", "seasonal_naive": "Seasonal naive", "lightgbm": "LightGBM"}
COLOR = {"persistence": "#9aa0a6", "seasonal_naive": "#e8a33d", "lightgbm": "#2f6fdf"}


def load_fold(run: Path, fold: str) -> pd.DataFrame:
    fr = pd.read_parquet(run / "frames" / f"{fold}.parquet")
    for p in sorted((run / "preds").glob(f"{fold}__*.parquet")):
        pr = pd.read_parquet(p)
        cols = [c for c in pr.columns if c not in ("link_id", "origin_utc")]
        fr = fr.merge(pr[["link_id", "origin_utc", *cols]], on=["link_id", "origin_utc"], how="left", validate="1:1")
    fr["origin_utc"] = pd.to_datetime(fr["origin_utc"], utc=True)
    fr["is_rain"] = np.where(fr["eval_precip_target_mm"].isna(), "unknown",
                             np.where(fr["eval_precip_target_mm"] >= 0.1, "rain", "dry"))
    fr["actual_state"] = traffic_state(fr["y"], fr["free_flow_speed"])
    fr["peak"] = np.where(fr["is_peak"] == 1, "peak", "off_peak")
    return fr


def fold_metrics(fr: pd.DataFrame, models) -> dict:
    out = {}
    for m in models:
        if m not in fr or fr[m].isna().all():
            continue
        d = fr[fr[m].notna()]
        pstate = traffic_state(d[m], d["free_flow_speed"])
        cls = classification_report(d["actual_state"], pstate)
        out[m] = {"n": int(len(d)), "mae": mae(d.y, d[m]), "rmse": rmse(d.y, d[m]),
                  "mae_by_borough": grouped_mae(d, "borough", p=m).set_index("borough")["mae"].round(4).to_dict(),
                  "mae_by_peak": grouped_mae(d, "peak", p=m).set_index("peak")["mae"].round(4).to_dict(),
                  "mae_by_rain": grouped_mae(d, "is_rain", p=m).set_index("is_rain")["mae"].round(4).to_dict(),
                  "mae_by_actual_state": grouped_mae(d, "actual_state", p=m).set_index("actual_state")["mae"].round(4).to_dict(),
                  "state_classification": cls}
    return out


def link_data_volume(fr: pd.DataFrame) -> pd.Series:
    n = fr.groupby("link_id").size()
    return pd.qcut(n, 3, labels=["low_data", "mid_data", "high_data"]).rename("data_group")


def plot_bar(ax, labels, series: dict, title, ylabel="MAE (mph)"):
    x = np.arange(len(labels))
    wdt = 0.8 / max(1, len(series))
    for i, (m, vals) in enumerate(series.items()):
        ax.bar(x + i * wdt - 0.4 + wdt / 2, vals, wdt, label=LABEL.get(m, m), color=COLOR.get(m))
    ax.set_xticks(x, labels, rotation=0)
    ax.set_title(title, loc="left", fontsize=11)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(frameon=False, fontsize=8)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    run, out = Path(args.run), Path(args.out)
    (out / "figures").mkdir(parents=True, exist_ok=True)
    folds = sorted(p.stem for p in (run / "frames").glob("*.parquet"))
    wf = [f for f in folds if f.startswith("expanding_")]
    res = {"folds": {}, "walk_forward_summary": {}}
    per_fold_rows = []
    for f in folds:
        fr = load_fold(run, f)
        models = MODELS + (["lightgbm_sliding"] if "lightgbm_sliding" in fr else [])
        res["folds"][f] = fold_metrics(fr, models)
        for m, v in res["folds"][f].items():
            per_fold_rows.append({"fold": f, "model": m, "n": v["n"], "mae": v["mae"], "rmse": v["rmse"],
                                  "macro_f1": v["state_classification"]["macro_f1"]})
    pf = pd.DataFrame(per_fold_rows)
    pf.to_csv(out / "metrics_by_fold.csv", index=False)
    wfp = pf[pf.fold.isin(wf)]
    for m, g in wfp.groupby("model"):
        res["walk_forward_summary"][m] = {"mae_mean": g.mae.mean(), "mae_std": g.mae.std(ddof=1), "rmse_mean": g.rmse.mean(),
                                          "macro_f1_mean": g.macro_f1.mean(), "folds": len(g)}

    # biểu đồ theo fold
    fig, ax = plt.subplots(figsize=(9, 4))
    for m in [x for x in MODELS + ["lightgbm_sliding"] if x in set(wfp.model)]:
        g = wfp[wfp.model == m].sort_values("fold")
        ax.plot([s.replace("expanding_", "") for s in g.fold], g.mae, marker="o", label=LABEL.get(m, "LightGBM (sliding 90 ngày)"),
                color=COLOR.get(m, "#7b4fd6"))
    ax.set_title("MAE theo fold walk-forward (tháng validation)", loc="left")
    ax.set_ylabel("MAE (mph)"); ax.grid(alpha=0.3); ax.legend(frameon=False)
    fig.tight_layout(); fig.savefig(out / "figures" / "mae_by_fold.png", dpi=130); plt.close(fig)

    # phân tích sâu trên final test (nếu có) - nếu chưa có thì fold dev cuối
    focus = "final_test" if "final_test" in folds else wf[-1]
    fr = load_fold(run, focus)
    res["focus_fold"] = focus
    models = [m for m in MODELS if m in fr]
    dg = link_data_volume(fr)
    fr = fr.merge(dg, left_on="link_id", right_index=True)
    res["focus_extra"] = {m: {"mae_by_data_group": grouped_mae(fr, "data_group", p=m).set_index("data_group")["mae"].round(4).to_dict()}
                          for m in models}
    link_tbl = pd.concat([grouped_mae(fr, ["link_id", "borough"], p=m).assign(model=m) for m in models])
    link_tbl.to_csv(out / f"mae_by_link_{focus}.csv", index=False)
    res["focus_extra"]["link_mae_distribution"] = {m: link_tbl[link_tbl.model == m]["mae"].describe().round(3).to_dict() for m in models}

    hr = {m: grouped_mae(fr, "local_hour", p=m).set_index("local_hour")["mae"] for m in models}
    fig, ax = plt.subplots(figsize=(9, 4))
    for m in models:
        ax.plot(hr[m].index, hr[m].values, marker=".", label=LABEL[m], color=COLOR[m])
    ax.set_title(f"MAE theo giờ (giờ New York của cửa sổ target) - {focus}", loc="left")
    ax.set_xlabel("giờ"); ax.set_ylabel("MAE (mph)"); ax.grid(alpha=0.3); ax.legend(frameon=False)
    fig.tight_layout(); fig.savefig(out / "figures" / "mae_by_hour.png", dpi=130); plt.close(fig)

    for key, title, fname in [("borough", "MAE theo borough", "mae_by_borough.png"),
                              ("actual_state", "MAE theo trạng thái thực tế", "mae_by_state.png"),
                              ("is_rain", "MAE giờ mưa / khô", "mae_by_rain.png"),
                              ("peak", "MAE cao điểm / thấp điểm", "mae_by_peak.png")]:
        tb = {m: grouped_mae(fr, key, p=m).set_index(key)["mae"] for m in models}
        labels = [l for l in (STATES if key == "actual_state" else tb[models[0]].index) if l in tb[models[0]].index]
        fig, ax = plt.subplots(figsize=(8, 3.8))
        plot_bar(ax, labels, {m: [tb[m].get(l, np.nan) for l in labels] for m in models}, f"{title} - {focus}")
        fig.tight_layout(); fig.savefig(out / "figures" / fname, dpi=130); plt.close(fig)

    if "lightgbm" in fr:
        s = fr.sample(n=min(20000, len(fr)), random_state=1)
        fig, ax = plt.subplots(figsize=(5, 5))
        ax.hexbin(s.y, s.lightgbm, gridsize=60, cmap="Blues", mincnt=1, bins="log")
        lim = [0, max(s.y.max(), s.lightgbm.max())]
        ax.plot(lim, lim, color="#d33", lw=1)
        ax.set_xlabel("tốc độ thực tế (mph)"); ax.set_ylabel("tốc độ dự báo (mph)")
        ax.set_title(f"Actual vs Predicted - LightGBM - {focus}", loc="left", fontsize=10)
        fig.tight_layout(); fig.savefig(out / "figures" / "actual_vs_predicted.png", dpi=130); plt.close(fig)
        # chuỗi thời gian một link (link có nhiều dữ liệu nhất), 2 ngày đầu
        lk = fr.groupby("link_id").size().idxmax()
        d = fr[fr.link_id == lk].sort_values("origin_utc")
        d = d[d.origin_utc < d.origin_utc.min() + pd.Timedelta(days=2)]
        fig, ax = plt.subplots(figsize=(10, 3.6))
        ax.plot(d.origin_utc, d.y, color="#222", lw=1, label="thực tế")
        for m in models:
            ax.plot(d.origin_utc, d[m], lw=1, label=LABEL[m], color=COLOR[m], alpha=0.9)
        ax.set_title(f"Chuỗi dự báo 15' - link {lk} - 2 ngày đầu {focus}", loc="left", fontsize=10)
        ax.set_ylabel("mph"); ax.legend(frameon=False, ncol=4, fontsize=8); ax.grid(alpha=0.3)
        fig.tight_layout(); fig.savefig(out / "figures" / "timeseries_example.png", dpi=130); plt.close(fig)
        cm = np.array(res["folds"][focus]["lightgbm"]["state_classification"]["confusion"]["matrix"])
        fig, ax = plt.subplots(figsize=(4.6, 4))
        ax.imshow(cm / cm.sum(axis=1, keepdims=True), cmap="Blues", vmin=0, vmax=1)
        for i in range(3):
            for j in range(3):
                ax.text(j, i, f"{cm[i, j]:,}\n{cm[i, j] / max(1, cm[i].sum()):.0%}", ha="center", va="center", fontsize=8)
        ax.set_xticks(range(3), STATES, fontsize=8); ax.set_yticks(range(3), STATES, fontsize=8)
        ax.set_xlabel("dự báo"); ax.set_ylabel("thực tế"); ax.set_title(f"Trạng thái - LightGBM - {focus}", loc="left", fontsize=10)
        fig.tight_layout(); fig.savefig(out / "figures" / "confusion_lightgbm.png", dpi=130); plt.close(fig)

    imps = sorted(run.glob("importance_expanding_*.csv"))
    if imps:
        imp = pd.concat([pd.read_csv(p) for p in imps]).groupby("feature")["gain"].mean().sort_values()
        imp = imp / imp.sum()
        imp.tail(25).to_csv(out / "feature_importance_top25.csv")
        fig, ax = plt.subplots(figsize=(7, 7))
        ax.barh(imp.tail(25).index, imp.tail(25).values, color="#2f6fdf")
        ax.set_title("Feature importance (gain, trung bình 5 fold)", loc="left", fontsize=10)
        ax.set_xlabel("tỷ lệ gain")
        fig.tight_layout(); fig.savefig(out / "figures" / "feature_importance.png", dpi=130); plt.close(fig)

    for name in ("lgbm_results.json", "baselines_summary.json", "tuning.json"):
        if (run / name).exists():
            res[name.replace(".json", "")] = json.loads((run / name).read_text())
    (out / "metrics.json").write_text(json.dumps(res, indent=1, default=float))
    print(json.dumps(res["walk_forward_summary"], indent=1))
    if "final_test" in res["folds"]:
        print(json.dumps({m: {k: v[k] for k in ("n", "mae", "rmse")} for m, v in res["folds"]["final_test"].items()}, indent=1))


if __name__ == "__main__":
    main()
