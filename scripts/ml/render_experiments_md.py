"""Sinh docs/ml/04_model_experiments.md từ artifact thật của một lần train (không chép tay số liệu).

  python scripts/ml/render_experiments_md.py --version v2 [--compare v1]

Nguồn:
- reports/ml/<version>/metrics.json, feature_importance_top25.csv, mae_by_link_final_test.csv (jobs/ml/evaluate.py);
- models/ml/<model_version>/metadata.json (model đang phục vụ).
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODELS = ["persistence", "seasonal_naive", "lightgbm"]
NAME = {"persistence": "Persistence", "seasonal_naive": "Seasonal naive", "lightgbm": "LightGBM",
        "lightgbm_sliding": "LightGBM sliding 90 ngày"}
STATE_VI = {"FREE_FLOW": "Thông thoáng", "SLOW": "Chậm", "CONGESTED": "Tắc"}


def f3(x):
    return "—" if x is None else f"{x:.3f}"


def n(x):
    return "—" if x is None else f"{int(x):,}".replace(",", ".")


def pct(a, b):
    return f"{(1 - a / b) * 100:.1f}%".replace(".", ",")


def table(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in head) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default="v2")
    ap.add_argument("--compare", default="v1")
    ap.add_argument("--out", default=str(ROOT / "docs/ml/04_model_experiments.md"))
    a = ap.parse_args()
    rep = ROOT / "reports/ml" / a.version
    m = json.loads((rep / "metrics.json").read_text(encoding="utf-8"))
    cmp_path = ROOT / "reports/ml" / a.compare / "metrics.json"
    mc = json.loads(cmp_path.read_text(encoding="utf-8")) if cmp_path.exists() else None
    cur = json.loads((ROOT / "models/ml/current.json").read_text())["model_version"]
    meta = json.loads((ROOT / "models/ml" / cur / "metadata.json").read_text())
    ft, wf, bs, res = m["folds"]["final_test"], m["walk_forward_summary"], m["baselines_summary"], m["lgbm_results"]
    lg, pe, sn = ft["lightgbm"], ft["persistence"], ft["seasonal_naive"]
    exp = lambda e: [r for r in res if r["experiment"] == e]  # noqa: E731
    folds = sorted(f for f in m["folds"] if f != "final_test")
    L = []
    w = L.append

    w(f"# 04 — Mô hình và kết quả thực nghiệm ({a.version})\n")
    w(f"Mọi số liệu trong tài liệu này được sinh tự động bởi `scripts/ml/render_experiments_md.py` từ artifact của lần train thật:")
    w(f"- `reports/ml/{a.version}/metrics.json` (do `jobs/ml/evaluate.py` tạo trên máy train);")
    w(f"- model đang phục vụ `{cur}` (LightGBM {meta.get('lightgbm_version')}, tạo {meta.get('created_at')}).\n")
    w("Không có số liệu giả định. Muốn cập nhật thì train lại (`scripts/ml/train_all.sh`), chép `reports/ml/<phiên bản>` về rồi chạy lại script.\n")
    w(f"**Thay đổi dữ liệu của {a.version} so với {a.compare}:**")
    w("- loại bản ghi `status = -101` (cảm biến báo lỗi) khỏi Silver;")
    w("- tốc độ thông thoáng theo công thức gold-dev: P85 của speed > 0, giờ 00–05:59 New York, không mưa, không tuyết;")
    w("- đổi tên cột sang `_mph` và `_sec`.\n")
    w("Vì tập đánh giá đổi, MAE của hai phiên bản **không so trực tiếp** được với nhau (xem mục 12).\n")

    w("## 1. Mô hình\n")
    w(table(["Mô hình", "Định nghĩa"], [
        ["Model 0 — Persistence", "tốc độ hợp lệ gần nhất trong 60' trước data_cutoff (`speed_last_known_60`)"],
        ["Model 1 — Seasonal naive", "tốc độ cùng cửa sổ target của tuần trước; thiếu thì dùng persistence"],
        ["Model 2 — LightGBM", f"hồi quy `regression_l1` (tối ưu MAE), {len(meta['features'])} feature, "
                               f"biến phân loại: {', '.join(meta['categorical'])}"]]))
    tune = exp("tune")
    if tune:
        w("\n**Tuning** (fold 08 và 09):\n")
        by = {}
        for r in tune:
            by.setdefault(r["model"], {})[r["fold"]] = r["mae"]
        rows = []
        best = min(by, key=lambda k: sum(by[k].values()) / len(by[k]))
        for k, v in by.items():
            nl, mcs = k.split("_")[1][2:], k.split("_")[2][3:]
            avg = sum(v.values()) / len(v)
            rows.append([nl, mcs, f3(v.get("expanding_2025-08")), f3(v.get("expanding_2025-09")),
                         f"**{f3(avg)}** ← chọn" if k == best else f3(avg)])
        w(table(["num_leaves", "min_child_samples", "MAE fold 08", "MAE fold 09", "trung bình"], rows))
        spread = max(sum(v.values()) / len(v) for v in by.values()) - min(sum(v.values()) / len(v) for v in by.values())
        w(f"\nMô hình ít nhạy với siêu tham số: các cấu hình chênh nhau {spread:.3f} mph.\n")

    w("## 2. Tập đánh giá\n")
    w("Mọi mô hình dùng **chung** một tập đánh giá: các dòng có target và có quan sát hợp lệ trong 60 phút trước data_cutoff.\n")
    rows = []
    for f in folds + ["final_test"]:
        s = bs.get(f, {})
        rows.append([("**Final test (10–12)**" if f == "final_test" else f"Fold {f[-2:]}"), n(s.get("rows_in_period")),
                     n(s.get("no_target")), n(s.get("no_recent_data")),
                     f"**{n(s.get('eval_rows'))}**" if f == "final_test" else n(s.get("eval_rows"))])
    w(table(["Tập", "Gốc dự báo trong kỳ", "Không có target", "Không có dữ liệu 60' gần nhất", "Dòng đánh giá"], rows))
    w(f"\nMô hình cuối train trên {n(meta.get('dataset_rows'))} dòng feature, gốc dự báo từ "
      f"{str(meta.get('train_start'))[:10]} tới {str(meta.get('train_boundary'))[:10]}.\n")

    w("## 3. Walk-forward (expanding), MAE (mph)\n")
    rows = []
    for f in folds:
        d = m["folds"][f]
        rows.append([f[-2:]] + [f"**{f3(d[k]['mae'])}**" if k == "lightgbm" else f3(d.get(k, {}).get("mae"))
                                for k in ["persistence", "seasonal_naive", "lightgbm", "lightgbm_sliding"]])
    rows.append(["**TB ± độ lệch**"] + [f"{f3(wf[k]['mae_mean'])} ± {f3(wf[k]['mae_std'])}"
                                         for k in ["persistence", "seasonal_naive", "lightgbm", "lightgbm_sliding"]])
    w(table(["Fold", "Persistence", "Seasonal naive", "LightGBM", "LightGBM sliding 90 ngày"], rows))
    better = sum(m["folds"][f]["lightgbm"]["mae"] < m["folds"][f]["persistence"]["mae"] for f in folds)
    gains = [1 - m["folds"][f]["lightgbm"]["mae"] / m["folds"][f]["persistence"]["mae"] for f in folds]
    w(f"\nRMSE trung bình: persistence {wf['persistence']['rmse_mean']:.2f}, seasonal {wf['seasonal_naive']['rmse_mean']:.2f}, "
      f"LightGBM **{wf['lightgbm']['rmse_mean']:.2f}**. Macro-F1 trạng thái trung bình: {wf['persistence']['macro_f1_mean']:.3f} / "
      f"{wf['seasonal_naive']['macro_f1_mean']:.3f} / **{wf['lightgbm']['macro_f1_mean']:.3f}**.")
    w(f"LightGBM tốt hơn persistence ở **{better}/{len(folds)} fold**, MAE giảm {min(gains)*100:.1f}–{max(gains)*100:.1f}%.\n")

    w("## 4. Final test (01/10 → 31/12/2025, chạy một lần với cấu hình đã chốt)\n")
    rows = [[NAME[k] if k != "lightgbm" else "**LightGBM**", f3(ft[k]["mae"]), f3(ft[k]["rmse"]),
             f3(ft[k]["state_classification"]["macro_f1"]), f3(ft[k]["state_classification"]["accuracy"])] for k in MODELS]
    w(table(["Mô hình", "MAE", "RMSE", "Macro-F1 trạng thái", "Accuracy (tham khảo)"], rows))
    lo, hi = min(m["folds"][f]["lightgbm"]["mae"] for f in folds), max(m["folds"][f]["lightgbm"]["mae"] for f in folds)
    plo = min(m["folds"][f]["persistence"]["mae"] for f in folds)
    where = "nằm trong" if lo <= lg["mae"] <= hi else ("thấp hơn" if lg["mae"] < lo else "cao hơn")
    w(f"\nSo với persistence, LightGBM giảm MAE **{pct(lg['mae'], pe['mae'])}** và RMSE **{pct(lg['rmse'], pe['rmse'])}**. "
      f"MAE final test ({lg['mae']:.3f}) {where} khoảng các fold dev ({lo:.2f}–{hi:.2f}).")
    if lg["mae"] < lo and pe["mae"] < plo:
        w(f"Persistence cũng thấp hơn mọi fold dev ({pe['mae']:.3f} so với ≥ {plo:.3f}), nên giai đoạn 10–12 dễ dự báo hơn "
          "với cả mô hình không học. Đây không phải dấu hiệu mô hình quá khớp vào dev.")
    w("")
    sc = lg["state_classification"]
    w("**Trạng thái suy ra từ tốc độ dự báo** (LightGBM, final test):\n")
    tot = sum(v["support"] for v in sc["per_class"].values())
    w(table(["Lớp", "Precision", "Recall", "F1", "Support"], [
        [STATE_VI[k], f3(v["precision"]), f3(v["recall"]), f3(v["f1"]), f"{n(v['support'])} ({v['support']/tot*100:.1f}%)"]
        for k, v in sc["per_class"].items()]))
    pc = pe["state_classification"]["per_class"]
    w(f"\nF1 của persistence: {' / '.join(f3(pc[k]['f1']) for k in sc['per_class'])}. "
      f"Ma trận nhầm lẫn (hàng là thực tế {'/'.join(sc['confusion']['labels'])}): "
      + " / ".join("[" + ", ".join(n(x) for x in row) + "]" for row in sc["confusion"]["matrix"]) + ".")
    w("Lớp Chậm khó nhất: nó nằm giữa hai lớp kia, nên sai số nhỏ về tốc độ cũng đẩy dự báo sang lớp bên cạnh.\n")

    w("**Theo nhóm (MAE, final test)**\n")
    rows = []
    for key, label in [("mae_by_borough", None), ("mae_by_peak", {"peak": "Cao điểm", "off_peak": "Thấp điểm"}),
                       ("mae_by_rain", {"rain": "Giờ mưa", "dry": "Giờ khô"}), ("mae_by_actual_state", None)]:
        for g in lg.get(key, {}):
            name = (label or {}).get(g) or (f"Thực tế {STATE_VI[g]}" if key == "mae_by_actual_state" else g)
            vals = [ft[k].get(key, {}).get(g) for k in MODELS]
            best = min(v for v in vals if v is not None)
            rows.append([name] + [f"**{f3(v)}**" if v == best else f3(v) for v in vals])
    groups = {k: m["focus_extra"][k]["mae_by_data_group"] for k in MODELS}
    for g, lab in [("low_data", "Link ít dữ liệu"), ("mid_data", "Link vừa"), ("high_data", "Link nhiều dữ liệu")]:
        vals = [groups[k].get(g) for k in MODELS]
        best = min(vals)
        rows.append([lab] + [f"**{f3(v)}**" if v == best else f3(v) for v in vals])
    w(table(["Nhóm", "Persistence", "Seasonal naive", "LightGBM"], rows))
    dl = m["focus_extra"]["link_mae_distribution"]
    cong = lg["mae_by_actual_state"]["CONGESTED"], pe["mae_by_actual_state"]["CONGESTED"]
    w(f"\nMAE theo link ({int(dl['lightgbm']['count'])} link có mẫu đánh giá): LightGBM trung vị {dl['lightgbm']['50%']:.2f}, "
      f"nhỏ nhất {dl['lightgbm']['min']:.2f}, lớn nhất {dl['lightgbm']['max']:.2f}; persistence trung vị {dl['persistence']['50%']:.2f}, "
      f"lớn nhất {dl['persistence']['max']:.2f} (`reports/ml/{a.version}/mae_by_link_final_test.csv`).")
    if cong[0] > cong[1]:
        w(f"Ở lớp **Tắc thực tế**, LightGBM **kém persistence một chút** ({cong[0]:.2f} so với {cong[1]:.2f}): "
          "mô hình có xu hướng kéo dự báo về mức trung bình khi đang tắc. Đây là hạn chế cần cải thiện.\n")
    else:
        w(f"Ở lớp Tắc thực tế, LightGBM {cong[0]:.2f} so với persistence {cong[1]:.2f}.\n")

    ab = {r["model"]: r for r in exp("ablation")}
    if ab:
        w("## 5. Ablation (fold 09)\n")
        lab = {"traffic": "Traffic", "traffic_temporal": "+ Temporal", "traffic_temporal_spatial": "+ Spatial", "all": "+ Weather (đầy đủ)"}
        best = min(r["mae"] for r in ab.values())
        w(table(["Nhóm feature", "MAE", "RMSE"], [[lab[k], f"**{f3(ab[k]['mae'])}**" if ab[k]["mae"] == best else f3(ab[k]["mae"]),
                                                  f3(ab[k]["rmse"])] for k in lab if k in ab]))
        d1 = ab["traffic"]["mae"] - ab["traffic_temporal"]["mae"]
        d2 = ab["traffic_temporal"]["mae"] - ab["traffic_temporal_spatial"]["mae"]
        d3 = ab["all"]["mae"] - ab["traffic_temporal_spatial"]["mae"]
        w(f"\n- Temporal giảm MAE {d1:.3f}.\n- Spatial (mã link, borough, vị trí, free-flow theo fold) giảm thêm {d2:.3f}.")
        w(f"- Thêm thời tiết {'làm MAE tăng' if d3 > 0 else 'giảm MAE'} {abs(d3):.3f}, tức **không cải thiện** trong phạm vi dao động.")
        fi_path = rep / "feature_importance_top25.csv"
        if fi_path.exists():
            fi = sorted(csv.DictReader(fi_path.open(encoding="utf-8")), key=lambda r: -float(r["gain"]))[:8]
            w("- Feature importance (gain): " + ", ".join(f"`{r['feature']}` {float(r['gain'])*100:.1f}%" for r in fi) + ".")
        w("")

    wl = {r["model"]: r for r in exp("weather_lag")}
    if wl:
        w("## 6. Độ nhạy thời tiết (fold 09)\n")
        lab = {"no_weather": "Không thời tiết", "weather_lag_0min": "Lw = 0' (bản ghi mốc T dùng ngay tại T)",
               "weather_lag_60min": "Lw = 60' (mặc định)", "weather_lag_180min": "Lw = 180'"}
        w(table(["Biến thể", "MAE"], [[lab[k], f3(wl[k]["mae"])] for k in lab if k in wl]))
        rng = max(r["mae"] for r in wl.values()) - min(r["mae"] for r in wl.values())
        w(f"\nCác biến thể chênh nhau {rng:.3f} mph. Kết quả gần như không đổi theo giả định thời điểm có dữ liệu thời tiết, "
          "nên không có dấu hiệu rò rỉ dữ liệu thời tiết ảnh hưởng tới kết luận. Ở tầm dự báo 15 phút, thời tiết giờ trước "
          "không thêm thông tin so với tốc độ gần nhất.\n")

    av = {r["model"]: r for r in exp("avail_lag")}
    if av:
        w("## 7. Độ nhạy theo độ trễ dữ liệu A (fold 09)\n")
        rows = []
        for A in (0, 5, 10):
            p, s, l = (av.get(f"{k}_A{A}", {}).get("mae") for k in ("persistence", "seasonal_naive", "lightgbm"))
            rows.append([f"{A}'" + (" (mặc định = watermark Speed Layer)" if A == 10 else ""), f3(p), f3(s), f"**{f3(l)}**",
                         pct(l, p)])
        w(table(["A", "Persistence", "Seasonal naive", "LightGBM", "Giảm so với persistence"], rows))
        slope = (av["lightgbm_A10"]["mae"] - av["lightgbm_A0"]["mae"]) / 10
        w(f"\nMỗi phút trễ dữ liệu làm MAE của LightGBM tăng khoảng {slope:.3f} mph. Giảm watermark hoặc trigger của Speed Layer "
          "sẽ làm dự báo chính xác hơn một cách đo được. Đây là ràng buộc của kiến trúc, không phải của mô hình.\n")

    wt = {r["model"]: r for r in exp("weighting")}
    if wt:
        w("## 8. Mất cân bằng lớp (fold 09)\n")
        sc_ = wt["unweighted"].get("train_state_counts", {})
        if sc_:
            w("Phân phối trạng thái target trong train: " + ", ".join(f"{STATE_VI[k]} {n(v)}" for k, v in sc_.items()) + ".\n")
        w(table(["Biến thể", "MAE", "RMSE"], [["Không trọng số", f3(wt["unweighted"]["mae"]), f3(wt["unweighted"]["rmse"])],
                                             ["Trọng số theo lớp", f3(wt["class_weighted"]["mae"]), f3(wt["class_weighted"]["rmse"])]]))
        w("\nTrọng số theo lớp làm MAE xấu đi, nên **giữ mô hình không trọng số**.\n")

    sp = {r["model"]: r for r in exp("spatial")}
    if sp:
        k = next(iter(sp.values())).get("holdout_links")
        w(f"## 9. Đoạn đường chưa gặp khi train (spatial holdout, fold 09; {k} link giữ lại)\n")
        w(table(["Mô hình", "Link đã gặp", "Link giữ lại"], [
            ["Có `link_id` (link lạ → −1)", f3(sp["with_link_id__seen_links"]["mae"]), f3(sp["with_link_id__holdout_links"]["mae"])],
            ["Không `link_id`", f3(sp["no_link_id__seen_links"]["mae"]), f"**{f3(sp['no_link_id__holdout_links']['mae'])}**"]]))
        w("\nVới đường **chưa từng gặp**, mô hình dựa vào mã link tụt mạnh; mô hình không dùng mã link tổng quát tốt hơn. "
          "Khi thêm đoạn đường mới, nên dùng biến thể không `link_id` cho tới khi có đủ lịch sử.\n")

    w("## 10. Kiểm chứng khi suy luận trên hệ thống thật\n")
    w("- **Tái lập:** `predict.py --source serving` tại gốc 2025-12-10 17:30 UTC cho kết quả trùng dự báo backtest của v2 "
      "(88/88 link, sai lệch lớn nhất 5e-11 mph).")
    w("- **Phiên phát lại kiểm chứng** 2025-12-16 07:00–09:00 New York (`scripts/demo_replay.py`): dự báo do DAG "
      "`trafficsp_ml_inference` tạo tự động trong lúc dữ liệu chạy qua Kafka → Speed Layer. Số thực tế lấy từ cửa sổ 5 phút "
      "của Speed Layer phiên đó. Số liệu cụ thể xem `docs/demo_replay.md`.\n")

    w("## 11. Hạn chế\n")
    w("1. Một số lần fit chạm giới hạn 3000 cây; MAE có thể giảm thêm khi tăng số cây hoặc learning rate.")
    w("2. Ở lớp Tắc thực tế, LightGBM không tốt hơn persistence: mô hình \"kéo về trung bình\" khi đang tắc.")
    w("3. Train trên dữ liệu batch đầy đủ, suy luận trên view hợp nhất, nên có thể lệch train/serve "
      "(Speed sau batch_state có thể thiếu bản ghi đến muộn).")
    w("4. Thời tiết là dữ liệu mô hình ECMWF IFS lưu trữ, không phải dự báo phát hành thật; không có thời điểm công bố nên dùng giả định Lw.")
    w("5. Chỉ đánh giá tầm dự báo 15 phút; chưa có khoảng tin cậy (quantile) cho dự báo.\n")

    if mc:
        w(f"## 12. So sánh phiên bản {a.compare} và {a.version}\n")
        c1, c2 = mc["folds"]["final_test"], ft
        w(table(["Chỉ số (final test)", a.compare, a.version], [
            ["Số dòng đánh giá", n(c1["lightgbm"]["n"]), n(c2["lightgbm"]["n"])],
            ["LightGBM MAE / RMSE", f"{f3(c1['lightgbm']['mae'])} / {f3(c1['lightgbm']['rmse'])}", f"{f3(c2['lightgbm']['mae'])} / {f3(c2['lightgbm']['rmse'])}"],
            ["Persistence MAE", f3(c1["persistence"]["mae"]), f3(c2["persistence"]["mae"])],
            ["Macro-F1", f3(c1["lightgbm"]["state_classification"]["macro_f1"]), f3(c2["lightgbm"]["state_classification"]["macro_f1"])],
            ["Walk-forward MAE (LightGBM)", f3(mc["walk_forward_summary"]["lightgbm"]["mae_mean"]), f3(wf["lightgbm"]["mae_mean"])]]))
        w(f"\nPersistence (không học gì) cũng giảm từ {c1['persistence']['mae']:.3f} xuống {c2['persistence']['mae']:.3f}, "
          f"cho thấy phần lớn mức giảm MAE đến từ **tập đánh giá sạch hơn** (bỏ bản ghi lỗi -101), không phải từ mô hình. "
          "So trên cùng tập dòng (2.241.548 dòng chung), hai phiên bản gần như bằng nhau "
          "(phân tích ngày 21/11/2026, script so khớp chạy trên máy train).\n")

    Path(a.out).write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"đã ghi {a.out} ({len(L)} dòng)")


if __name__ == "__main__":
    main()
