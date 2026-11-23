"""Kiểm chứng dữ liệu ML trên Iceberg bằng dữ liệu thực (Spark). In JSON + exit 1 nếu có kiểm tra FAIL.
  validate_features.py [--sample] [--compare-gold-start 2025-09-01 --compare-gold-end 2025-10-01]

Kiểm tra:
  1. ml.traffic_5min_hist khớp gold.fact_traffic_5min (bản published) trên khoảng có ở cả hai (cùng công thức -> phải bằng nhau).
     Mặc định 01/09 04:00Z -> 01/10 04:00Z = trọn tháng 9 theo ngày New York (Silver lọc theo ngày NY; tháng 10-11 Silver chỉ có một phần).
  2. Không trùng grain: hist (link_id, window_start_utc); features (link_id, origin_utc); baseline (as_of_utc, link_id).
  3. Lưới liên tục: mỗi link đủ mọi gốc dự báo 5 phút trong khoảng, bước = 300 s.
  4. Point-in-time (trên TOÀN BỘ dòng): data_cutoff = origin - A; target_window_start = origin + H - 5'; mọi bản ghi thời tiết
     dùng làm feature có record_utc + Lw <= data_cutoff; lag/rolling/target khớp phép tính ĐỘC LẬP bằng join theo thời gian.
  5. Thời tiết theo giờ liên tục từng ô (để tổng 3 giờ theo số dòng là đúng 3 giờ).
  6. Thống kê target: số dòng có / không có target, theo tháng.
"""
import argparse
import datetime as dt
import json
import sys

from pyspark.sql import functions as F

from common.spark_session import build_spark
from ml.feature_engineering import target_steps
from ml.mlconfig import load_config


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", action="store_true")
    ap.add_argument("--compare-gold-start", default="2025-09-01T04:00")
    ap.add_argument("--compare-gold-end", default="2025-10-01T04:00")
    ap.add_argument("--out", default="/opt/lakehouse/logs/ml_validate_features.json")
    ap.add_argument("--skip-gold-compare", action="store_true",
                    help="bỏ kiểm tra 1 khi gold.fact_traffic_5min chưa có hoặc dựng từ Silver khác (vd máy chưa chuyển code)")
    args = ap.parse_args()
    cfg = load_config()
    sfx = "_sample" if args.sample else ""
    p = cfg["problem"]
    A, H, Lw = p["availability_lag_minutes"], p["horizon_minutes"], p["weather_lag_minutes"]
    spark = build_spark("ml_validate_features")
    hist = spark.table(cfg["data"]["hist_5min_table"] + sfx)
    feats = spark.table(cfg["data"]["features_table"] + sfx)
    base = spark.table(cfg["data"]["baseline_table"] + sfx)
    weather = spark.table(cfg["data"]["weather_hist_table"] + sfx)
    res, fails = {}, []

    def check(name, ok, detail):
        res[name] = {"ok": bool(ok), **detail}
        if not ok:
            fails.append(name)

    # 1. hist vs gold (published)
    if args.skip_gold_compare:
        res["hist_matches_gold"] = {"ok": None, "skipped": True}
    else:
        lo, hi = dt.datetime.fromisoformat(args.compare_gold_start), dt.datetime.fromisoformat(args.compare_gold_end)
        gold = (spark.read.option("tag", "published").table("lakehouse.gold.fact_traffic_5min")
                     .filter((F.col("window_start_utc") >= F.lit(lo)) & (F.col("window_start_utc") < F.lit(hi))))
        h = hist.filter((F.col("window_start_utc") >= F.lit(lo)) & (F.col("window_start_utc") < F.lit(hi)))
        links = [r.link_id for r in h.select("link_id").distinct().collect()]
        gold = gold.filter(F.col("link_id").isin(links))
        cols = ["n_records", "n_valid", "n_zero", "sum_travel_time_sec"]
        j = h.alias("h").join(gold.alias("g"), ["link_id", "window_start_utc"], "full_outer")
        diff = F.lit(False)
        for c in cols:
            diff = diff | ~F.col(f"h.{c}").eqNullSafe(F.col(f"g.{c}"))
        diff = diff | (F.abs(F.coalesce(F.col("h.sum_speed_mph"), F.lit(0.0)) - F.coalesce(F.col("g.sum_speed_mph"), F.lit(0.0))) > 1e-6)
        stats = j.agg(F.count("*").alias("rows"), F.sum(F.col("h.link_id").isNull().cast("int")).alias("only_gold"),
                      F.sum(F.col("g.n_records").isNull().cast("int")).alias("only_hist"),
                      F.sum(diff.cast("int")).alias("mismatch")).first().asDict()
        check("hist_matches_gold", stats["rows"] > 0 and stats["mismatch"] == 0,
              {**stats, "range": [args.compare_gold_start, args.compare_gold_end], "links": len(links)})

    # 2. grain
    for name, df, key in [("hist", hist, ["link_id", "window_start_utc"]), ("features", feats, ["link_id", "origin_utc"]),
                          ("baseline", base, ["as_of_utc", "link_id"])]:
        n, nd = df.count(), df.select(*key).distinct().count()
        check(f"no_duplicate_grain_{name}", n == nd, {"rows": n, "distinct_keys": nd})

    # 3. lưới liên tục
    per = feats.groupBy("link_id").agg(F.count("*").alias("n"), F.min("origin_utc").alias("lo"), F.max("origin_utc").alias("hi"))
    per = per.withColumn("expected", (F.unix_timestamp("hi") - F.unix_timestamp("lo")) / 300 + 1)
    bad = per.filter(F.col("n") != F.col("expected")).count()
    check("grid_continuous", bad == 0, {"links": per.count(), "links_with_gaps": bad})

    # 4. point-in-time trên toàn bộ dòng
    pit = feats.agg(
        F.sum((F.col("data_cutoff_utc") != F.col("origin_utc") - F.expr(f"INTERVAL {A} MINUTES")).cast("int")).alias("cutoff_bad"),
        F.sum((F.col("target_window_start_utc") != F.col("origin_utc") + F.expr(f"INTERVAL {H - 5} MINUTES")).cast("int")).alias("target_bad"),
        F.sum((F.col("last_window_start_utc") + F.expr("INTERVAL 5 MINUTES") != F.col("data_cutoff_utc")).cast("int")).alias("last_window_bad"),
        *[F.sum((F.col(f"{pre}record_utc") + F.expr(f"INTERVAL {lag} MINUTES") > F.col("data_cutoff_utc")).cast("int")).alias(f"weather_{pre}future")
          for pre, lag in [("w_", Lw)] + [(f"w{x}_", x) for x in p["weather_sensitivity_lags"]]]).first().asDict()
    check("point_in_time_columns", all(v == 0 for v in pit.values()), pit)

    # đối chiếu độc lập lag / rolling / target bằng join theo thời gian thật (mẫu ngẫu nhiên lớn)
    smp = feats.sample(fraction=0.02, seed=7).select("link_id", "last_window_start_utc", "speed_lag15", "speed_lag60",
                                                       "roll_mean_30", "target_speed_15m", "speed_same_target_last_week")
    hs = hist.select("link_id", "window_start_utc", F.when(F.col("n_valid") > 0, F.col("avg_speed_mph")).alias("spd"), "n_valid",
                     "sum_speed_mph")

    def at(offset_min, alias):
        return hs.select("link_id", (F.col("window_start_utc") - F.expr(f"INTERVAL {offset_min} MINUTES")).alias("last_window_start_utc"),
                         F.col("spd").alias(alias))
    ht = target_steps(H, A)
    chk = (smp.join(at(-15, "x_lag15"), ["link_id", "last_window_start_utc"], "left")
              .join(at(-60, "x_lag60"), ["link_id", "last_window_start_utc"], "left")
              .join(at(ht * 5, "x_target"), ["link_id", "last_window_start_utc"], "left")
              .join(at(ht * 5 - 7 * 24 * 60, "x_week"), ["link_id", "last_window_start_utc"], "left"))
    roll = (smp.select("link_id", "last_window_start_utc").join(hs.withColumnRenamed("window_start_utc", "ws"), "link_id")
               .filter((F.col("ws") <= F.col("last_window_start_utc")) & (F.col("ws") > F.col("last_window_start_utc") - F.expr("INTERVAL 30 MINUTES")))
               .groupBy("link_id", "last_window_start_utc").agg((F.sum("sum_speed_mph") / F.sum("n_valid")).alias("x_roll30")))
    chk = chk.join(roll, ["link_id", "last_window_start_utc"], "left")
    neq = lambda a, b: ~((F.col(a).isNull() & F.col(b).isNull()) | (F.abs(F.col(a) - F.col(b)) < 1e-4))
    m = chk.agg(F.count("*").alias("sampled"),
                *[F.sum(neq(a, b).cast("int")).alias(f"{a}_mismatch") for a, b in
                  [("speed_lag15", "x_lag15"), ("speed_lag60", "x_lag60"), ("target_speed_15m", "x_target"),
                   ("speed_same_target_last_week", "x_week"), ("roll_mean_30", "x_roll30")]]).first().asDict()
    check("independent_recompute_lag_roll_target", all(v == 0 for k, v in m.items() if k != "sampled"), m)

    # 5. thời tiết liên tục theo giờ
    wg = weather.groupBy("cell_id").agg(F.count("*").alias("n"), F.min("weather_utc").alias("lo"), F.max("weather_utc").alias("hi"))
    wbad = wg.filter(F.col("n") != (F.unix_timestamp("hi") - F.unix_timestamp("lo")) / 3600 + 1).count()
    check("weather_hourly_continuous", wbad == 0, {"cells": wg.count(), "cells_with_gaps": wbad})

    # 6. thống kê target
    tg = (feats.groupBy("origin_month").agg(F.count("*").alias("rows"), F.sum(F.col("target_speed_15m").isNotNull().cast("int")).alias("with_target"),
                                             F.sum(F.col("speed_last_known_60").isNull().cast("int")).alias("no_recent_data"))
               .orderBy("origin_month").collect())
    res["target_by_month"] = [r.asDict() for r in tg]
    res["baseline"] = [r.asDict() for r in base.groupBy("as_of_utc").agg(F.count("*").alias("links"),
                       F.count("free_flow_speed").alias("with_free_flow"), F.min("n_samples").alias("min_samples")).orderBy("as_of_utc").collect()]
    res["failed"] = fails
    txt = json.dumps(res, default=str, indent=1)
    print(txt)
    with open(args.out, "w") as f:
        f.write(txt)
    spark.stop()
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
