"""Dựng dữ liệu ML trên Iceberg (namespace lakehouse.ml). Chạy trong container Spark (scripts/ml/run_spark.sh):

  build_dataset.py --stage hist       # file thô 2025 -> ml.traffic_5min_hist (+ ml.weather_hourly_hist)
  build_dataset.py --stage baseline   # free-flow point-in-time theo từng as_of -> ml.link_baseline_asof
  build_dataset.py --stage features   # lưới + feature + target -> ml.features_15m
  build_dataset.py --stage export     # ghi Parquet (float32) để train ngoài Spark
  build_dataset.py --stage all [--sample-links 8 --start 2025-08-01 --end 2025-10-01]   # chế độ mẫu nhỏ: bảng *_sample

Nguồn lịch sử (--source hoặc data.source trong conf/ml/forecast.yaml):
  silver (mặc định): silver.traffic_cleaned + silver.weather_cleaned - dữ liệu Batch Layer đã làm sạch, ML không làm sạch lại.
                     Chỉ có những ngày đã đi qua Kafka -> muốn đủ cả năm để huấn luyện/backtest phải phát lại dữ liệu trước.
  raw              : file thô 2025, làm sạch bằng CÙNG hàm với Batch Layer (dùng tạm khi Silver chưa đủ năm).
Gom 5 phút dùng chung common/aggregations.py, đối chiếu với gold bằng validate_features.py.
Mọi bước ghi bằng createOrReplace -> chạy lại không sinh trùng (idempotent).
"""
import argparse
import datetime as dt
import json
import time

from pyspark.sql import functions as F

from common.config import SILVER_LINK_INFO
from common.spark_session import build_spark
from ml.feature_engineering import baseline_asof, build_features, build_hist_5min, traffic_records, weather_hist
from ml.mlconfig import load_config

FLOAT_SKIP = {"link_id", "origin_month", "borough", "cell_id"}


def tbl(cfg, key, sample):
    return cfg["data"][key] + ("_sample" if sample else "")


def links_df(spark, sample_links):
    links = spark.table(SILVER_LINK_INFO).select("link_id", "borough", "latitude", "longitude", "cell_id", "cell_distance_km")
    if sample_links:
        ids = [r.link_id for r in links.orderBy("link_id").select("link_id").collect()]
        pick = ids[:: max(1, len(ids) // sample_links)][:sample_links]
        links = links.filter(F.col("link_id").isin(pick))
    return links


def stage_hist(spark, cfg, args, links):
    lo, hi = args.start - dt.timedelta(days=8), args.end + dt.timedelta(days=1)     # đệm 8 ngày cho lag tuần
    rec = traffic_records(spark, cfg, args.source, lo, hi).join(links.select("link_id"), "link_id")
    hist = build_hist_5min(rec).withColumn("processed_at", F.current_timestamp())
    hist.writeTo(tbl(cfg, "hist_5min_table", args.sample)).using("iceberg").tableProperty("format-version", "2").createOrReplace()
    weather_hist(spark, cfg, args.source).writeTo(tbl(cfg, "weather_hist_table", args.sample)) \
        .using("iceberg").createOrReplace()


def stage_baseline(spark, cfg, args, links):
    b = cfg["baseline"]
    rec = traffic_records(spark, cfg, args.source).join(links.select("link_id"), "link_id").cache()
    weather = spark.table(tbl(cfg, "weather_hist_table", args.sample))
    out = None
    for as_of in b["as_of_dates"]:
        part = baseline_asof(rec, links, weather, b["history_start"], as_of, b)
        out = part if out is None else out.unionByName(part)
    out.withColumn("computed_at", F.current_timestamp()).writeTo(tbl(cfg, "baseline_table", args.sample)) \
        .using("iceberg").createOrReplace()
    rec.unpersist()


def stage_features(spark, cfg, args, links):
    hist = spark.table(tbl(cfg, "hist_5min_table", args.sample))
    weather = spark.table(tbl(cfg, "weather_hist_table", args.sample))
    # lưới bắt đầu sớm 8 ngày để lag tuần có giá trị; sau đó chỉ giữ gốc dự báo trong [start, end)
    feats = build_features(spark, hist, links, weather, cfg, args.start - dt.timedelta(days=8), args.end)
    feats = feats.filter((F.col("origin_utc") >= F.lit(args.start)) & (F.col("origin_utc") < F.lit(args.end)))
    feats = feats.withColumn("feature_version", F.lit(cfg["version"])).withColumn("built_at", F.current_timestamp())
    (feats.repartition("origin_month").sortWithinPartitions("origin_utc", "link_id")
          .writeTo(tbl(cfg, "features_table", args.sample)).using("iceberg").partitionedBy(F.col("origin_month"))
          .tableProperty("format-version", "2").createOrReplace())


def stage_export(spark, cfg, args):
    feats = spark.table(tbl(cfg, "features_table", args.sample))
    cols = []
    for f in feats.schema.fields:
        t = f.dataType.simpleString()
        if f.name in FLOAT_SKIP or t in ("string", "timestamp", "boolean"):
            cols.append(F.col(f.name))
        elif t in ("double", "float"):
            cols.append(F.col(f.name).cast("float"))
        else:
            cols.append(F.col(f.name).cast("int") if f.name not in ("link_id",) else F.col(f.name))
    out = f"{cfg['data']['export_dir']}/{'features_sample' if args.sample else 'features'}_{cfg['version']}"
    feats.select(cols).repartition("origin_month").write.mode("overwrite").partitionBy("origin_month").parquet(out)
    base = f"{cfg['data']['export_dir']}/{'baseline_sample' if args.sample else 'baseline'}_{cfg['version']}"
    spark.table(tbl(cfg, "baseline_table", args.sample)).coalesce(1).write.mode("overwrite").parquet(base)
    print(json.dumps({"export": out, "baseline": base}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, choices=["hist", "baseline", "features", "export", "all"])
    ap.add_argument("--start", default="2025-01-01")
    ap.add_argument("--end", default="2026-01-01")
    ap.add_argument("--sample-links", type=int, default=0, help=">0: chế độ mẫu nhỏ, ghi bảng *_sample")
    ap.add_argument("--source", choices=["silver", "raw"], help="nguồn lịch sử; mặc định data.source trong forecast.yaml")
    args = ap.parse_args()
    args.start, args.end = dt.datetime.fromisoformat(args.start), dt.datetime.fromisoformat(args.end)
    args.sample = args.sample_links > 0
    cfg = load_config()
    args.source = args.source or cfg["data"].get("source", "silver")
    spark = build_spark("ml_build_dataset", {"spark.sql.legacy.parquet.nanosAsLong": "true",
                                             "spark.sql.shuffle.partitions": "64"})
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg['data']['namespace']}")
    links = links_df(spark, args.sample_links)
    stages = ["hist", "baseline", "features", "export"] if args.stage == "all" else [args.stage]
    for st in stages:
        t0 = time.time()
        if st == "hist":
            stage_hist(spark, cfg, args, links)
        elif st == "baseline":
            stage_baseline(spark, cfg, args, links)
        elif st == "features":
            stage_features(spark, cfg, args, links)
        else:
            stage_export(spark, cfg, args)
        print(json.dumps({"stage": st, "sample": args.sample, "seconds": round(time.time() - t0, 1)}), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
