# speed_layer.py: SPEED LAYER của Lambda bằng Spark Structured Streaming (không dùng Flink).
#
# 3 streaming query độc lập, mỗi query một checkpoint riêng (config.CHECKPOINT_SPEED_*):
#   speed_latest  : Kafka traffic_raw -> parse/kiểm tra -> foreachBatch -> ClickHouse traffic_latest (+ _any, + thống kê)
#                   Không cần state Spark: bảng ReplacingMergeTree(event_time) đảm bảo bản cũ đến muộn không ghi đè bản mới.
#   speed_5min    : Kafka traffic_raw -> event-time UTC -> watermark -> dropDuplicates(link_id, data_as_of, event_time_utc)
#                   -> tumbling window 5 phút (append) -> ClickHouse traffic_5min_rt
#                   (KHÔNG dùng dropDuplicatesWithinWatermark: Spark 3.5.0 lỗi None.get khi canonicalize plan - SPARK-46676,
#                    sửa ở 3.5.1. Có event_time_utc trong khóa nên state vẫn được dọn theo watermark = state TTL.)
#                   2 toán tử stateful nối tiếp chỉ được Spark 3.5 hỗ trợ ở append mode -> mỗi cửa sổ phát 1 lần
#                   khi watermark vượt window_end. Bản ghi muộn hơn watermark bị loại (đếm numRowsDroppedByWatermark)
#                   và được Batch Layer sửa khi recompute (Lambda handoff).
#   speed_weather : Kafka weather_raw -> ClickHouse weather_hourly (ghép thời tiết theo khoảng giờ làm ở Query Layer).
#
# Chính sách thời gian/chất lượng dùng chung với Batch: common/time_rules.py, common/quality_rules.py.
# Chạy (trong container Spark):
#   docker exec -e PYTHONPATH=/opt/spark/jobs traffic-spark-master /opt/spark/bin/spark-submit \
#       /opt/spark/jobs/stream/speed_layer.py --queries latest,5min,weather
import argparse
import datetime as dt
import json
import os
import time

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQueryListener

from common import config
from common.clickhouse_io import get_client, insert_df
from common.quality_rules import traffic_state_col, with_quality
from common.schemas import TRAFFIC_FIELDS, WEATHER_FIELDS, to_ddl
from common.spark_session import build_spark
from common.time_rules import with_event_time

UTC = dt.timezone.utc


# ------------------------------------------------------------------ đọc + chuẩn hóa
def read_kafka(spark: SparkSession, topic: str, starting: str, max_offsets: int) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", starting)
            .option("maxOffsetsPerTrigger", max_offsets)
            .load())


def parse_traffic(raw: DataFrame) -> DataFrame:
    """Giữ nguyên giá trị gốc (speed, travel_time, data_as_of); chỉ thêm cột chuẩn hóa + cờ."""
    p = raw.select(F.from_json(F.col("value").cast("string"), to_ddl(TRAFFIC_FIELDS)).alias("r"),
                   F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
                   F.col("timestamp").alias("kafka_ts"))
    df = p.select(F.col("r.link_id").cast("long").alias("link_id"), "r.data_as_of", "r.speed", "r.travel_time",
                  "r.run_id", "kafka_partition", "kafka_offset", "kafka_ts")
    return with_quality(with_event_time(df, "data_as_of"), "speed", "travel_time")


def parse_weather(raw: DataFrame) -> DataFrame:
    p = raw.select(F.from_json(F.col("value").cast("string"), to_ddl(WEATHER_FIELDS)).alias("r"),
                   F.col("offset").alias("kafka_offset"), F.col("timestamp").alias("kafka_ts"))
    return (p.select("r.*", "kafka_offset", "kafka_ts")
             .withColumn("time_utc", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm")))


# ------------------------------------------------------------------ free-flow baseline (từ link_dim)
class Baseline:
    """Free-flow theo link, nạp từ ClickHouse link_dim (do Batch tính), làm mới định kỳ."""

    def __init__(self, spark: SparkSession, refresh_sec: int = 300):
        self.spark, self.refresh, self.loaded, self.df = spark, refresh_sec, 0.0, None

    def get(self) -> DataFrame:
        if self.df is None or time.time() - self.loaded > self.refresh:
            try:
                rows = get_client().query("SELECT link_id, free_flow_speed FROM link_dim FINAL").result_rows
            except Exception as e:                       # chưa có bảng / ClickHouse tạm lỗi: dùng bản cũ
                print(f"[baseline] không nạp được link_dim: {e}")
                rows = None
            if rows is not None or self.df is None:
                pdf = pd.DataFrame(rows or [], columns=["link_id", "free_flow_speed"])
                self.df = self.spark.createDataFrame(pdf, "link_id long, free_flow_speed double").cache()
                self.loaded = time.time()
        return self.df


def _utc(pdf: pd.DataFrame, cols):
    for c in cols:
        pdf[c] = pd.to_datetime(pdf[c]).dt.tz_localize("UTC") if pd.to_datetime(pdf[c]).dt.tz is None else pdf[c]
    return pdf


def _now():
    return pd.Timestamp.now(tz="UTC")


def _write_latency(client, query: str, batch_id: int, kafka_ts: pd.Series):
    """Ghi phân vị kafka_ts của micro-batch + thời điểm ghi xong -> bảng speed_latency (đo độ trễ end-to-end)."""
    ts = pd.to_datetime(kafka_ts.dropna(), utc=True)
    if not len(ts):
        return
    q = ts.quantile([0.05, 0.5])
    row = pd.DataFrame([{"query": query, "batch_id": batch_id, "n_items": len(ts), "kafka_ts_min": ts.min(),
                         "kafka_ts_p05": q.iloc[0], "kafka_ts_p50": q.iloc[1], "kafka_ts_max": ts.max(), "done_at": _now()}])
    try:
        insert_df(client, "speed_latency", row, token=f"latency:{query}:{batch_id}")
    except Exception as e:                       # số đo benchmark không được làm chết query dữ liệu
        print(f"[speed] không ghi được latency: {e}")


# ------------------------------------------------------------------ sink: latest reading
def make_latest_sink(baseline: Baseline):
    def sink(batch: DataFrame, batch_id: int):
        client = get_client()
        b = batch.persist()
        s = b.agg(F.count("*").alias("n"), F.sum(F.col("is_valid").cast("int")).alias("v"),
                  F.sum(F.col("dst_ambiguous").cast("int")).alias("amb"),
                  F.sum(F.col("dst_nonexistent").cast("int")).alias("nonex"),
                  F.sum((F.col("local_ts").isNull()).cast("int")).alias("unparse"),
                  F.min("event_time_utc").alias("mn"), F.max("event_time_utc").alias("mx")).collect()[0]
        reasons = {r["invalid_reason"]: int(r["count"]) for r in
                   b.filter(F.col("invalid_reason").isNotNull()).groupBy("invalid_reason").count().collect()}
        stats = pd.DataFrame([{"query": "speed_latest", "batch_id": batch_id, "n_rows": int(s["n"] or 0), "n_valid": int(s["v"] or 0),
                               "n_dst_ambiguous": int(s["amb"] or 0), "n_dst_nonexistent": int(s["nonex"] or 0),
                               "n_unparseable": int(s["unparse"] or 0), "invalid_reasons": reasons,
                               "min_event_time": s["mn"], "max_event_time": s["mx"], "written_at": _now()}])
        stats = _utc(stats, ["min_event_time", "max_event_time"])
        insert_df(client, "speed_batch_stats", stats, token=f"stats:speed_latest:{batch_id}")

        keep = ["data_as_of", "speed", "travel_time", "run_id", "kafka_partition", "kafka_offset", "kafka_ts", "is_valid", "invalid_reason"]
        timed = b.filter(F.col("event_time_utc").isNotNull() & F.col("link_id").isNotNull())
        # 1 dòng / link: bản có event_time lớn nhất trong micro-batch (bảng ClickHouse lo phần so với các batch trước)
        latest_any = (timed.groupBy("link_id")
                      .agg(F.max_by(F.struct(*keep), "event_time_utc").alias("r"), F.max("event_time_utc").alias("event_time"))
                      .select("link_id", "event_time", "r.*"))
        pdf_any = latest_any.select("link_id", "event_time", "data_as_of", "speed", "travel_time",
                                    F.col("is_valid").cast("int").alias("is_valid"),
                                    F.coalesce("invalid_reason", F.lit("")).alias("invalid_reason"), "kafka_ts").toPandas()
        if len(pdf_any):
            pdf_any["updated_at"], pdf_any["batch_id"] = _now(), batch_id
            insert_df(client, "traffic_latest_any", _utc(pdf_any, ["event_time", "kafka_ts"]), token=f"speed_latest_any:{batch_id}")

        latest = (timed.filter("is_valid").groupBy("link_id")
                  .agg(F.max_by(F.struct(*keep), "event_time_utc").alias("r"), F.max("event_time_utc").alias("event_time"))
                  .select("link_id", "event_time", "r.*")
                  .join(F.broadcast(baseline.get()), "link_id", "left")
                  .withColumn("traffic_state", traffic_state_col("speed", "free_flow_speed"))
                  .withColumn("quality_flag", F.lit("OK")))
        pdf = latest.select("link_id", "event_time", "data_as_of", "speed", "travel_time", "traffic_state", "quality_flag",
                            "free_flow_speed", "run_id", "kafka_partition", "kafka_offset", "kafka_ts").toPandas()
        if len(pdf):
            pdf["updated_at"], pdf["batch_id"] = _now(), batch_id
            insert_df(client, "traffic_latest", _utc(pdf, ["event_time", "kafka_ts"]), token=f"speed_latest:{batch_id}")
        _write_latency(client, "speed_latest", batch_id, b.select("kafka_ts").toPandas()["kafka_ts"])
        b.unpersist()
    return sink


# ------------------------------------------------------------------ 5 phút
def window_5min(traffic: DataFrame, watermark: str) -> DataFrame:
    valid = F.col("is_valid")
    ev = traffic.filter(F.col("event_time_utc").isNotNull() & F.col("link_id").isNotNull())   # DST mơ hồ đã bị loại + đếm ở speed_latest
    return (ev.withWatermark("event_time_utc", watermark)
              # business key (link_id, data_as_of) + event_time để state hết hạn theo watermark; không dựa vào Kafka offset/run_id
              .dropDuplicates(["link_id", "data_as_of", "event_time_utc"])
              .groupBy(F.window("event_time_utc", config.SPEED_WINDOW), "link_id")
              .agg(F.count("*").alias("n_records"),
                   F.sum(valid.cast("int")).alias("n_valid"),
                   F.sum((F.col("speed") == 0).cast("int")).alias("n_zero"),
                   F.sum((~valid & (F.col("speed") != 0)).cast("int")).alias("n_invalid_other"),
                   F.sum(F.when(valid, F.col("speed"))).alias("sum_speed"),
                   F.sum(F.when(valid, F.col("speed") * F.col("speed"))).alias("sum_speed_sq"),
                   F.sum(F.when(valid, F.col("travel_time"))).alias("sum_travel_time"),
                   F.min(F.when(valid, F.col("speed"))).alias("min_speed"),
                   F.max(F.when(valid, F.col("speed"))).alias("max_speed"),
                   F.max("kafka_ts").alias("kafka_ts_max")))


def make_5min_sink(baseline: Baseline):
    def sink(batch: DataFrame, batch_id: int):
        out = (batch.select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "link_id",
                            "n_records", F.coalesce("n_valid", F.lit(0)).alias("n_valid"), F.coalesce("n_zero", F.lit(0)).alias("n_zero"),
                            F.coalesce("n_invalid_other", F.lit(0)).alias("n_invalid_other"),
                            F.coalesce("sum_speed", F.lit(0.0)).alias("sum_speed"), F.coalesce("sum_speed_sq", F.lit(0.0)).alias("sum_speed_sq"),
                            F.coalesce("sum_travel_time", F.lit(0)).alias("sum_travel_time"), "min_speed", "max_speed", "kafka_ts_max")
               .withColumn("avg_speed", F.when(F.col("n_valid") > 0, F.col("sum_speed") / F.col("n_valid")))
               .join(F.broadcast(baseline.get()), "link_id", "left")
               .withColumn("traffic_state", traffic_state_col("avg_speed", "free_flow_speed", "n_valid"))
               .withColumn("quality_flag", F.when(F.col("n_valid") == 0, "NO_VALID_DATA").otherwise("OK"))
               .drop("free_flow_speed"))
        pdf = out.toPandas()
        if len(pdf):
            pdf["updated_at"], pdf["batch_id"] = _now(), batch_id
            client = get_client()
            insert_df(client, "traffic_5min_rt", _utc(pdf, ["window_start", "window_end", "kafka_ts_max"]),
                      token=f"speed_5min:{batch_id}")
            _write_latency(client, "speed_5min", batch_id, pdf["kafka_ts_max"])
    return sink


# ------------------------------------------------------------------ weather
def weather_sink(batch: DataFrame, batch_id: int):
    w = (batch.filter(F.col("cell_id").isNotNull() & F.col("time_utc").isNotNull())
              .groupBy("cell_id", "time_utc")
              .agg(F.max_by(F.struct("temperature_2m", "relative_humidity_2m", "precipitation", "snowfall", "wind_speed_10m",
                                     "wind_gusts_10m", "weather_code", "run_id", "kafka_ts"), "kafka_offset").alias("r"))
              .select("cell_id", "time_utc", "r.*"))
    pdf = w.toPandas()
    if len(pdf):
        pdf["source"], pdf["updated_at"] = "stream", _now()
        insert_df(get_client(), "weather_hourly", _utc(pdf, ["time_utc", "kafka_ts"]), token=f"speed_weather:{batch_id}")


# ------------------------------------------------------------------ quan sát (benchmark)
class ProgressListener(StreamingQueryListener):
    def __init__(self, log_path: str | None = None):
        self.log_path = log_path

    def onQueryStarted(self, event):
        print(f"[speed] start {event.name} id={event.id}")

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        st = p.get("stateOperators") or []
        src = (p.get("sources") or [{}])[0].get("metrics") or {}
        row = {"query": p.get("name") or "", "run_uuid": p.get("runId"), "batch_id": p.get("batchId"),
               "ts": pd.Timestamp(p.get("timestamp")), "num_input_rows": p.get("numInputRows", 0),
               "input_rows_per_sec": float(p.get("inputRowsPerSecond") or 0), "processed_rows_per_sec": float(p.get("processedRowsPerSecond") or 0),
               "batch_duration_ms": (p.get("durationMs") or {}).get("triggerExecution", 0), "trigger_execution_ms": (p.get("durationMs") or {}).get("triggerExecution", 0),
               "add_batch_ms": (p.get("durationMs") or {}).get("addBatch", 0), "get_batch_ms": (p.get("durationMs") or {}).get("getBatch", 0),
               "watermark": (p.get("eventTime") or {}).get("watermark", ""),
               "state_rows": sum(int(s.get("numRowsTotal", 0)) for s in st), "state_memory_bytes": sum(int(s.get("memoryUsedBytes", 0)) for s in st),
               "rows_dropped_by_watermark": sum(int(s.get("numRowsDroppedByWatermark", 0)) for s in st),
               "max_offsets_behind": int(float(src["maxOffsetsBehindLatest"])) if "maxOffsetsBehindLatest" in src else None,
               "avg_offsets_behind": float(src["avgOffsetsBehindLatest"]) if "avgOffsetsBehindLatest" in src else None,
               "raw_json": event.progress.json}
        try:
            insert_df(get_client(), "stream_progress", pd.DataFrame([row]))
        except Exception as e:
            print(f"[speed] không ghi được progress: {e}")
        if self.log_path:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps({k: (str(v) if k == "ts" else v) for k, v in row.items() if k != "raw_json"}) + "\n")

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[speed] stop id={event.id} exception={event.exception}")


# ------------------------------------------------------------------ main
def start(df: DataFrame, name: str, checkpoint: str, sink, args, mode: str = "update"):
    w = (df.writeStream.queryName(name).outputMode(mode).option("checkpointLocation", checkpoint).foreachBatch(sink))
    w = w.trigger(availableNow=True) if args.available_now else w.trigger(processingTime=args.trigger)
    return w.start()


def main():
    ap = argparse.ArgumentParser(description="Speed layer (Spark Structured Streaming)")
    ap.add_argument("--queries", default="latest,5min,weather")
    ap.add_argument("--trigger", default=config.SPEED_TRIGGER)
    ap.add_argument("--watermark", default=config.SPEED_WATERMARK)
    ap.add_argument("--starting-offsets", default=config.SPEED_STARTING_OFFSETS)
    ap.add_argument("--max-offsets", type=int, default=config.SPEED_MAX_OFFSETS_PER_TRIGGER)
    ap.add_argument("--checkpoint-root", default=config.CHECKPOINT_ROOT, help="đổi để chạy thử nghiệm độc lập")
    ap.add_argument("--available-now", action="store_true", help="xử lý hết dữ liệu đang có rồi dừng")
    ap.add_argument("--run-seconds", type=int, default=0, help="dừng sau N giây (kiểm thử/benchmark)")
    ap.add_argument("--progress-log", help="ghi thêm tiến độ ra file JSONL")
    args = ap.parse_args()

    conf = {"spark.sql.shuffle.partitions": config.SPEED_SHUFFLE_PARTITIONS}
    if os.getenv("SPEED_STATE_STORE", "rocksdb") == "rocksdb":
        conf["spark.sql.streaming.stateStore.providerClass"] = \
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    spark = build_spark("speed_layer", conf)
    spark.streams.addListener(ProgressListener(args.progress_log))
    baseline = Baseline(spark)
    ck = lambda q: f"{args.checkpoint_root}/speed_{q}"
    wanted = [q.strip() for q in args.queries.split(",") if q.strip()]
    queries = []
    if "latest" in wanted:
        t = parse_traffic(read_kafka(spark, config.TOPIC_TRAFFIC, args.starting_offsets, args.max_offsets))
        queries.append(start(t, "speed_latest", ck("latest"), make_latest_sink(baseline), args, "append"))
    if "5min" in wanted:
        t = parse_traffic(read_kafka(spark, config.TOPIC_TRAFFIC, args.starting_offsets, args.max_offsets))
        queries.append(start(window_5min(t, args.watermark), "speed_5min", ck("5min"), make_5min_sink(baseline), args, "append"))
    if "weather" in wanted:
        w = parse_weather(read_kafka(spark, config.TOPIC_WEATHER, args.starting_offsets, args.max_offsets))
        queries.append(start(w, "speed_weather", ck("weather"), weather_sink, args, "append"))

    if args.run_seconds:
        time.sleep(args.run_seconds)
        for q in queries:
            q.stop()
    elif args.available_now:
        for q in queries:
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
