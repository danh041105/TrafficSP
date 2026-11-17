# speed_layer.py: SPEED LAYER của Lambda bằng Spark Structured Streaming -> Apache Iceberg (không Flink, không ClickHouse).
#
# 3 streaming query độc lập, mỗi query một checkpoint riêng (config.CHECKPOINT_SPEED_*):
#   speed_5min    : Kafka traffic_raw -> event-time UTC (DST: NULL + cờ) -> watermark -> dropDuplicates(link_id, data_as_of,
#                   event_time_utc) -> tumbling window 5 phút (append) -> Iceberg speed.traffic_5min_rt bằng SINK ICEBERG GỐC.
#                   Sink Iceberg ghi epoch (batchId) + queryId vào summary của snapshot; khi micro-batch chạy lại sau lỗi
#                   (cùng checkpoint) nó bỏ qua epoch đã commit -> không ghi trùng. Mỗi cửa sổ chỉ phát 1 lần (append).
#                   Không dùng dropDuplicatesWithinWatermark: Spark 3.5.0 lỗi None.get (SPARK-46676, sửa ở 3.5.1).
#   speed_latest  : Kafka traffic_raw -> foreachBatch -> MERGE INTO speed.traffic_latest (1 dòng / link).
#                   Chỉ cập nhật khi bản mới hơn theo (event_time, kafka_offset) -> event cũ không ghi đè event mới,
#                   chạy lại cùng micro-batch là no-op (idempotent). Giữ riêng bản hợp lệ mới nhất và quan trắc mới nhất.
#   speed_weather : Kafka weather_raw -> Iceberg speed.weather_obs (sink gốc, append). Ghép thời tiết theo khoảng giờ
#                   [H, H+1) làm ở Trino (serving.v_weather_interval).
# Batch Layer KHÔNG ghi vào các bảng speed.*; Speed Layer KHÔNG ghi vào gold.*.
# Quy tắc thời gian/chất lượng dùng chung với Batch: common/time_rules.py, common/quality_rules.py, common/aggregations.py.
# Số đo vận hành (tiến độ, thống kê chất lượng, độ trễ) ghi JSONL ở OPS_LOG_DIR - không ghi Iceberg để không thêm commit nhỏ.
#
# Chạy (trong container Spark; dev có supervisor tự chạy lại):
#   docker exec -d traffic-spark-master bash /opt/spark/jobs/stream/run_speed_layer.sh --trigger "30 seconds"
import argparse
import json
import os
import time

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQueryListener

from common import config
from common.aggregations import measures
from common.quality_rules import with_quality
from common.read_kafka import read_kafka
from common.schemas import (SPEED_TRAFFIC_5MIN_FIELDS, SPEED_WEATHER_OBS_FIELDS, TRAFFIC_FIELDS, WEATHER_FIELDS,
                            to_ddl)
from common.spark_session import build_spark
from common.time_rules import with_event_time


# ------------------------------------------------------------------ nhật ký vận hành (JSONL)
class OpsLog:
    def __init__(self, directory: str):
        self.dir = directory
        os.makedirs(directory, exist_ok=True)

    def write(self, name: str, row: dict):
        try:
            with open(os.path.join(self.dir, f"{name}.jsonl"), "a", encoding="utf-8") as f:
                f.write(json.dumps(row, default=str) + "\n")
        except OSError as e:                    # số đo không được làm chết query dữ liệu
            print(f"[speed] không ghi được {name}: {e}")


def conform(df: DataFrame, fields: dict) -> DataFrame:
    return df.select([F.col(c).cast(t).alias(c) for c, t in fields.items()])


# ------------------------------------------------------------------ đọc + chuẩn hóa
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
                   F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
                   F.col("timestamp").alias("kafka_ts"))
    return (p.select("r.*", "kafka_partition", "kafka_offset", "kafka_ts")
             .withColumn("time_utc", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
             .filter(F.col("cell_id").isNotNull() & F.col("time_utc").isNotNull())
             .withColumn("processed_at", F.current_timestamp()))


# ------------------------------------------------------------------ 5 phút (sink Iceberg gốc)
def window_5min(traffic: DataFrame, watermark: str) -> DataFrame:
    ev = traffic.filter(F.col("event_time_utc").isNotNull() & F.col("link_id").isNotNull())   # DST mơ hồ: bị loại, đếm ở latest
    agg = (ev.withWatermark("event_time_utc", watermark)
             # business key (link_id, data_as_of) + event_time để state hết hạn theo watermark; không dựa vào offset/run_id
             .dropDuplicates(["link_id", "data_as_of", "event_time_utc"])
             .groupBy(F.window("event_time_utc", config.SPEED_WINDOW), "link_id")
             .agg(*measures(), F.max("kafka_ts").alias("kafka_ts_max")))
    out = (agg.withColumn("window_start_utc", F.col("window.start")).withColumn("window_end_utc", F.col("window.end"))
              .withColumn("avg_speed", F.when(F.col("n_valid") > 0, F.col("sum_speed") / F.col("n_valid")))
              .withColumn("quality_flag", F.when(F.col("n_valid") == 0, "NO_VALID_DATA").otherwise("OK"))
              .withColumn("processed_at", F.current_timestamp()))
    return conform(out, SPEED_TRAFFIC_5MIN_FIELDS)


# ------------------------------------------------------------------ latest reading (foreachBatch + MERGE INTO)
LATEST_KEEP = ["event_time_utc", "data_as_of", "speed", "travel_time", "is_valid", "invalid_reason",
               "kafka_partition", "kafka_offset", "kafka_ts", "run_id"]


def latest_source(b: DataFrame) -> DataFrame:
    """1 dòng / link trong micro-batch: bản hợp lệ mới nhất + quan trắc mới nhất, xếp theo (event_time, kafka_offset).
    Cùng link luôn cùng partition Kafka (key = link_id) nên kafka_offset phân xử được bản trùng event_time."""
    timed = b.filter(F.col("event_time_utc").isNotNull() & F.col("link_id").isNotNull())
    order = F.struct("event_time_utc", "kafka_offset")
    obs = timed.groupBy("link_id").agg(F.max_by(F.struct(*LATEST_KEEP), order).alias("o"))
    val = timed.filter("is_valid").groupBy("link_id").agg(F.max_by(F.struct(*LATEST_KEEP), order).alias("v"))
    return (obs.join(val, "link_id", "left")
               .select("link_id",
                       F.col("v.event_time_utc").alias("valid_event_time"), F.col("v.data_as_of").alias("valid_data_as_of"),
                       F.col("v.speed").alias("speed"), F.col("v.travel_time").alias("travel_time"),
                       F.col("v.kafka_partition").alias("valid_kafka_partition"), F.col("v.kafka_offset").alias("valid_kafka_offset"),
                       F.col("v.kafka_ts").alias("valid_kafka_ts"),
                       F.col("o.event_time_utc").alias("obs_event_time"), F.col("o.data_as_of").alias("obs_data_as_of"),
                       F.col("o.speed").alias("obs_speed"), F.col("o.travel_time").alias("obs_travel_time"),
                       F.col("o.is_valid").alias("obs_is_valid"), F.col("o.invalid_reason").alias("obs_invalid_reason"),
                       F.col("o.kafka_partition").alias("obs_kafka_partition"), F.col("o.kafka_offset").alias("obs_kafka_offset"),
                       F.col("o.kafka_ts").alias("obs_kafka_ts"), F.col("o.run_id").alias("run_id")))


VALID_COLS = ["valid_event_time", "valid_data_as_of", "speed", "travel_time", "valid_kafka_partition",
              "valid_kafka_offset", "valid_kafka_ts"]
OBS_COLS = ["obs_event_time", "obs_data_as_of", "obs_speed", "obs_travel_time", "obs_is_valid", "obs_invalid_reason",
            "obs_kafka_partition", "obs_kafka_offset", "obs_kafka_ts", "run_id"]
NEWER_VALID = ("s.valid_event_time IS NOT NULL AND (t.valid_event_time IS NULL OR s.valid_event_time > t.valid_event_time "
               "OR (s.valid_event_time = t.valid_event_time AND s.valid_kafka_offset > t.valid_kafka_offset))")
NEWER_OBS = ("t.obs_event_time IS NULL OR s.obs_event_time > t.obs_event_time "
             "OR (s.obs_event_time = t.obs_event_time AND s.obs_kafka_offset > t.obs_kafka_offset)")


def merge_sql(view: str) -> str:
    sets = [f"{c} = CASE WHEN {NEWER_VALID} THEN s.{c} ELSE t.{c} END" for c in VALID_COLS]
    sets += [f"{c} = CASE WHEN {NEWER_OBS} THEN s.{c} ELSE t.{c} END" for c in OBS_COLS]
    sets += ["batch_id = s.batch_id", "updated_at = s.updated_at"]
    return (f"MERGE INTO {config.SPEED_TRAFFIC_LATEST} t USING {view} s ON t.link_id = s.link_id "
            f"WHEN MATCHED AND (({NEWER_VALID}) OR ({NEWER_OBS})) THEN UPDATE SET {', '.join(sets)} "
            f"WHEN NOT MATCHED THEN INSERT *")


def make_latest_sink(ops: OpsLog):
    def sink(batch: DataFrame, batch_id: int):
        spark = batch.sparkSession
        b = batch.persist()
        s = b.agg(F.count("*").alias("n"), F.sum(F.col("is_valid").cast("int")).alias("v"),
                  F.sum(F.col("dst_ambiguous").cast("int")).alias("amb"),
                  F.sum(F.col("dst_nonexistent").cast("int")).alias("nonex"),
                  F.sum(F.col("local_ts").isNull().cast("int")).alias("unparse"),
                  F.min("event_time_utc").alias("mn"), F.max("event_time_utc").alias("mx"),
                  F.min("kafka_ts").alias("k_min"), F.expr("percentile_approx(kafka_ts, 0.05)").alias("k_p05"),
                  F.expr("percentile_approx(kafka_ts, 0.5)").alias("k_p50"), F.max("kafka_ts").alias("k_max")).collect()[0]
        if not s["n"]:
            b.unpersist(); return
        reasons = {r["invalid_reason"]: int(r["count"]) for r in
                   b.filter(F.col("invalid_reason").isNotNull()).groupBy("invalid_reason").count().collect()}
        src = (latest_source(b).withColumn("batch_id", F.lit(batch_id).cast("bigint"))
                               .withColumn("updated_at", F.current_timestamp()))
        view = f"speed_latest_src_{batch_id}"
        src.createOrReplaceTempView(view)
        t0 = time.time()
        spark.sql(merge_sql(view))                       # Iceberg tự retry khi xung đột commit (commit.retry.num-retries)
        merge_ms = int((time.time() - t0) * 1000)
        done = time.time()
        spark.catalog.dropTempView(view)
        ops.write("speed_batch_stats", {"query": "speed_latest", "batch_id": batch_id, "n_rows": s["n"], "n_valid": s["v"],
                                        "n_dst_ambiguous": s["amb"], "n_dst_nonexistent": s["nonex"], "n_unparseable": s["unparse"],
                                        "invalid_reasons": reasons, "min_event_time": s["mn"], "max_event_time": s["mx"]})
        ops.write("speed_latency", {"query": "speed_latest", "batch_id": batch_id, "n_items": s["n"], "merge_ms": merge_ms,
                                    "kafka_ts_min": s["k_min"], "kafka_ts_p05": s["k_p05"], "kafka_ts_p50": s["k_p50"],
                                    "kafka_ts_max": s["k_max"], "done_at_epoch": done})
        b.unpersist()
    return sink


# ------------------------------------------------------------------ quan sát (benchmark)
class ProgressListener(StreamingQueryListener):
    def __init__(self, ops: OpsLog):
        self.ops = ops

    def onQueryStarted(self, event):
        print(f"[speed] start {event.name} id={event.id} runId={event.runId}")

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        st = p.get("stateOperators") or []
        src = (p.get("sources") or [{}])[0].get("metrics") or {}
        dur = p.get("durationMs") or {}
        sink = (p.get("sink") or {})
        self.ops.write("speed_progress", {
            "query": p.get("name") or "", "query_id": p.get("id"), "run_id": p.get("runId"), "batch_id": p.get("batchId"),
            "ts": p.get("timestamp"), "num_input_rows": p.get("numInputRows", 0),
            "input_rows_per_sec": p.get("inputRowsPerSecond"), "processed_rows_per_sec": p.get("processedRowsPerSecond"),
            "trigger_execution_ms": dur.get("triggerExecution", 0), "add_batch_ms": dur.get("addBatch", 0),
            "commit_offsets_ms": dur.get("commitOffsets", 0), "watermark": (p.get("eventTime") or {}).get("watermark", ""),
            "state_rows": sum(int(s.get("numRowsTotal", 0)) for s in st),
            "state_memory_bytes": sum(int(s.get("memoryUsedBytes", 0)) for s in st),
            "rows_dropped_by_watermark": sum(int(s.get("numRowsDroppedByWatermark", 0)) for s in st),
            "max_offsets_behind": src.get("maxOffsetsBehindLatest"), "avg_offsets_behind": src.get("avgOffsetsBehindLatest"),
            "sink_rows": sink.get("numOutputRows")})

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        print(f"[speed] stop id={event.id} exception={event.exception}")


# ------------------------------------------------------------------ main
def start_iceberg(df: DataFrame, name: str, checkpoint: str, table: str, args):
    w = (df.writeStream.queryName(name).format("iceberg").outputMode("append")
           .option("checkpointLocation", checkpoint).option("fanout-enabled", "true"))
    w = w.trigger(availableNow=True) if args.available_now else w.trigger(processingTime=args.trigger)
    return w.toTable(table)


def start_foreach(df: DataFrame, name: str, checkpoint: str, sink, args):
    w = df.writeStream.queryName(name).outputMode("append").option("checkpointLocation", checkpoint).foreachBatch(sink)
    w = w.trigger(availableNow=True) if args.available_now else w.trigger(processingTime=args.trigger)
    return w.start()


def main():
    ap = argparse.ArgumentParser(description="Speed layer (Spark Structured Streaming -> Iceberg)")
    ap.add_argument("--queries", default="latest,5min,weather")
    ap.add_argument("--trigger", default=config.SPEED_TRIGGER)
    ap.add_argument("--watermark", default=config.SPEED_WATERMARK)
    ap.add_argument("--starting-offsets", default=config.SPEED_STARTING_OFFSETS,
                    help="chỉ dùng ở lần chạy đầu của mỗi checkpoint (earliest | latest | JSON offset)")
    ap.add_argument("--max-offsets", type=int, default=config.SPEED_MAX_OFFSETS_PER_TRIGGER)
    ap.add_argument("--checkpoint-root", default=None, help="đổi để chạy thử nghiệm độc lập (mặc định config.CHECKPOINT_SPEED_*)")
    ap.add_argument("--available-now", action="store_true", help="xử lý hết dữ liệu đang có rồi dừng")
    ap.add_argument("--run-seconds", type=int, default=0, help="dừng sau N giây (kiểm thử/benchmark)")
    ap.add_argument("--ops-log-dir", default=config.OPS_LOG_DIR)
    args = ap.parse_args()

    conf = {"spark.sql.shuffle.partitions": config.SPEED_SHUFFLE_PARTITIONS}
    if os.getenv("SPEED_STATE_STORE", "rocksdb") == "rocksdb":
        conf["spark.sql.streaming.stateStore.providerClass"] = \
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    spark = build_spark("speed_layer", conf)
    ops = OpsLog(args.ops_log_dir)
    spark.streams.addListener(ProgressListener(ops))
    ck = {"latest": config.CHECKPOINT_SPEED_LATEST, "5min": config.CHECKPOINT_SPEED_5MIN, "weather": config.CHECKPOINT_SPEED_WEATHER}
    if args.checkpoint_root:
        ck = {q: f"{args.checkpoint_root}/speed_{q}" for q in ck}
    wanted = [q.strip() for q in args.queries.split(",") if q.strip()]
    traffic = lambda: parse_traffic(read_kafka(spark, config.TOPIC_TRAFFIC, args.starting_offsets, args.max_offsets))
    queries = []
    if "latest" in wanted:
        queries.append(start_foreach(traffic(), "speed_latest", ck["latest"], make_latest_sink(ops), args))
    if "5min" in wanted:
        queries.append(start_iceberg(window_5min(traffic(), args.watermark), "speed_5min", ck["5min"], config.SPEED_TRAFFIC_5MIN, args))
    if "weather" in wanted:
        w = parse_weather(read_kafka(spark, config.TOPIC_WEATHER, args.starting_offsets, args.max_offsets))
        queries.append(start_iceberg(conform(w, SPEED_WEATHER_OBS_FIELDS), "speed_weather", ck["weather"], config.SPEED_WEATHER_OBS, args))

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
