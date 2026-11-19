"""Job streaming demo cho dashboard monitoring.

Đọc topic traffic_raw từ Kafka, tính tốc độ trung bình theo link_id trong cửa sổ 1 phút,
ghi ra sink "noop" (không lưu đâu cả) -> chỉ để Spark sinh metric executor + streaming.

Chạy qua scripts/monitoring_demo/run_demo.sh, không chạy tay.
Tham số: số giây chạy (mặc định 900).
"""
import sys
import time

from pyspark.sql import SparkSession, functions as F, types as T

run_seconds = int(sys.argv[1]) if len(sys.argv) > 1 else 900

spark = (SparkSession.builder.appName("monitoring_demo")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("link_id", T.IntegerType()),
    T.StructField("speed", T.IntegerType()),
    T.StructField("seq", T.LongType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", "traffic_raw")
       .option("startingOffsets", "latest")
       .option("maxOffsetsPerTrigger", 2000)
       .load())

avg_speed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("r"), "timestamp")
             .select("r.*", "timestamp")
             .withWatermark("timestamp", "2 minutes")
             .groupBy(F.window("timestamp", "1 minute"), "link_id")
             .agg(F.avg("speed").alias("avg_speed"), F.count("*").alias("n")))

query = (avg_speed.writeStream.format("noop").outputMode("update")
         .queryName("demo_link_speed_1m")                 # tên hiện trên panel Streaming
         .trigger(processingTime="10 seconds")
         .option("checkpointLocation", "/tmp/chk_monitoring_demo")
         .start())

time.sleep(run_seconds)
query.stop()
spark.stop()
