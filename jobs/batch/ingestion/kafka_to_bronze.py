from pyspark.sql import DataFrame, functions as F
from common.config import (TOPIC_TRAFFIC, TOPIC_WEATHER,
                           BRONZE_TRAFFIC_TABLE, BRONZE_WEATHER_TABLE,
                           CHECKPOINT_TRAFFIC, CHECKPOINT_WEATHER)
from common.schemas import TRAFFIC_FIELDS, WEATHER_FIELDS, METADATA_FIELDS, field_names
from common.spark_session import build_spark
from common.read_kafka import read_kafka, parse_kafka_json
from common.logger import get_logger

log = get_logger("kafka_to_bronze")

# chuyển dữ liệu thô đọc từ Kafka thành DataFrame để lưu vào tầng Bronze của Data Lakehouse.
# chuyển từ json --> dataframe sẽ không vi phạm nguyên tắc as-is với lớp bronze trong kiến trúc medallion đâu nhé :D

def to_bronze(raw: DataFrame, fields: dict, time_col: str) -> DataFrame:
    # time_col là "data_as_of" với traffic và "time" với weather. Lấy 7 ký tự đầu sẽ được "2025-01".
    df = (parse_kafka_json(raw, fields)
            .withColumn("event_month", F.substring(time_col, 1, 7))
            .withColumn("ingest_ts", F.current_timestamp()))
    return df.select(*field_names(fields), *field_names(METADATA_FIELDS))

def start_query(df: DataFrame, table: str, checkpoint: str, name: str):
    # availableNow: đọc hết các offset đang có trong Kafka tại lúc khởi động, chia theo maxOffsetsPerTrigger.
    # Bản chất đây là một job batch, chỉ mượn API streaming để có checkpoint offset.
    # checkpoint offset cho phép lưu lại lần cuối cùng đọc giúp spark không phải đọc lại toàn bộ topic mỗi lần chạy
    # fanout-enabled cho phép một micro-batch ghi vào nhiều partition cùng lúc mà Spark không phải sắp xếp dữ liệu trước. 
    # Trường hợp này xảy ra khi một batch 500k message nằm vắt qua ranh giới hai tháng.
    return (df.writeStream.format("iceberg")
              .outputMode("append")
              .queryName(name)
              .option("checkpointLocation", checkpoint)
              .option("fanout-enabled", "true")
              .trigger(availableNow=True)
              .toTable(table))

def main():
    spark = build_spark("kafka_to_bronze_batch")
    queries = [
        start_query(to_bronze(read_kafka(spark, TOPIC_TRAFFIC), TRAFFIC_FIELDS, "data_as_of"),
                    BRONZE_TRAFFIC_TABLE, CHECKPOINT_TRAFFIC, "traffic_to_bronze"),
        start_query(to_bronze(read_kafka(spark, TOPIC_WEATHER), WEATHER_FIELDS, "time"),
                    BRONZE_WEATHER_TABLE, CHECKPOINT_WEATHER, "weather_to_bronze"),
    ]
    for q in queries:
        q.awaitTermination()   # mỗi query tự dừng khi đọc hết offset có tại lúc khởi động
        n_rows = sum(p["numInputRows"] for p in q.recentProgress)
        log.info("[%s] đã ghi %d dòng vào Bronze qua %d micro-batch",
            q.name, n_rows, len(q.recentProgress))

if __name__ == "__main__":
    main()