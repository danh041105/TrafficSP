# đọc hai topic Kafka, tách JSON thành cột, thêm 4 cột metadata rồi ghi vào hai bảng Bronze vừa tạo
import argparse
from pyspark.sql import DataFrame, SparkSession, functions as F
from common.config import (KAFKA_BOOTSTRAP, TOPIC_TRAFFIC, TOPIC_WEATHER, MAX_OFFSETS_PER_TRIGGER,
                           BRONZE_TRAFFIC_TABLE, BRONZE_WEATHER_TABLE,
                           CHECKPOINT_TRAFFIC, CHECKPOINT_WEATHER, TRIGGER_INTERVAL)
from common.schemas import TRAFFIC_FIELDS, WEATHER_FIELDS, METADATA_FIELDS, field_names, to_ddl
from common.spark_session import build_spark

# Đọc Kafka
# KAFKA_BOOTSTRAP đọc từ biến môi trường.
# startingOffsets = earliest đọc theo offset cũ nhất với mỗi topic. Những lần sau Spark chỉ đọc vị trí đã dừng trong checkpoint.
# Nhờ vậy GĐ1 và GĐ2 dùng chung checkpoint mà không đọc trùng.
# maxOffsetsPerTrigger giới hạn mỗi micro-batch. Tránh việc để Spark đọc hết 1 lần cả 9 tháng ~ 9tr bản ghi. 
def read_kafka(spark: SparkSession, topic: str) -> DataFrame:
    df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
                .load()
        )
    return df

# chuyển dữ liệu thô đọc từ Kafka thành DataFrame để lưu vào tầng Bronze của Data Lakehouse.
# chuyển từ json --> dataframe sẽ không vi phạm nguyên tắc as-is với lớp bronze trong kiến trúc medallion đâu nhé :D

def to_bronze(raw: DataFrame, fields: dict, time_col: str) -> DataFrame:
    # raw: dataframe đọc từ kafka, fields: tên + kiểu dữ liệu của các trường trong json
    # time_col: tên cột chứa thời gian sự kiện (traffic ~ data_as_of) và (weather ~ time)

    # Kafka trả về các cột như: key, value, topic, partition, offset, timestamp...
    # Trong đó, value chứa nội dung JSON dưới dạng binary.
    # quy trình parsed sẽ là: chuyển value về string sau đó apply với schemas bronze đã có
    parsed = raw.select(
            F.from_json(F.col("value").cast("string"), to_ddl(fields)).alias("raw_df"),
            # Giữ metadata Kafka để truy vết nguồn gốc bản ghi
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
    )
    # time_col là "data_as_of" với traffic và "time" với weather. Lấy 7 ký tự đầu sẽ được "2025-01".
    df = (parsed.select("raw_df.*", "kafka_partition", "kafka_offset")
            .withColumn("event_month", F.substring(time_col, 1, 7))          
            .withColumn("ingest_ts", F.current_timestamp())
    )
    # Sắp xếp cột đúng thứ tự trong bảng: payload trước, metadata sau
    return df.select(*field_names(fields), *field_names(METADATA_FIELDS))

def start_query(df: DataFrame, table: str, checkpoint: str, name: str, mode: str):
    writer = (df.writeStream.format("iceberg")
              .outputMode("append")
              .queryName(name)
              .option("checkpointLocation", checkpoint)
              .option("fanout-enabled", "true"))
    # fanout-enabled cho phép một micro-batch ghi vào nhiều partition cùng lúc mà Spark không phải sắp xếp dữ liệu trước. 
    # Trường hợp này xảy ra khi một batch 500k message nằm vắt qua ranh giới hai tháng.
    if mode == "once":
        writer = writer.trigger(availableNow=True)          # GĐ1: đọc hết rồi tự dừng
    else:
        writer = writer.trigger(processingTime=TRIGGER_INTERVAL)   # GĐ2: chạy mãi, 30 s/lần
    return writer.toTable(table)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "continuous"], default="continuous")
    args = parser.parse_args()

    spark = build_spark("stream_to_bronze")
    queries = [
        start_query(to_bronze(read_kafka(spark, TOPIC_TRAFFIC), TRAFFIC_FIELDS, "data_as_of"),
                    BRONZE_TRAFFIC_TABLE, CHECKPOINT_TRAFFIC, "traffic_to_bronze", args.mode),
        start_query(to_bronze(read_kafka(spark, TOPIC_WEATHER), WEATHER_FIELDS, "time"),
                    BRONZE_WEATHER_TABLE, CHECKPOINT_WEATHER, "weather_to_bronze", args.mode),
    ]
    for q in queries:
        q.awaitTermination()   # chờ cả hai; ở chế độ once, mỗi query tự dừng khi đọc hết
        
if __name__ == "__main__":
    main()