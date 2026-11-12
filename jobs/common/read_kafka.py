from pyspark.sql import SparkSession, DataFrame, functions as F
from common.config import KAFKA_BOOTSTRAP, MAX_OFFSETS_PER_TRIGGER
from common.schemas import to_ddl
# Đọc Kafka
# KAFKA_BOOTSTRAP đọc từ biến môi trường.
# startingOffsets = earliest đọc theo offset cũ nhất với mỗi topic. Những lần sau Spark chỉ đọc vị trí đã dừng trong checkpoint.
# các lần chạy batch nối tiếp nhau dùng chung checkpoint nên không đọc trùng
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

# raw: dataframe đọc từ kafka, fields: tên + kiểu dữ liệu của các trường trong json
# Kafka trả về các cột như: key, value, topic, partition, offset, timestamp...
# Trong đó, value chứa nội dung JSON dưới dạng binary.
# quy trình parsed sẽ là: chuyển value về string sau đó apply với schemas bronze đã có
def parse_kafka_json(raw: DataFrame, fields: dict) -> DataFrame:
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), to_ddl(fields)).alias("payload"),
        # Giữ metadata Kafka để truy vết nguồn gốc bản ghi
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset")
    )
    new_df = parsed.select("payload.*", "kafka_partition", "kafka_offset")
    return new_df