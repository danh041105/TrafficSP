# road_link.py: tra road_link_key (khoá thay thế SCD2 của gold.dim_road_link, mô hình sao gold-dev) theo link_id + NGÀY New York.
# Dùng CHUNG cho Batch View (fact_traffic_5min / fact_traffic_hourly), Speed Layer (speed.traffic_5min_rt) và bảng fact của
# gold-dev (fact_traffic_reading, fact_congestion_hourly) -> mọi bảng fact cùng một quy tắc as-of:
#   phiên bản có  valid_from <= ngày_NY(ts)  và  (valid_to IS NULL  hoặc  ngày_NY(ts) < valid_to).
# dim_road_link.link_id là STRING (gold.sql), Silver/Speed là BIGINT -> join bằng CAST. Không có phiên bản phù hợp -> NULL.
from pyspark.sql import DataFrame, SparkSession, functions as F

from common import config


def ny_date(ts_col: str):
    return F.to_date(F.from_utc_timestamp(F.col(ts_col), config.CALENDAR_TIMEZONE))


def dim_versions(spark: SparkSession) -> DataFrame:
    return (spark.table(config.GOLD_DIM_ROAD_LINK)
                 .select(F.col("link_id").cast("bigint").alias("_rl_link_id"), F.col("road_link_key").alias("_rl_key"),
                         F.col("valid_from").alias("_rl_from"), F.col("valid_to").alias("_rl_to")))


def with_road_link_key(df: DataFrame, spark: SparkSession, ts_col: str, link_col: str = "link_id") -> DataFrame:
    """Thêm cột road_link_key (int) theo phiên bản dim hiệu lực tại ngày New York của ts_col. Left join: không mất dòng.
    Dùng được cho cả DataFrame streaming (stream-static join; bảng dim đọc như dữ liệu tĩnh)."""
    d = ny_date(ts_col)
    v = dim_versions(spark)
    cond = ((F.col(link_col) == F.col("_rl_link_id")) & (d >= F.col("_rl_from"))
            & (F.col("_rl_to").isNull() | (d < F.col("_rl_to"))))
    return (df.join(F.broadcast(v), cond, "left")
              .withColumn("road_link_key", F.col("_rl_key").cast("int"))
              .drop("_rl_link_id", "_rl_key", "_rl_from", "_rl_to"))
