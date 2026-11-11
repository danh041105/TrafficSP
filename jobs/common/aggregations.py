# aggregations.py: công thức gom traffic dùng CHUNG cho Batch View (Gold) và đối chiếu với Speed Layer.
# Cửa sổ 5 phút căn theo epoch UTC giống F.window(ts, "5 minutes") của Structured Streaming.
# Roll-up: chỉ cộng thành phần (n_*, sum_*), MIN/MAX cho min/max; avg luôn tính lại = sum_speed / n_valid.
from pyspark.sql import DataFrame, functions as F

WINDOW_SECONDS = 300


def floor_ts(col, seconds: int):
    return F.timestamp_seconds(F.floor(F.unix_timestamp(col) / seconds) * seconds)


def measures(valid_col="is_valid", speed_col="speed", tt_col="travel_time"):
    v, s, t = F.col(valid_col), F.col(speed_col), F.col(tt_col)
    return [F.count("*").alias("n_records"),
            F.sum(v.cast("int")).alias("n_valid"),
            F.sum((s == 0).cast("int")).alias("n_zero"),
            F.sum((~v & (s != 0)).cast("int")).alias("n_invalid_other"),
            F.coalesce(F.sum(F.when(v, s)), F.lit(0.0)).alias("sum_speed"),
            F.coalesce(F.sum(F.when(v, s * s)), F.lit(0.0)).alias("sum_speed_sq"),
            F.coalesce(F.sum(F.when(v, t)), F.lit(0)).cast("bigint").alias("sum_travel_time"),
            F.min(F.when(v, s)).alias("min_speed"),
            F.max(F.when(v, s)).alias("max_speed")]


def aggregate_5min(df: DataFrame, ts_col="event_time_utc") -> DataFrame:
    """df: link_id, <ts_col> (UTC, không NULL), speed, travel_time, is_valid. Đã khử trùng theo business key."""
    return (df.filter(F.col(ts_col).isNotNull())
              .withColumn("window_start", floor_ts(F.col(ts_col), WINDOW_SECONDS))
              .groupBy("link_id", "window_start").agg(*measures())
              .withColumn("window_end", F.col("window_start") + F.expr("INTERVAL 5 MINUTES")))


def rollup_hourly(df5: DataFrame) -> DataFrame:
    """Gộp 5 phút -> 1 giờ chỉ bằng thành phần cộng được."""
    return (df5.withColumn("hour_utc", floor_ts(F.col("window_start"), 3600))
               .groupBy("link_id", "hour_utc")
               .agg(*[F.sum(c).alias(c) for c in ("n_records", "n_valid", "n_zero", "n_invalid_other", "sum_speed", "sum_speed_sq")],
                    F.sum("sum_travel_time").cast("bigint").alias("sum_travel_time"),
                    F.min("min_speed").alias("min_speed"), F.max("max_speed").alias("max_speed")))


def with_avg(df: DataFrame) -> DataFrame:
    return df.withColumn("avg_speed", F.when(F.col("n_valid") > 0, F.col("sum_speed") / F.col("n_valid")))
