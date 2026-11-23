# aggregations.py: công thức gom traffic dùng CHUNG cho Batch View (Gold) và đối chiếu với Speed Layer.
# Cửa sổ 5 phút căn theo epoch UTC giống F.window(ts, "5 minutes") của Structured Streaming.
# Chỉ lưu thành phần CỘNG ĐƯỢC (n_*, sum_*): gộp lên giờ/ngày/borough hay ghép batch + speed chỉ cần cộng.
# Cột suy ra được KHÔNG lưu, view tính khi đọc: avg_speed_mph = sum_speed_mph / n_valid,
# n_invalid_other = n_records - n_valid - n_zero, window_end_utc = window_start_utc + 5 phút, quality_flag (n_valid = 0).
# Tên cột theo quy ước gold-dev (đơn vị trong tên: _mph, _sec); khoá lịch date_key / hour_key theo giờ New York.
from pyspark.sql import DataFrame, functions as F

WINDOW_SECONDS = 300


def floor_ts(col, seconds: int):
    return F.timestamp_seconds(F.floor(F.unix_timestamp(col) / seconds) * seconds)


def measures(valid_col="is_valid", speed_col="speed", tt_col="travel_time"):
    v, s, t = F.col(valid_col), F.col(speed_col), F.col(tt_col)
    return [F.count("*").alias("n_records"),
            F.sum(v.cast("int")).alias("n_valid"),
            F.sum((s == 0).cast("int")).alias("n_zero"),
            F.coalesce(F.sum(F.when(v, s)), F.lit(0.0)).alias("sum_speed_mph"),
            F.coalesce(F.sum(F.when(v, s * s)), F.lit(0.0)).alias("sum_speed_sq"),
            F.coalesce(F.sum(F.when(v, t)), F.lit(0)).cast("bigint").alias("sum_travel_time_sec")]


def with_calendar_keys(df: DataFrame, ts_col: str, tz: str = "America/New_York") -> DataFrame:
    """date_key = YYYYMMDD và hour_key = 0-23 của thời điểm ts_col (UTC) đổi sang giờ New York (như dim_date / dim_hour).
    Cần spark.sql.session.timeZone = UTC (spark_session.build_spark đã đặt)."""
    local = F.from_utc_timestamp(F.col(ts_col), tz)
    return (df.withColumn("date_key", F.date_format(local, "yyyyMMdd").cast("int"))
              .withColumn("hour_key", F.hour(local)))


def aggregate_5min(df: DataFrame, ts_col="event_time_utc") -> DataFrame:
    """df: link_id, <ts_col> (UTC, không NULL), speed, travel_time, is_valid. Đã khử trùng theo business key."""
    return (df.filter(F.col(ts_col).isNotNull())
              .withColumn("window_start", floor_ts(F.col(ts_col), WINDOW_SECONDS))
              .groupBy("link_id", "window_start").agg(*measures()))


def rollup_hourly(df5: DataFrame) -> DataFrame:
    """Gộp 5 phút -> 1 giờ chỉ bằng thành phần cộng được."""
    return (df5.withColumn("hour_start_utc", floor_ts(F.col("window_start"), 3600))
               .groupBy("link_id", "hour_start_utc")
               .agg(*[F.sum(c).alias(c) for c in ("n_records", "n_valid", "n_zero", "sum_speed_mph", "sum_speed_sq")],
                    F.sum("sum_travel_time_sec").cast("bigint").alias("sum_travel_time_sec"),
                    F.count("*").cast("int").alias("n_windows")))      # số cửa sổ 5 phút có dữ liệu trong giờ


def with_avg(df: DataFrame) -> DataFrame:
    return df.withColumn("avg_speed_mph", F.when(F.col("n_valid") > 0, F.col("sum_speed_mph") / F.col("n_valid")))
