from pyspark.sql import DataFrame, functions as F
# Cửa sổ 5 phút căn theo epoch UTC giống F.window(ts, "5 minutes") của Structured Streaming.

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

def with_avg(df: DataFrame) -> DataFrame:
    return df.withColumn("avg_speed", F.when(F.col("n_valid") > 0, F.col("sum_speed") / F.col("n_valid")))