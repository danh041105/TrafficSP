# Các hàm làm sạch dùng chung và riêng (traffic và weather) cho Bronze -> Silver 
# Mỗi hàm nhận DataFrame, trả DataFrame
from pyspark.sql import DataFrame, Window, Column
from pyspark.sql import functions as F
from common.config import TRAFFIC_TIMEZONE
from zoneinfo import ZoneInfo
from functools import lru_cache
from datetime import datetime, timedelta

# Bỏ trùng
def deduplicate(df: DataFrame, key: list[str]) -> DataFrame:
    # partition sẽ theo link_id và traffic_utc nhé
    """Mỗi khóa giữ 1 dòng: dòng có kafka_offset lớn nhất (bản được gửi sau cùng)."""
    window = Window.partitionBy(*key).orderBy(F.col("kafka_offset").desc())
    new_df = (df
            .withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
    )
    return new_df

# Bỏ dòng thiếu cột khóa như link_id, traffic_utc
def drop_null_keys(df: DataFrame, keys: list[str]) -> DataFrame:
    return df.dropna(subset=keys)

# ----------------- Traffic -----------------
# Chuyển kiểu dữ liệu
def cast_traffic(df: DataFrame) -> DataFrame:
    new_df = (df.withColumn("id", F.col("id").cast("bigint"))
              .withColumn("link_id", F.col("link_id").cast("bigint"))
              .withColumn("status", F.col("status").cast("int")))
    return new_df

# Cột data_as_of ("2025-01-01T00:03:03", giờ New York) 
# traffic_utc + date/hour/day_of_week theo giờ New York"""
# enrich data: từ data_as_of thêm các trường như: day_of_week, is_weekend, season
def enriched_traffic_time(df: DataFrame) -> DataFrame:
    local_ts = F.to_timestamp("data_as_of", "yyyy-MM-dd'T'HH:mm:ss")
    new_df = (df
            .withColumn("_local_ts", local_ts)
            .withColumn("traffic_utc", F.to_utc_timestamp("_local_ts", TRAFFIC_TIMEZONE))
            .withColumn("date", F.to_date("_local_ts"))
            .withColumn("hour", F.hour("_local_ts"))
            .withColumn("day_of_week", F.dayofweek("_local_ts"))
            .withColumn("is_weekend", F.dayofweek("_local_ts").isin(1, 7))
            .withColumn("season", 
                        F.when(F.month("_local_ts").isin(3, 4, 5), "Spring")
                         .when(F.month("_local_ts").isin(6, 7, 8), "Summer")
                         .when(F.month("_local_Ts").isin(9, 10, 11), "Fall")
                         .otherwise("Winter")
            )
            .drop("_local_ts", "data_as_of")
    )
    return new_df

# Ngày, giờ của New York bị lặp hoặc không tồn tại do đổi giờ do sự thay đổi múi giờ của bên mẽo (dst và est)
# ngày 09/03 có khung giờ 02:00–02:59 không tồn tại, còn ngày 02/11 có khung giờ 01:00–01:59 bị lặp.
@lru_cache(maxsize=None)
def dst_hours(year: int) -> list:
    tz = ZoneInfo(TRAFFIC_TIMEZONE)
    date = datetime(year, 1, 1)
    out = []
    while date.year == year:
        # Giờ đổi giờ là giờ cho 2 offset khác nhau khi fold=0 và fold=1
        off0 = date.replace(tzinfo=tz, fold=0).utcoffset()
        off1 = date.replace(tzinfo=tz, fold=1).utcoffset()
        if off0 != off1:
            # off0 > off1: giờ lặp. off0 < off1: giờ không tồn tại
            reason = "dst_duplicated" if off0 > off1 else "dst_nonexistent"
            out.append((date.date().isoformat(), date.hour, reason))
        date += timedelta(hours=1)
    return out

# thêm cờ reason trong bảng quarantine: dst_duplicated, dst_nonexistent, null: (không phải 2 trường hợp trên)
def dst_reason(years=(2025,)) -> Column:
    reason = F.lit(None).cast("string")
    for year in years:
        for d, h, r in dst_hours(year):
            reason = F.when((F.col("date") == d) & (F.col("hour") == h), F.lit(r)).otherwise(reason)
    return reason

# Bỏ giờ lặp 01:00-01:59 ngày 02/11/2025 (giờ New York) khi hết giờ mùa hè
def drop_dst_duplicated_hours(df: DataFrame, years=(2025,)) -> DataFrame:
    new_df = df.filter(dst_reason(years).isNull())
    return new_df
# ----------------- Weather -----------------
# Chuyển giờ và tên cột của weather

def parse_weather_time(df: DataFrame) -> DataFrame:
   new_df = (df
            .withColumn("time", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
            .withColumnRenamed("time", "weather_utc")
    )
   return new_df