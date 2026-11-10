# Các hàm làm sạch dùng chung cho Bronze -> Silver (sau này job stream cũng dùng).
# Mỗi hàm nhận DataFrame, trả DataFrame (hoặc Column). Chỉ dùng DataFrame API, không UDF.
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from common.config import TRAFFIC_TIMEZONE, DST_REPEATED_HOURS_NY


# ----------------- Chung -----------------
def deduplicate(df: DataFrame, keys: list[str]) -> DataFrame:
    """Mỗi khóa giữ 1 dòng: dòng có kafka_offset lớn nhất (bản được gửi sau cùng)."""
    w = Window.partitionBy(*keys).orderBy(F.col("kafka_offset").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


def drop_null_keys(df: DataFrame, keys: list[str]) -> DataFrame:
    """Bỏ dòng thiếu cột khóa (null do dữ liệu gốc hoặc do ép kiểu/đọc thời gian thất bại)."""
    return df.dropna(subset=keys)


# ----------------- Traffic -----------------
def cast_traffic(df: DataFrame) -> DataFrame:
    """Chuỗi -> số. Giá trị không ép được sẽ thành null, drop_null_keys xử lý sau."""
    return (df.withColumn("id", F.col("id").cast("bigint"))
              .withColumn("link_id", F.col("link_id").cast("bigint"))
              .withColumn("status", F.col("status").cast("int")))


def parse_traffic_time(df: DataFrame) -> DataFrame:
    """data_as_of ("2025-01-01T00:03:03", giờ New York) -> traffic_utc + date/hour/day_of_week theo giờ New York."""
    # Session đang ở UTC nên to_timestamp giữ nguyên "giờ trên đồng hồ" New York
    local_ts = F.to_timestamp("data_as_of", "yyyy-MM-dd'T'HH:mm:ss")
    return (df.withColumn("_local_ts", local_ts)
              .withColumn("traffic_utc", F.to_utc_timestamp("_local_ts", TRAFFIC_TIMEZONE))
              .withColumn("date", F.to_date("_local_ts"))
              .withColumn("hour", F.hour("_local_ts"))
              .withColumn("day_of_week", F.dayofweek("_local_ts"))   # 1 = Chủ nhật
              .drop("_local_ts"))


def drop_dst_repeated_hours(df: DataFrame) -> DataFrame:
    """Bỏ giờ lặp khi đổi giờ: không xác định được đó là lần thứ nhất hay thứ hai."""
    key = F.format_string("%s %02d", F.date_format("date", "yyyy-MM-dd"), F.col("hour"))
    return df.filter(~key.isin(DST_REPEATED_HOURS_NY))


def is_valid_traffic() -> Column:
    """Quy tắc bản ghi hợp lệ (đã chốt): 0 < speed <= 100 và travel_time > 0.
    Silver KHÔNG lọc theo điều kiện này; Gold dùng để đếm n_valid."""
    return F.coalesce(
        (F.col("speed") > 0) & (F.col("speed") <= 100) & (F.col("travel_time") > 0),
        F.lit(False),
    )


# ----------------- Weather -----------------
def parse_weather_time(df: DataFrame) -> DataFrame:
    """time ("2025-01-01T00:00", đã là UTC) -> weather_utc."""
    return df.withColumn("weather_utc", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))