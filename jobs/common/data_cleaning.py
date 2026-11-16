# Các hàm làm sạch dùng chung và riêng (traffic và weather) cho Bronze -> Silver 
# Mỗi hàm nhận DataFrame, trả DataFrame
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Múi giờ của New York
TRAFFIC_TIMEZONE = "America/New_York"

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

# Bỏ dòng thiếu cột khóa như link_id, traffic_utc, link_points, borough
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

def parsed_traffic_time(df: DataFrame) -> DataFrame:
    local_ts = F.to_timestamp("data_as_of", "yyyy-MM-dd'T'HH:mm:ss")
    new_df = (df
            .withColumn("_local_ts", local_ts)
            .withColumn("traffic_utc", F.to_utc_timestamp("_local_ts", TRAFFIC_TIMEZONE))
            .withColumn("date", F.to_date("_local_ts"))
            .withColumn("hour", F.hour("_local_ts"))
            .withColumn("day_of_week", F.dayofweek("_local_ts"))
            .drop("_local_ts", "data_as_of")
    )
    return new_df


# Bỏ giờ lặp 01:00-01:59 ngày 02/11/2025 (giờ New York) khi hết giờ mùa hè
def drop_dst_repeated_hours(df: DataFrame) -> DataFrame:
    new_df = df.filter(~((F.col("date") == "2025-11-02") & (F.col("hour") == 1)))
    return new_df

# ----------------- Weather -----------------
# Chuyển giờ và tên cột của weather

def parse_weather_time(df: DataFrame) -> DataFrame:
   new_df = (df
            .withColumn("time", F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm"))
            .withColumnRenamed("time", "weather_utc")
    )
   return new_df