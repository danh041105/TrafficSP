# Kiểm tra các hàm trong common/data_cleaning.py bằng dữ liệu tự tạo.
# Chạy: docker exec -it traffic-airflow-scheduler spark-submit /opt/airflow/jobs/tests/test_data_cleaning.py
from pyspark.sql import functions as F

from common.spark_session import build_spark
from common.data_cleaning import (cast_traffic, enriched_traffic_time, drop_null_keys, drop_invalid_status, dst_hours,
                                  dst_reason, drop_dst_duplicated_hours, deduplicate, parse_weather_time)
from batch.transformation.bronze_to_silver import traffic_in_scope, traffic_quarantine

spark = build_spark("test_data_cleaning")

# ----------------- Giờ đổi DST -----------------
assert dst_hours(2025) == [("2025-03-09", 2, "dst_nonexistent"), ("2025-11-02", 1, "dst_duplicated")], dst_hours(2025)
assert dst_hours(2026) == [("2026-03-08", 2, "dst_nonexistent"), ("2026-11-01", 1, "dst_duplicated")], dst_hours(2026)
print("DST_HOURS: OK")

# Chỉ loại -101; NULL và mã khác không bị loại theo quy tắc ba giá trị SQL.
statuses = spark.createDataFrame([(1, -101), (2, 0), (3, None), (4, 7)], "id int, status int")
assert {r.id for r in drop_invalid_status(statuses).collect()} == {2, 3, 4}
print("STATUS_FILTER: OK")

# Offset của partition khác không tạo thứ tự toàn cục.
versions = spark.createDataFrame([
    ("k", 0, 999, "2025-01-01 00:00:00", "old"),
    ("k", 1, 1, "2025-01-01 01:00:00", "new"),
], ["key", "kafka_partition", "kafka_offset", "ingest_ts", "value"])
versions = versions.withColumn("ingest_ts", F.to_timestamp("ingest_ts"))
assert deduplicate(versions, ["key"]).first()["value"] == "new"
print("CROSS_PARTITION_DEDUP: OK")

# ----------------- Traffic -----------------
cols = ["id", "link_id", "status", "speed", "travel_time", "data_as_of", "kafka_partition", "kafka_offset"]
rows = [
    ("1", "100", "0", 40.0, 60, "2025-01-15T08:00:00", 0, 1),   # A: mùa đông, NY = UTC-5
    ("1", "100", "0", 40.0, 60, "2025-07-15T08:00:00", 0, 2),   # B: mùa hè,  NY = UTC-4
    ("1", "100", "0", 40.0, 60, "2025-01-31T22:00:00", 0, 3),   # C: UTC đã sang 01/02, date vẫn 31/01
    ("1", "100", "0", 40.0, 60, "2025-11-02T01:30:00", 0, 4),   # D: giờ lặp DST -> bị loại
    ("1", "100", "0", 40.0, 60, "2025-11-02T02:30:00", 0, 5),   # E: ngay sau giờ lặp -> giữ
    ("1", "abc", "0", 40.0, 60, "2025-01-15T09:00:00", 0, 6),   # F: link_id hỏng -> bị loại
    ("1", "100", "0", 40.0, 60, "khong-phai-gio",      0, 7),   # G: thời gian hỏng -> bị loại
    ("1", "100", "0", 40.0, 60, "2025-03-09T02:30:00", 0, 8),   # I: giờ không tồn tại DST -> bị loại
    ("1", "100", "0", 40.0, 60, "2025-03-09T03:30:00", 0, 9),   # J: ngay sau giờ không tồn tại -> giữ
    ("1", "200", "0", 42.2, 60, "2025-03-10T12:00:00", 1, 10),  # H1: bản đầu
    ("1", "200", "0", 42.2, 60, "2025-03-10T12:00:00", 1, 10),  # H2: đọc lại Kafka (trùng y hệt)
    ("1", "200", "0", 38.5, 60, "2025-03-10T12:00:00", 1, 25),  # H3: bản sửa -> phải được giữ
]
parsed = enriched_traffic_time(cast_traffic(spark.createDataFrame(rows, cols)))

# dst_reason: chỉ D và I có nhãn, các dòng khác null
reasons = {r["kafka_offset"]: r["reason"]
           for r in parsed.withColumn("reason", dst_reason()).collect()}
assert reasons[4] == "dst_duplicated" and reasons[8] == "dst_nonexistent", reasons
assert all(v is None for k, v in reasons.items() if k not in (4, 8)), reasons
print("DST_REASON: OK")

df = drop_null_keys(parsed, ["link_id", "traffic_utc"])
df = drop_dst_duplicated_hours(df)
df = deduplicate(df, ["link_id", "traffic_utc"])
df.orderBy("kafka_offset").show(truncate=False)

# offset -> (traffic_utc, date, hour, day_of_week, speed). dayofweek: 1 = Chủ nhật
expected = {
    1:  ("2025-01-15 13:00:00", "2025-01-15", 8, 4, 40.0),
    2:  ("2025-07-15 12:00:00", "2025-07-15", 8, 3, 40.0),
    3:  ("2025-02-01 03:00:00", "2025-01-31", 22, 6, 40.0),
    5:  ("2025-11-02 07:30:00", "2025-11-02", 2, 1, 40.0),
    9:  ("2025-03-09 07:30:00", "2025-03-09", 3, 1, 40.0),
    25: ("2025-03-10 16:00:00", "2025-03-10", 12, 2, 38.5),
}
# Đổi timestamp sang chuỗi trong Spark (theo session UTC) để so sánh không lệ thuộc múi giờ máy
actual = {
    r["kafka_offset"]: (r["utc"], r["d"], r["hour"], r["day_of_week"], r["speed"])
    for r in df.select("kafka_offset",
                       F.date_format("traffic_utc", "yyyy-MM-dd HH:mm:ss").alias("utc"),
                       F.date_format("date", "yyyy-MM-dd").alias("d"),
                       "hour", "day_of_week", "speed").collect()
}

assert set(actual) == set(expected), f"Sai số dòng giữ lại: {sorted(actual)}"
for off, exp in expected.items():
    assert actual[off] == exp, f"offset {off}: mong đợi {exp}, nhận {actual[off]}"
assert "data_as_of" not in df.columns, "data_as_of chưa bị bỏ"
assert dict(df.dtypes)["link_id"] == "bigint", "link_id chưa ép sang bigint"
assert parsed.filter(F.col("kafka_offset") == 7).first()["season"] is None
assert parsed.filter(F.col("kafka_offset") == 1).first()["season"] == "Winter"
assert parsed.filter(F.col("kafka_offset") == 2).first()["season"] == "Summer"
print("TRAFFIC: OK")

# Lỗi timestamp vẫn đến quarantine và replay cùng Kafka record không sinh trùng.
scope_rows = [
    ("1", "100", "0", 40.0, 60, "2025-01-15T08:00:00", 0, 101),
    ("1", "100", "0", 40.0, 60, "2025-01-15Tbad", 0, 102),
    ("1", "100", "0", 40.0, 60, "2025-01-15Tbad", 0, 102),
    ("1", "100", "0", 40.0, 60, "bad-time", 0, 103),
    ("1", "100", "0", 40.0, 60, "2025-01-16T08:00:00", 0, 104),
    ("1", "100", "0", 40.0, 60, "2025-01-16Tbad", 0, 105),
    ("1", None, "0", 40.0, 60, "2025-01-15T09:00:00", 0, 106),
]
scoped = traffic_in_scope(spark.createDataFrame(scope_rows, cols), "2025-01-15", "2025-01-15")
assert {r.kafka_offset for r in scoped.select("kafka_offset").collect()} == {101, 102, 103, 106}
quarantined = traffic_quarantine(scoped, (2025,))
q_rows = quarantined.select("kafka_offset", "date", "reason").collect()
assert len(q_rows) == 3, q_rows
assert {r.kafka_offset for r in q_rows} == {102, 103, 106}, q_rows
assert all(r.reason == "missing_key" for r in q_rows), q_rows
assert next(r.date for r in q_rows if r.kafka_offset == 102).isoformat() == "2025-01-15"
assert next(r.date for r in q_rows if r.kafka_offset == 103) is None
print("QUARANTINE_SCOPE_AND_DEDUP: OK")

# ----------------- Weather -----------------
w = parse_weather_time(spark.createDataFrame([("40.8787_-73.9972", "2025-10-01T04:00")],
                                             ["cell_id", "time"]))
w.show(truncate=False)
assert "time" not in w.columns and dict(w.dtypes)["weather_utc"] == "timestamp"
assert w.select(F.date_format("weather_utc", "yyyy-MM-dd HH:mm").alias("t")).first()["t"] == "2025-10-01 04:00"
print("WEATHER: OK")

spark.stop()
