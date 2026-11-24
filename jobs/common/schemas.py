# file này giúp định nghĩa các field sử dụng trong message Kafka dưới dạng json
# Các trường trong traffic được sử dụng

# ----------------- Bronze ----------------------
TRAFFIC_FIELDS = {
    "id": "string",
    "speed": "double",
    "travel_time": "bigint",
    "status": "string",
    "data_as_of": "string",
    "link_id": "string",
    "link_points": "string",
    "encoded_poly_line": "string",
    "encoded_poly_line_lvls": "string",
    "owner": "string",
    "transcom_id": "string",
    "borough": "string",
    "link_name": "string",
    # Các nhãn kịch bản producer ứng với 2 giai đoạn
    # 1. test: thử batch với 1 ngày
    # 2. gd1: giai đoạn 1: 01/01/2025 - 30/09/2025
    # 3. gd2: giai đoạn 2: 01/10/2025 - 31/12/2025
    "run_id": "string"
}

WEATHER_FIELDS = {
    "cell_id": "string",
    "latitude": "double",
    "longitude": "double",
    "time": "string", # giờ UTC, dạng "2025-01-01T00:00"
    "temperature_2m": "double",      # °C
    "relative_humidity_2m": "int",   # %
    "precipitation": "double",       # mm, của giờ [T-1h, T)
    "snowfall": "double",            # cm
    "wind_speed_10m": "double",      # km/h
    "wind_gusts_10m": "double",      # km/h
    "weather_code": "int",           # mã WMO
    "run_id": "string"
}

# Thêm các cột metadata cho mỗi lần ingest dữ liệu
# ingest_ts: thời điểm Spark ghi dòng đó (current_timestamp() trong job stream).
# kafka_partition và kafka_offset: kiểu int và bigint để khớp đúng kiểu mà Kafka source của Spark trả về. 
# Cặp này là duy nhất trong một topic, nên sau này bạn dùng nó để tìm bản ghi trùng.
METADATA_FIELDS = {
    "event_month": "string", # "2025-01", lấy 7 ký tự đầu của data_as_of / time
    "ingest_ts": "timestamp",
    "kafka_partition": "int",
    "kafka_offset": "bigint",
}

# ----------------- Silver ----------------------
SILVER_TRAFFIC_FIELDS = {
    "id": "bigint",
    "link_id": "bigint",
    "transcom_id": "string",
    "traffic_utc": "timestamp",       # data_as_of (giờ New York) đã đổi sang UTC
    "date": "date",                   # ngày theo giờ New York
    "hour": "int",                    # từ 0h đến 23h
    "day_of_week": "int",             # thứ trong tuần theo giờ New York
    "is_weekend": "boolean",          # có phải cuối tuần hay k ?
    "season": "string",               # Đang là mùa nào ?
    "speed": "double",                # mph
    "travel_time": "bigint",          # giây
    "status": "int",
    "encoded_poly_line": "string",
    "encoded_poly_line_lvls": "string",
    "run_id": "string",               # test / gd1 / gd2, giữ lại để truy vết
    "processed_ts": "timestamp",      # lúc job Silver ghi dòng này
}

SILVER_WEATHER_FIELDS = {
    "cell_id": "string",
    "weather_utc": "timestamp",       
    "latitude": "double",             
    "longitude": "double",
    "temperature_2m": "double",       # °C
    "relative_humidity_2m": "int",    # %
    "precipitation": "double",        # mm
    "snowfall": "double",             # cm
    "wind_speed_10m": "double",       # km/h
    "wind_gusts_10m": "double",       # km/h
    "weather_code": "int",            # mã WMO
    "run_id": "string",
    "processed_ts": "timestamp",
}

SILVER_LINK_INFO_FIELDS = {
    "link_id": "bigint",
    "link_name": "string",
    "borough": "string",
    "link_points": "string",          # giữ nguyên chuỗi gốc để sau vẽ đường trên bản đồ
    "latitude": "double",             # trung vị các điểm trong link_points
    "longitude": "double",
    "cell_id": "string",              # ô thời tiết gần nhất
    "cell_distance_km": "double",     # khoảng cách tới tâm ô, sẽ được tính bằng khoảng cách Haversine
    "processed_ts": "timestamp",
}

# Nhật ký các lần chạy job Bronze -> Silver.
# Một dòng = 1 lần chạy x 1 nguồn x 1 ngày dữ liệu x 1 kafka_partition mà lần chạy đó đọc được.
# Chỉ ghi lần chạy biến đổi dữ liệu; compaction Bronze không ghi vào đây
SILVER_INGEST_AUDIT_FIELDS = {
    "job_run_id": "string",        # mã của lần chạy job Silver (uuid)
    "source": "string",            # "traffic" / "weather"
    "event_date": "date",          # traffic: ngày theo giờ New York; weather: ngày theo UTC
    "event_hour": "int",           # traffic: giờ theo New York; weather: giờ theo UTC
    "kafka_partition": "int",
    "min_offset": "bigint",
    "max_offset": "bigint",
    "min_ingest_ts": "timestamp",
    "max_ingest_ts": "timestamp",
    "n_bronze_rows": "bigint",     # số dòng đọc từ Bronze
    "n_duplicates": "bigint",      # số dòng trước khử trùng − số dòng ngay sau khử trùng
    "n_invalid": "bigint",         # số dòng bị lọc trước khử trùng (thiếu khóa, giờ DST, status -101)
    "n_silver_rows": "bigint",     # số dòng ghi vào Silver
    "processed_ts": "timestamp",   
}
# Bản ghi traffic KHÔNG đưa vào traffic_cleaned (giờ DST mơ hồ/không tồn tại, thiếu khóa) - giữ lại để truy vết,
# không xóa âm thầm. Dùng chung chính sách với Speed Layer (common/time_rules.py).
SILVER_TRAFFIC_QUARANTINE_FIELDS = {
    "link_id": "bigint",
    "date": "date",                   # ngày theo giờ New York
    "hour": "int",
    "speed": "double",
    "travel_time": "bigint",
    "reason": "string",               # dst_duplicated / dst_nonexistent / missing_key (data_cleaning.dst_reason)
    "run_id": "string",
    "kafka_partition": "int",
    "kafka_offset": "bigint",
    "processed_ts": "timestamp",
}

# ----------------- Gold (Batch View của Lambda, theo quy ước gold-dev) ----------------------
# gold.fact_traffic_5min: grain (link_id, window_start_utc); gold.fact_traffic_hourly: grain (link_id, hour_start_utc).
# Đặt tên theo jobs/ddl/gold.sql: đơn vị trong tên cột (_mph, _sec), thời điểm *_utc, khoá lịch date_key (YYYYMMDD)
# và hour_key (0-23) theo giờ New York -> join được với dim_date / dim_hour. link_id = khoá nghiệp vụ của Silver
# (dim_road_link.link_id là STRING: join bằng CAST). road_link_key = khoá thay thế SCD2 của dim_road_link (như các fact
# của gold-dev), tra as-of theo ngày New York (common/road_link.py). Chỉ lưu thành phần CỘNG ĐƯỢC; cột suy ra do view tính:
# avg_speed_mph = sum_speed_mph / n_valid, n_invalid_other = n_records - n_valid - n_zero,
# window_end_utc = window_start_utc + 5', quality_flag = NO_VALID_DATA khi n_valid = 0.
CALENDAR_KEY_FIELDS = {
    "date_key": "int",               # YYYYMMDD của ngày New York (FK logic tới dim_date.date_key)
    "hour_key": "int",               # giờ New York 0-23 (FK logic tới dim_hour.hour_key)
}
GOLD_AGG_MEASURES = {
    "n_records": "int",              # tổng bản ghi trong khung
    "n_valid": "int",                # bản hợp lệ (0 < speed <= 100, travel_time > 0)
    "n_zero": "int",                 # bản speed = 0 (không có số đo)
    "sum_speed_mph": "double",       # tổng speed các bản hợp lệ  -> avg = sum_speed_mph / n_valid
    "sum_speed_sq": "double",        # tổng bình phương speed hợp lệ (mph^2) -> độ lệch chuẩn
    "sum_travel_time_sec": "bigint", # tổng travel_time các bản hợp lệ (giây)
}
GOLD_TRAFFIC_5MIN_FIELDS = {"link_id": "bigint", "road_link_key": "int", "window_start_utc": "timestamp", **CALENDAR_KEY_FIELDS,
                            **GOLD_AGG_MEASURES, "batch_run_id": "string", "processed_at": "timestamp"}
GOLD_TRAFFIC_HOURLY_FIELDS = {"link_id": "bigint", "road_link_key": "int", "hour_start_utc": "timestamp", **CALENDAR_KEY_FIELDS,
                              **GOLD_AGG_MEASURES, "n_windows": "int",
                              "batch_run_id": "string", "processed_at": "timestamp"}

# Nhật ký trạng thái Lambda handoff (append-only: mỗi lần chuyển trạng thái = 1 dòng; không UPDATE/DELETE).
# batch_state hiện hành = batch_state của dòng PUBLISHED mới nhất. status: PREPARING | VALIDATED | PUBLISHED | FAILED.
GOLD_LAMBDA_CONTROL_FIELDS = {
    "handoff_id": "string",
    "status": "string",
    "batch_state_prev": "timestamp",
    "batch_state": "timestamp",           # mốc mới (UTC, bội 5 phút); batch cho window_start_utc < batch_state
    "batch_run_id": "string",
    "snapshot_5min": "bigint",            # snapshot gold.fact_traffic_5min đã kiểm chứng (tag published trỏ vào đây)
    "snapshot_hourly": "bigint",
    "checks": "string",                   # JSON kết quả kiểm tra
    "message": "string",
    "created_at": "timestamp",
}

# ----------------- Speed (chỉ Speed Layer ghi) ----------------------
# Cùng cột (tên, kiểu, khoá lịch) với gold.fact_traffic_5min -> view ghép 2 bảng bằng UNION ALL.
SPEED_TRAFFIC_5MIN_FIELDS = {"link_id": "bigint", "road_link_key": "int", "window_start_utc": "timestamp", **CALENDAR_KEY_FIELDS,
                             **GOLD_AGG_MEASURES, "processed_at": "timestamp"}    # lúc Speed Layer ghi dòng này
# 1 dòng / link. Bản HỢP LỆ mới nhất (tốc độ hiện tại) + quan trắc mới nhất bất kể hợp lệ (cảm biến còn gửi không),
# để phân biệt cảm biến im lặng (STALE) với cảm biến vẫn gửi nhưng giá trị không hợp lệ (NO_VALID_DATA).
SPEED_TRAFFIC_LATEST_FIELDS = {
    "link_id": "bigint",
    "reading_ts_utc": "timestamp",    # thời điểm (UTC) của bản hợp lệ mới nhất (tên như fact_traffic_reading)
    "speed_mph": "double",            # của bản hợp lệ đó
    "travel_time_sec": "bigint",      # của bản hợp lệ đó
    "kafka_offset": "bigint",         # phân xử 2 bản trùng reading_ts_utc (offset lớn hơn = mới hơn)
    "last_seen_ts_utc": "timestamp",  # lần cuối nhận bản ghi, hợp lệ hay không
    "last_seen_valid": "boolean",     # bản nhận cuối có hợp lệ không
    "invalid_reason": "string",       # lý do không hợp lệ của bản nhận cuối (NULL nếu hợp lệ)
    "updated_at": "timestamp",
}
# Không lưu latitude/longitude: tọa độ ô thời tiết cố định, nằm ở silver.weather_cleaned (join theo cell_id).
SPEED_WEATHER_OBS_FIELDS = {
    "cell_id": "string", "time_utc": "timestamp",
    "temperature_2m": "double", "relative_humidity_2m": "int", "precipitation": "double", "snowfall": "double",
    "wind_speed_10m": "double", "wind_gusts_10m": "double", "weather_code": "int",
    "kafka_offset": "bigint",         # bản tin trùng (cell_id, time_utc): offset lớn hơn = bản mới hơn
}

# Producer: dùng field_names() để chọn cột gửi đi.
# Spark:    dùng to_ddl() làm schema cho from_json.


def field_names(fields: dict) -> list[str]:
    return list(fields)

def to_ddl(fields: dict) -> str:
    columns = []
    for column_name, data_type in fields.items():
        # Thêm `` để không bị trùng với tên biến giúp SparkSQL nhận diện `column_name` là tên cột
        columns.append(f"`{column_name}` {data_type}")

    ddl = ", ".join(columns)
    return ddl

# Chọn đúng cột, đúng thứ tự và đúng kiểu theo schema Silver.
# Import pyspark bên trong hàm: producer (container không có pyspark) cũng import file này để lấy field_names().
def conform(df, fields: dict):
    from pyspark.sql import functions as F
    return df.select([F.col(c).cast(t).alias(c) for c, t in fields.items()])

# if __name__ == "__main__":
#     print(field_names(TRAFFIC_FIELDS))
#     print(type(to_ddl(WEATHER_FIELDS)))
