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
    "traffic_utc": "timestamp",      # data_as_of (giờ New York) đã đổi sang UTC
    "date": "date",                   # ngày theo giờ New York
    "hour": "int",                    # từ 0h đến 23h
    "day_of_week": "int",            # thứ trong tuần theo giờ New York
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
    "kafka_partition": "int",
    "min_offset": "bigint",
    "max_offset": "bigint",
    "min_ingest_ts": "timestamp",
    "max_ingest_ts": "timestamp",
    "n_bronze_rows": "bigint",     # số dòng đọc từ Bronze
    "n_duplicates": "bigint",      # số dòng trước khử trùng − số dòng ngay sau khử trùng
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
    "reason": "string",               # dst_ambiguous / dst_nonexistent / missing_key
    "run_id": "string",
    "kafka_partition": "int",
    "kafka_offset": "bigint",
    "processed_ts": "timestamp",
}

# ----------------- Gold (Batch View) ----------------------
# Grain (link_id, window_start_utc) 5 phút; (link_id, hour_utc) 1 giờ. Chỉ lưu thành phần cộng được + min/max.
GOLD_AGG_MEASURES = {
    "n_records": "int",
    "n_valid": "int",
    "n_zero": "int",
    "n_invalid_other": "int",
    "sum_speed": "double",
    "sum_speed_sq": "double",
    "sum_travel_time": "bigint",
    "min_speed": "double",
    "max_speed": "double",
}
GOLD_TRAFFIC_5MIN_FIELDS = {"link_id": "bigint", "window_start_utc": "timestamp", "window_end_utc": "timestamp",
                            **GOLD_AGG_MEASURES, "avg_speed": "double", "quality_flag": "string",
                            "batch_run_id": "string", "processed_at": "timestamp"}
GOLD_TRAFFIC_HOURLY_FIELDS = {"link_id": "bigint", "hour_utc": "timestamp",
                              **GOLD_AGG_MEASURES, "avg_speed": "double", "n_windows": "int",
                              "batch_run_id": "string", "processed_at": "timestamp"}

# Nhật ký trạng thái Lambda handoff (append-only: mỗi lần chuyển trạng thái = 1 dòng; không UPDATE/DELETE).
# T_batch hiện hành = t_batch của dòng PUBLISHED mới nhất. status: PREPARING | VALIDATED | PUBLISHED | FAILED.
GOLD_LAMBDA_CONTROL_FIELDS = {
    "handoff_id": "string",
    "status": "string",
    "t_batch_prev": "timestamp",
    "t_batch": "timestamp",               # mốc mới (UTC, bội 5 phút); batch cho window_start_utc < t_batch
    "batch_run_id": "string",
    "snapshot_5min": "bigint",            # snapshot gold.traffic_5min_batch đã kiểm chứng (tag published trỏ vào đây)
    "snapshot_hourly": "bigint",
    "checks": "string",                   # JSON kết quả kiểm tra
    "message": "string",
    "created_at": "timestamp",
}

# ----------------- Speed (chỉ Speed Layer ghi) ----------------------
SPEED_TRAFFIC_5MIN_FIELDS = {"link_id": "bigint", "window_start_utc": "timestamp", "window_end_utc": "timestamp",
                             **GOLD_AGG_MEASURES, "avg_speed": "double", "quality_flag": "string",
                             "kafka_ts_max": "timestamp",    # message cuối của cửa sổ vào Kafka lúc nào (đo độ trễ)
                             "processed_at": "timestamp"}
# 1 dòng / link. Hai nhóm cột độc lập: bản HỢP LỆ mới nhất (valid_*) và quan trắc mới nhất bất kể hợp lệ (obs_*),
# để phân biệt cảm biến im lặng (STALE) với cảm biến vẫn gửi nhưng giá trị không hợp lệ (NO_VALID_DATA).
SPEED_TRAFFIC_LATEST_FIELDS = {
    "link_id": "bigint",
    "valid_event_time": "timestamp", "valid_data_as_of": "string", "speed": "double", "travel_time": "bigint",
    "valid_kafka_partition": "int", "valid_kafka_offset": "bigint", "valid_kafka_ts": "timestamp",
    "obs_event_time": "timestamp", "obs_data_as_of": "string", "obs_speed": "double", "obs_travel_time": "bigint",
    "obs_is_valid": "boolean", "obs_invalid_reason": "string",
    "obs_kafka_partition": "int", "obs_kafka_offset": "bigint", "obs_kafka_ts": "timestamp",
    "run_id": "string", "batch_id": "bigint", "updated_at": "timestamp",
}
SPEED_WEATHER_OBS_FIELDS = {
    "cell_id": "string", "time_utc": "timestamp",
    "temperature_2m": "double", "relative_humidity_2m": "int", "precipitation": "double", "snowfall": "double",
    "wind_speed_10m": "double", "wind_gusts_10m": "double", "weather_code": "int",
    "run_id": "string", "kafka_partition": "int", "kafka_offset": "bigint", "kafka_ts": "timestamp",
    "processed_at": "timestamp",
}
GOLD_LINK_BASELINE_FIELDS = {
    "link_id": "bigint",
    "free_flow_speed": "double",      # p85 tốc độ hợp lệ, lúc khô, 22:00-05:59 giờ New York (D-18)
    "n_samples": "bigint",
    "history_start": "date",
    "history_end": "date",
    "baseline_version": "string",
    "computed_ts": "timestamp",
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

# if __name__ == "__main__":
#     print(field_names(TRAFFIC_FIELDS))
#     print(type(to_ddl(WEATHER_FIELDS)))