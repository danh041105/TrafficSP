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