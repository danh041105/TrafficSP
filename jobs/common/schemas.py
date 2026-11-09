# file này giúp định nghĩa các field sử dụng trong message Kafka dưới dạng json
# Các trường trong traffic được sử dụng
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