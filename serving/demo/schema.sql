-- Schema cho CHẾ ĐỘ DEMO của Serving API (SERVING_SOURCE=demo).
-- Mô phỏng tối thiểu các view serving.* trên Trino để chạy giao diện trên máy không có lakehouse.
-- KHÔNG phải một phần của kiến trúc chính (ClickHouse đã được thay bằng Trino).
CREATE DATABASE IF NOT EXISTS traffic;

-- ~ serving.v_link_dim + silver.link_info.link_points
CREATE TABLE IF NOT EXISTS traffic.dim_link
(
    link_id         UInt64,
    link_name       String,
    borough         LowCardinality(String),
    free_flow_speed Nullable(Float64),
    cell_id         LowCardinality(String),
    latitude        Float64,
    longitude       Float64,
    link_points     String
)
ENGINE = ReplacingMergeTree ORDER BY link_id;

-- ~ speed layer: từng bản ghi đã nhận (cả hợp lệ lẫn không) để suy ra latest + 5 phút
CREATE TABLE IF NOT EXISTS traffic.reading
(
    link_id     UInt64,
    event_time  DateTime('UTC'),
    speed       Float32,
    travel_time UInt32,
    is_valid    UInt8
)
ENGINE = MergeTree ORDER BY (link_id, event_time);

-- ~ gold.traffic_hourly: lịch sử giai đoạn 1 (01-09/2025)
CREATE TABLE IF NOT EXISTS traffic.hourly
(
    link_id   UInt64,
    hour_utc  DateTime('UTC'),
    sum_speed Float64,
    n_valid   UInt32
)
ENGINE = MergeTree ORDER BY (link_id, hour_utc);

-- ~ serving.v_weather_hourly (16 ô ECMWF IFS)
CREATE TABLE IF NOT EXISTS traffic.weather
(
    cell_id          LowCardinality(String),
    time_utc         DateTime('UTC'),
    temperature_c    Nullable(Float32),
    precipitation_mm Nullable(Float32),
    wind_speed_kmh   Nullable(Float32),
    weather_code     Nullable(UInt16)
)
ENGINE = MergeTree ORDER BY (cell_id, time_utc);

-- ~ serving.v_traffic_forecast (hợp đồng chờ phần MLOps). Demo ghi dự báo baseline.
CREATE TABLE IF NOT EXISTS traffic.forecast
(
    link_id       UInt64,
    issued_at     DateTime('UTC'),
    target_time   DateTime('UTC'),
    horizon_min   UInt16,
    pred_speed    Float32,
    pred_lower    Float32,
    pred_upper    Float32,
    model_name    LowCardinality(String),
    model_version LowCardinality(String)
)
ENGINE = ReplacingMergeTree(issued_at) ORDER BY (link_id, target_time, horizon_min);
