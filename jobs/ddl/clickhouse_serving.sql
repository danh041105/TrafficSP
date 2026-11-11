-- ClickHouse serving layer cho TrafficSP (Speed + Batch + Lambda handoff).
-- Chạy: python jobs/ddl/clickhouse_serving.py   (idempotent: chỉ CREATE ... IF NOT EXISTS / CREATE OR REPLACE VIEW)
-- Quy ước:
--   * Mọi thời gian lưu UTC. event_time = thời điểm quan trắc; updated_at = lúc hệ thống ghi.
--   * Ghi lặp lại (retry micro-batch, restart) không làm double-count: bảng ReplacingMergeTree theo đúng grain
--     + insert_deduplication_token (cần non_replicated_deduplication_window > 0).
--   * KHÔNG giả định merge nền đã chạy: mọi truy vấn đọc qua FINAL hoặc argMax trong các view v_*.

CREATE DATABASE IF NOT EXISTS traffic;

-- ---------------------------------------------------------------- Dimension phục vụ
-- 1 dòng / link: vị trí, ô thời tiết (mapping từ silver.link_info), free-flow (gold.link_baseline)
CREATE TABLE IF NOT EXISTS traffic.link_dim (
    link_id            UInt64,
    link_name          String,
    borough            LowCardinality(String),
    latitude           Float64,
    longitude          Float64,
    cell_id            String,
    cell_distance_km   Float64,
    mapping_version    String,
    free_flow_speed    Nullable(Float64),
    free_flow_samples  UInt32,
    baseline_version   String,
    updated_at         DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY link_id;

-- ---------------------------------------------------------------- Speed layer
-- A. Latest reading: 1 link -> quan trắc HỢP LỆ mới nhất. version = event_time nên bản cũ đến muộn không thể thắng.
CREATE TABLE IF NOT EXISTS traffic.traffic_latest (
    link_id          UInt64,
    event_time       DateTime('UTC'),
    data_as_of       String,                      -- chuỗi gốc giờ New York
    speed            Float64,
    travel_time      Int64,
    traffic_state    LowCardinality(String),      -- theo bản ghi này + free-flow lúc ghi
    quality_flag     LowCardinality(String),
    free_flow_speed  Nullable(Float64),
    run_id           String,
    kafka_partition  Int32,
    kafka_offset     Int64,
    kafka_ts         DateTime64(3, 'UTC'),        -- thời điểm producer gửi (đo độ trễ end-to-end)
    updated_at       DateTime64(3, 'UTC'),
    batch_id         Int64
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY link_id
SETTINGS non_replicated_deduplication_window = 1000;

-- A'. Quan trắc mới nhất bất kể hợp lệ hay không (để biết link đang gửi speed = 0 / bản ghi lỗi)
CREATE TABLE IF NOT EXISTS traffic.traffic_latest_any (
    link_id          UInt64,
    event_time       DateTime('UTC'),
    data_as_of       String,
    speed            Float64,
    travel_time      Int64,
    is_valid         UInt8,
    invalid_reason   LowCardinality(String),
    kafka_ts         DateTime64(3, 'UTC'),
    updated_at       DateTime64(3, 'UTC'),
    batch_id         Int64
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY link_id
SETTINGS non_replicated_deduplication_window = 1000;

-- B. 5-minute aggregate realtime: grain (link_id, window_start). Chỉ lưu thành phần cộng được + min/max.
CREATE TABLE IF NOT EXISTS traffic.traffic_5min_rt (
    link_id          UInt64,
    window_start     DateTime('UTC'),
    window_end       DateTime('UTC'),
    n_records        UInt32,
    n_valid          UInt32,
    n_zero           UInt32,
    n_invalid_other  UInt32,
    sum_speed        Float64,
    sum_speed_sq     Float64,
    sum_travel_time  Int64,
    min_speed        Nullable(Float64),
    max_speed        Nullable(Float64),
    avg_speed        Nullable(Float64),           -- = sum_speed / n_valid (tiện đọc; roll-up phải dùng sum/n)
    traffic_state    LowCardinality(String),
    quality_flag     LowCardinality(String),
    kafka_ts_max     DateTime64(3, 'UTC'),
    updated_at       DateTime64(3, 'UTC'),
    batch_id         Int64
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (link_id, window_start)
SETTINGS non_replicated_deduplication_window = 1000;

-- Weather theo giờ API (mốc T, UTC) - từ stream (gd2) hoặc backfill batch (gd1)
CREATE TABLE IF NOT EXISTS traffic.weather_hourly (
    cell_id               String,
    time_utc              DateTime('UTC'),
    temperature_2m        Nullable(Float64),
    relative_humidity_2m  Nullable(Int32),
    precipitation         Nullable(Float64),      -- tổng của giờ (T-1h, T]
    snowfall              Nullable(Float64),
    wind_speed_10m        Nullable(Float64),
    wind_gusts_10m        Nullable(Float64),
    weather_code          Nullable(Int32),
    source                LowCardinality(String),
    run_id                String,
    kafka_ts              Nullable(DateTime64(3, 'UTC')),
    updated_at            DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (cell_id, time_utc)
SETTINGS non_replicated_deduplication_window = 1000;

-- Thống kê chất lượng từng micro-batch (DST, lý do không hợp lệ, phạm vi event-time)
CREATE TABLE IF NOT EXISTS traffic.speed_batch_stats (
    query            LowCardinality(String),
    batch_id         Int64,
    n_rows           UInt64,
    n_valid          UInt64,
    n_dst_ambiguous  UInt64,
    n_dst_nonexistent UInt64,
    n_unparseable    UInt64,
    invalid_reasons  Map(String, UInt64),
    min_event_time   Nullable(DateTime('UTC')),
    max_event_time   Nullable(DateTime('UTC')),
    written_at       DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(written_at)
ORDER BY (query, batch_id);

-- Độ trễ end-to-end mỗi micro-batch: done_at (sau khi ghi xong ClickHouse) - kafka_ts (lúc message vào Kafka).
-- speed_latest: kafka_ts của từng bản ghi; speed_5min: kafka_ts_max của từng cửa sổ phát ra (gồm cả chờ watermark).
-- Độ trễ pXX = done_at - kafka_ts_p(100-XX). Đo bằng đồng hồ hệ thống thật (không phải event-time mô phỏng).
CREATE TABLE IF NOT EXISTS traffic.speed_latency (
    query          LowCardinality(String),
    batch_id       Int64,
    n_items        UInt64,
    kafka_ts_min   DateTime64(3, 'UTC'),
    kafka_ts_p05   DateTime64(3, 'UTC'),
    kafka_ts_p50   DateTime64(3, 'UTC'),
    kafka_ts_max   DateTime64(3, 'UTC'),
    done_at        DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(done_at)
ORDER BY (query, batch_id);

-- Tiến độ streaming (StreamingQueryListener) cho benchmark
CREATE TABLE IF NOT EXISTS traffic.stream_progress (
    query                  LowCardinality(String),
    run_uuid               String,
    batch_id               Int64,
    ts                     DateTime64(3, 'UTC'),
    num_input_rows         UInt64,
    input_rows_per_sec     Float64,
    processed_rows_per_sec Float64,
    batch_duration_ms      UInt64,
    trigger_execution_ms   UInt64,
    add_batch_ms           UInt64,
    get_batch_ms           UInt64,
    watermark              String,
    state_rows             UInt64,
    state_memory_bytes     UInt64,
    rows_dropped_by_watermark UInt64,
    max_offsets_behind     Nullable(Int64),
    avg_offsets_behind     Nullable(Float64),
    raw_json               String
) ENGINE = MergeTree
ORDER BY (query, ts);

-- ---------------------------------------------------------------- Batch layer
CREATE TABLE IF NOT EXISTS traffic.traffic_5min_batch (
    link_id          UInt64,
    window_start     DateTime('UTC'),
    window_end       DateTime('UTC'),
    n_records        UInt32,
    n_valid          UInt32,
    n_zero           UInt32,
    n_invalid_other  UInt32,
    sum_speed        Float64,
    sum_speed_sq     Float64,
    sum_travel_time  Int64,
    min_speed        Nullable(Float64),
    max_speed        Nullable(Float64),
    avg_speed        Nullable(Float64),
    traffic_state    LowCardinality(String),
    quality_flag     LowCardinality(String),
    batch_run_id     String,
    computed_at      DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (link_id, window_start);

-- Bảng đệm cùng cấu trúc: ghi xong + kiểm tra rồi mới REPLACE PARTITION sang bảng chính (atomic theo ngày)
CREATE TABLE IF NOT EXISTS traffic.traffic_5min_batch_staging AS traffic.traffic_5min_batch;

CREATE TABLE IF NOT EXISTS traffic.traffic_hourly (
    link_id          UInt64,
    hour_utc         DateTime('UTC'),
    n_records        UInt32,
    n_valid          UInt32,
    n_zero           UInt32,
    n_invalid_other  UInt32,
    sum_speed        Float64,
    sum_speed_sq     Float64,
    sum_travel_time  Int64,
    min_speed        Nullable(Float64),
    max_speed        Nullable(Float64),
    avg_speed        Nullable(Float64),
    batch_run_id     String,
    computed_at      DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMMDD(hour_utc)            -- cùng đơn vị (ngày UTC) với traffic_5min_batch để REPLACE PARTITION nguyên tử
ORDER BY (link_id, hour_utc);

CREATE TABLE IF NOT EXISTS traffic.traffic_hourly_staging AS traffic.traffic_hourly;

-- Nhật ký chạy batch (số bản ghi DST bị loại khỏi gom theo thời gian, v.v.)
CREATE TABLE IF NOT EXISTS traffic.batch_run_stats (
    batch_run_id     String,
    day              Date,
    n_silver_rows    UInt64,
    n_valid          UInt64,
    n_dst_excluded   UInt64,
    n_windows        UInt64,
    checks           String,
    computed_at      DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (day, batch_run_id);

-- ---------------------------------------------------------------- Lambda handoff
-- Mỗi lần publish thêm 1 dòng; T_batch hiện hành = max(t_batch) của các dòng PUBLISHED.
CREATE TABLE IF NOT EXISTS traffic.lambda_handoff (
    t_batch       DateTime('UTC'),
    batch_run_id  String,
    status        LowCardinality(String),      -- PUBLISHED | REJECTED
    checks        String,
    published_at  DateTime64(3, 'UTC')
) ENGINE = MergeTree
ORDER BY published_at;
