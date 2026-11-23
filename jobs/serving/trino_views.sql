-- Serving Layer trên Trino (catalog lakehouse = Iceberg JDBC catalog). Chỉ VIEW, không chứa dữ liệu.
-- Áp dụng (idempotent):  docker exec -i traffic-trino trino --catalog lakehouse < jobs/serving/trino_views.sql
-- Quy ước:
--   * Mọi thời gian là timestamp(6) with time zone (UTC). Batch View đọc qua tag 'published' (do lambda_handoff dời),
--     KHÔNG đọc nhánh main -> dữ liệu batch chưa kiểm chứng không lọt ra ngoài.
--   * Tên cột theo mô hình sao của gold-dev (jobs/ddl/gold.sql): đơn vị trong tên (_mph, _sec, _mm, _cm, _c, _pct),
--     thời điểm *_utc, khoá lịch date_key (YYYYMMDD) / hour_key (0-23) theo giờ New York, ô thời tiết = grid_key.
--   * Free-flow lấy từ gold.dim_road_link (phiên bản hiện hành: valid_to IS NULL); link_id của dim là STRING.
--   * SECURITY INVOKER: người truy vấn cần quyền SELECT trên bảng gốc (đã cấp trong conf/trino/access/lakehouse-rules.json).
--   * Quy tắc giống hệt bản Spark (common/quality_rules.py): CONGESTED < 0,5 ≤ SLOW < 0,8 ≤ FREE_FLOW.

-- ---------------------------------------------------------------- Lambda
-- Tên cũ của view (trước khi đổi cột t_batch thành batch_state): xoá để không còn view trỏ vào cột cũ.
DROP VIEW IF EXISTS lakehouse.serving.v_lambda_t_batch;
CREATE OR REPLACE VIEW lakehouse.serving.v_batch_state SECURITY INVOKER AS
SELECT coalesce(max(batch_state), TIMESTAMP '1970-01-01 00:00:00 UTC') AS batch_state
FROM lakehouse.gold.lambda_control
WHERE status = 'PUBLISHED';

CREATE OR REPLACE VIEW lakehouse.serving.v_lambda_status SECURITY INVOKER AS
-- Lần PUBLISHED gần nhất theo created_at (batch_state không bao giờ giảm; công bố lại bằng --correction giữ nguyên
-- batch_state nên KHÔNG dùng batch_state để chọn dòng - sẽ lấy nhầm snapshot của lần công bố cũ).
WITH pub AS (
    SELECT max_by(handoff_id, created_at) AS handoff_id, max(batch_state) AS batch_state,
           max_by(snapshot_5min, created_at) AS snapshot_5min, max_by(snapshot_hourly, created_at) AS snapshot_hourly,
           max(created_at) AS published_at
    FROM lakehouse.gold.lambda_control WHERE status = 'PUBLISHED'),
last AS (
    SELECT max_by(handoff_id, created_at) AS handoff_id, max_by(status, created_at) AS status,
           max_by(batch_state, created_at) AS batch_state, max_by(message, created_at) AS message, max(created_at) AS at
    FROM lakehouse.gold.lambda_control),
-- chỉ quét phân vùng từ batch_state trở đi (partition pruning) thay vì toàn bảng speed
spd AS (SELECT max(window_start_utc) + INTERVAL '5' MINUTE AS speed_max_window_end_utc, max(processed_at) AS speed_last_commit_utc
        FROM lakehouse.speed.traffic_5min_rt
        WHERE window_start_utc >= (SELECT batch_state FROM lakehouse.serving.v_batch_state))
SELECT coalesce(pub.batch_state, TIMESTAMP '1970-01-01 00:00:00 UTC') AS batch_state,
       pub.handoff_id AS published_handoff_id, pub.published_at, pub.snapshot_5min, pub.snapshot_hourly,
       (SELECT snapshot_id FROM lakehouse.gold."fact_traffic_5min$refs" WHERE name = 'published') AS tag_snapshot_5min,
       (SELECT snapshot_id FROM lakehouse.gold."fact_traffic_hourly$refs" WHERE name = 'published') AS tag_snapshot_hourly,
       last.handoff_id AS last_handoff_id, last.status AS last_status, last.batch_state AS last_batch_state,
       last.message AS last_message, last.at AS last_change_at,
       spd.speed_max_window_end_utc, spd.speed_last_commit_utc,
       date_diff('minute', coalesce(pub.batch_state, TIMESTAMP '1970-01-01 00:00:00 UTC'), spd.speed_max_window_end_utc)
           AS speed_ahead_of_batch_min
FROM spd CROSS JOIN last LEFT JOIN pub ON true;

-- ---------------------------------------------------------------- 5 phút hợp nhất (không chồng lấn, không hở)
-- Bảng chỉ lưu thành phần cộng được; các cột suy ra (window_end_utc, n_invalid_other, avg_speed_mph, quality_flag) tính ở đây.
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_5min_unified SECURITY INVOKER AS
SELECT u.link_id, u.road_link_key, u.window_start_utc, u.window_start_utc + INTERVAL '5' MINUTE AS window_end_utc,
       u.date_key, u.hour_key,
       u.n_records, u.n_valid, u.n_zero, u.n_records - u.n_valid - u.n_zero AS n_invalid_other,
       u.sum_speed_mph, u.sum_speed_sq, u.sum_travel_time_sec,
       if(u.n_valid > 0, u.sum_speed_mph / u.n_valid) AS avg_speed_mph,
       if(u.n_valid = 0, 'NO_VALID_DATA', 'OK') AS quality_flag,
       u.layer
FROM (
    SELECT b.link_id, b.road_link_key, b.window_start_utc, b.date_key, b.hour_key, b.n_records, b.n_valid, b.n_zero, b.sum_speed_mph,
           b.sum_speed_sq, b.sum_travel_time_sec, 'batch' AS layer
    FROM lakehouse.gold.fact_traffic_5min FOR VERSION AS OF 'published' b
    WHERE b.window_start_utc < (SELECT batch_state FROM lakehouse.serving.v_batch_state)
    UNION ALL
    SELECT s.link_id, s.road_link_key, s.window_start_utc, s.date_key, s.hour_key, s.n_records, s.n_valid, s.n_zero, s.sum_speed_mph,
           s.sum_speed_sq, s.sum_travel_time_sec, 'speed' AS layer
    FROM lakehouse.speed.traffic_5min_rt s
    WHERE s.window_start_utc >= (SELECT batch_state FROM lakehouse.serving.v_batch_state)) u;

-- ---------------------------------------------------------------- dimension link (vị trí, ô thời tiết, free-flow)
-- Vị trí từ silver.link_info; khoá thay thế, hình học và free-flow từ phiên bản HIỆN HÀNH của gold.dim_road_link (SCD2).
CREATE OR REPLACE VIEW lakehouse.serving.v_link_dim SECURITY INVOKER AS
SELECT l.link_id, d.road_link_key, l.link_name, l.borough, l.latitude, l.longitude, l.cell_id AS grid_key,
       l.cell_distance_km, d.geometry_wkt, d.free_flow_speed_mph, d.valid_from AS free_flow_valid_from
FROM lakehouse.silver.link_info l
LEFT JOIN lakehouse.gold.dim_road_link d ON d.link_id = CAST(l.link_id AS varchar) AND d.valid_to IS NULL;

-- ---------------------------------------------------------------- thời tiết
-- Một bản / (ô, giờ): ưu tiên Silver (batch đã làm sạch), nếu chưa có thì bản realtime mới nhất (kafka_offset lớn nhất).
-- Giữ tên cột như Silver (nguồn); view theo khoảng giờ bên dưới mới đổi sang tên/đơn vị của gold-dev.
CREATE OR REPLACE VIEW lakehouse.serving.v_weather_hourly SECURITY INVOKER AS
SELECT cell_id, time_utc, temperature_2m, relative_humidity_2m, precipitation, snowfall, wind_speed_10m,
       wind_gusts_10m, weather_code, source
FROM (
    SELECT *, row_number() OVER (PARTITION BY cell_id, time_utc ORDER BY prio, kafka_offset DESC) AS rn
    FROM (
        SELECT cell_id, weather_utc AS time_utc, temperature_2m, relative_humidity_2m, precipitation, snowfall,
               wind_speed_10m, wind_gusts_10m, weather_code, 'batch' AS source, 0 AS prio, CAST(0 AS bigint) AS kafka_offset
        FROM lakehouse.silver.weather_cleaned
        UNION ALL
        SELECT cell_id, time_utc, temperature_2m, relative_humidity_2m, precipitation, snowfall,
               wind_speed_10m, wind_gusts_10m, weather_code, 'speed', 1, kafka_offset
        FROM lakehouse.speed.weather_obs))
WHERE rn = 1;

-- Thời tiết theo KHOẢNG [H, H+1) (tên/đơn vị như gold.fact_weather_hourly; gió đổi km/h -> mph):
--   nhiệt độ / độ ẩm / gió / gió giật: tức thời tại mốc H
--   mưa / tuyết / weather_code: bản ghi mốc H+1 (= tổng của giờ trước mốc) -> FINAL
--   chưa có mốc H+1 (realtime, giờ chưa kết thúc): tạm dùng của mốc H -> PROVISIONAL (tự thành FINAL khi mốc H+1 tới)
CREATE OR REPLACE VIEW lakehouse.serving.v_weather_interval SECURITY INVOKER AS
SELECT a.cell_id AS grid_key, a.time_utc AS hour_start_utc,
       a.temperature_2m AS temperature_c, CAST(a.relative_humidity_2m AS double) AS relative_humidity_pct,
       a.wind_speed_10m / 1.609344 AS wind_speed_mph, a.wind_gusts_10m / 1.609344 AS wind_gust_mph,
       coalesce(b.precipitation, a.precipitation) AS precipitation_mm,
       coalesce(b.snowfall, a.snowfall) AS snowfall_cm,
       if(b.cell_id IS NOT NULL, b.weather_code, a.weather_code) AS weather_code,
       if(b.cell_id IS NOT NULL, 'FINAL', 'PROVISIONAL') AS weather_status
FROM lakehouse.serving.v_weather_hourly a
LEFT JOIN lakehouse.serving.v_weather_hourly b
       ON b.cell_id = a.cell_id AND b.time_utc = a.time_utc + INTERVAL '1' HOUR;

-- ---------------------------------------------------------------- 5 phút + trạng thái giao thông + thời tiết
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_5min_enriched SECURITY INVOKER AS
SELECT u.link_id, u.road_link_key, u.window_start_utc, u.window_end_utc, u.date_key, u.hour_key, u.layer,
       u.n_records, u.n_valid, u.n_zero, u.n_invalid_other, u.sum_speed_mph, u.sum_speed_sq, u.sum_travel_time_sec,
       u.avg_speed_mph, u.quality_flag,
       d.link_name, d.borough, d.grid_key, d.free_flow_speed_mph,
       CASE WHEN d.link_id IS NULL OR d.grid_key IS NULL THEN 'NO_MAPPING'
            WHEN d.free_flow_speed_mph IS NULL OR d.free_flow_speed_mph <= 0 THEN 'NO_BASELINE'
            WHEN u.n_valid = 0 THEN 'NO_VALID_DATA'
            WHEN u.avg_speed_mph / d.free_flow_speed_mph < 0.5 THEN 'CONGESTED'
            WHEN u.avg_speed_mph / d.free_flow_speed_mph < 0.8 THEN 'SLOW'
            ELSE 'FREE_FLOW' END AS traffic_state,
       w.temperature_c, w.relative_humidity_pct, w.wind_speed_mph, w.wind_gust_mph,
       w.precipitation_mm, w.snowfall_cm, w.weather_code,
       CASE WHEN d.grid_key IS NULL THEN 'NO_MAPPING'
            WHEN w.grid_key IS NULL THEN 'MISSING'
            ELSE w.weather_status END AS weather_status
FROM lakehouse.serving.v_traffic_5min_unified u
LEFT JOIN lakehouse.serving.v_link_dim d ON d.link_id = u.link_id
LEFT JOIN lakehouse.serving.v_weather_interval w
       ON w.grid_key = d.grid_key AND w.hour_start_utc = date_trunc('hour', u.window_start_utc);

-- ---------------------------------------------------------------- 1 giờ hợp nhất (OLAP)
-- Giờ nằm trọn trước batch_state: lấy thẳng gold.fact_traffic_hourly (đã tính sẵn). Giờ chứa batch_state trở đi: cộng
-- thành phần từ 5 phút hợp nhất (giờ ranh giới có thể vừa batch vừa speed). Không AVG(AVG).
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_hourly_unified SECURITY INVOKER AS
SELECT h.link_id, h.road_link_key, h.hour_start_utc, h.date_key, h.hour_key, h.n_records, h.n_valid, h.n_zero,
       h.n_records - h.n_valid - h.n_zero AS n_invalid_other,
       h.sum_speed_mph, h.sum_speed_sq, h.sum_travel_time_sec, if(h.n_valid > 0, h.sum_speed_mph / h.n_valid) AS avg_speed_mph,
       h.n_windows, 'batch' AS layers
FROM lakehouse.gold.fact_traffic_hourly FOR VERSION AS OF 'published' h
WHERE h.hour_start_utc < date_trunc('hour', (SELECT batch_state FROM lakehouse.serving.v_batch_state))
UNION ALL
SELECT link_id, road_link_key, hour_start_utc,
       CAST(format_datetime(hour_start_utc AT TIME ZONE 'America/New_York', 'yyyyMMdd') AS integer) AS date_key,
       hour(hour_start_utc AT TIME ZONE 'America/New_York') AS hour_key,
       n_records, n_valid, n_zero, n_records - n_valid - n_zero AS n_invalid_other,
       sum_speed_mph, sum_speed_sq, sum_travel_time_sec, if(n_valid > 0, sum_speed_mph / n_valid) AS avg_speed_mph,
       n_windows, layers
FROM (
    SELECT link_id, road_link_key, date_trunc('hour', window_start_utc) AS hour_start_utc,
           sum(n_records) AS n_records, sum(n_valid) AS n_valid, sum(n_zero) AS n_zero,
           sum(sum_speed_mph) AS sum_speed_mph, sum(sum_speed_sq) AS sum_speed_sq, sum(sum_travel_time_sec) AS sum_travel_time_sec,
           CAST(count(*) AS integer) AS n_windows, array_join(array_sort(array_distinct(array_agg(layer))), ',') AS layers
    FROM lakehouse.serving.v_traffic_5min_unified
    WHERE window_start_utc >= date_trunc('hour', (SELECT batch_state FROM lakehouse.serving.v_batch_state))
    GROUP BY link_id, road_link_key, date_trunc('hour', window_start_utc));

-- ---------------------------------------------------------------- trạng thái mới nhất từng link
-- Đồng hồ sự kiện = thời điểm lớn nhất đã nhận trên toàn mạng (dữ liệu phát lại năm 2025, không dùng giờ hệ thống).
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_latest SECURITY INVOKER AS
WITH clock AS (SELECT max(last_seen_ts_utc) AS event_clock_utc FROM lakehouse.speed.traffic_latest)
SELECT d.link_id, d.road_link_key, d.link_name, d.borough, d.free_flow_speed_mph,
       l.reading_ts_utc AS last_valid_reading_ts_utc, l.speed_mph, l.travel_time_sec,
       l.last_seen_ts_utc, l.last_seen_valid, l.invalid_reason AS last_invalid_reason, c.event_clock_utc,
       date_diff('minute', l.reading_ts_utc, c.event_clock_utc) AS valid_age_min,
       CASE WHEN l.link_id IS NULL THEN 'NO_DATA'
            WHEN l.reading_ts_utc IS NULL THEN 'NO_VALID_DATA'
            WHEN l.last_seen_ts_utc > l.reading_ts_utc AND NOT l.last_seen_valid THEN 'NO_VALID_DATA'
            WHEN date_diff('minute', l.reading_ts_utc, c.event_clock_utc) > 15 THEN 'STALE'
            WHEN d.free_flow_speed_mph IS NULL OR d.free_flow_speed_mph <= 0 THEN 'NO_BASELINE'
            WHEN l.speed_mph / d.free_flow_speed_mph < 0.5 THEN 'CONGESTED'
            WHEN l.speed_mph / d.free_flow_speed_mph < 0.8 THEN 'SLOW'
            ELSE 'FREE_FLOW' END AS traffic_state,
       l.updated_at
FROM lakehouse.serving.v_link_dim d
LEFT JOIN lakehouse.speed.traffic_latest l ON l.link_id = d.link_id
CROSS JOIN clock c;
