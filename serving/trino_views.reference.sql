-- Serving Layer trên Trino (catalog lakehouse = Iceberg JDBC catalog). Chỉ VIEW, không chứa dữ liệu.
-- Áp dụng (idempotent):  docker exec -i traffic-trino trino --catalog lakehouse < jobs/serving/trino_views.sql
-- Quy ước:
--   * Mọi thời gian là timestamp(6) with time zone (UTC). Batch View đọc qua tag 'published' (do lambda_handoff dời),
--     KHÔNG đọc nhánh main -> dữ liệu batch chưa kiểm chứng không lọt ra ngoài.
--   * SECURITY INVOKER: người truy vấn cần quyền SELECT trên bảng gốc (đã cấp trong conf/trino/access/lakehouse-rules.json).
--   * Quy tắc giống hệt bản Spark (common/quality_rules.py, D-18): CONGESTED < 0,5 ≤ SLOW < 0,8 ≤ FREE_FLOW.

-- ---------------------------------------------------------------- Lambda
CREATE OR REPLACE VIEW lakehouse.serving.v_lambda_t_batch SECURITY INVOKER AS
SELECT coalesce(max(t_batch), TIMESTAMP '1970-01-01 00:00:00 UTC') AS t_batch
FROM lakehouse.gold.lambda_control
WHERE status = 'PUBLISHED';

CREATE OR REPLACE VIEW lakehouse.serving.v_lambda_status SECURITY INVOKER AS
WITH pub AS (
    SELECT max_by(handoff_id, t_batch) AS handoff_id, max(t_batch) AS t_batch,
           max_by(snapshot_5min, t_batch) AS snapshot_5min, max_by(snapshot_hourly, t_batch) AS snapshot_hourly,
           max_by(created_at, t_batch) AS published_at
    FROM lakehouse.gold.lambda_control WHERE status = 'PUBLISHED'),
last AS (
    SELECT max_by(handoff_id, created_at) AS handoff_id, max_by(status, created_at) AS status,
           max_by(t_batch, created_at) AS t_batch, max_by(message, created_at) AS message, max(created_at) AS at
    FROM lakehouse.gold.lambda_control),
-- chỉ quét phân vùng từ T_batch trở đi (partition pruning) thay vì toàn bảng speed
spd AS (SELECT max(window_end_utc) AS speed_max_window_end, max(processed_at) AS speed_last_commit
        FROM lakehouse.speed.traffic_5min_rt
        WHERE window_start_utc >= (SELECT t_batch FROM lakehouse.serving.v_lambda_t_batch))
SELECT coalesce(pub.t_batch, TIMESTAMP '1970-01-01 00:00:00 UTC') AS t_batch,
       pub.handoff_id AS published_handoff_id, pub.published_at, pub.snapshot_5min, pub.snapshot_hourly,
       (SELECT snapshot_id FROM lakehouse.gold."traffic_5min_batch$refs" WHERE name = 'published') AS tag_snapshot_5min,
       (SELECT snapshot_id FROM lakehouse.gold."traffic_hourly$refs" WHERE name = 'published') AS tag_snapshot_hourly,
       last.handoff_id AS last_handoff_id, last.status AS last_status, last.t_batch AS last_t_batch,
       last.message AS last_message, last.at AS last_change_at,
       spd.speed_max_window_end, spd.speed_last_commit,
       date_diff('minute', coalesce(pub.t_batch, TIMESTAMP '1970-01-01 00:00:00 UTC'), spd.speed_max_window_end)
           AS speed_ahead_of_batch_min
FROM spd CROSS JOIN last LEFT JOIN pub ON true;

-- ---------------------------------------------------------------- 5 phút hợp nhất (không chồng lấn, không hở)
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_5min_unified SECURITY INVOKER AS
SELECT b.link_id, b.window_start_utc, b.window_end_utc, b.n_records, b.n_valid, b.n_zero, b.n_invalid_other,
       b.sum_speed, b.sum_speed_sq, b.sum_travel_time, b.min_speed, b.max_speed, b.avg_speed, b.quality_flag,
       'batch' AS layer
FROM lakehouse.gold.traffic_5min_batch FOR VERSION AS OF 'published' b
WHERE b.window_start_utc < (SELECT t_batch FROM lakehouse.serving.v_lambda_t_batch)
UNION ALL
SELECT s.link_id, s.window_start_utc, s.window_end_utc, s.n_records, s.n_valid, s.n_zero, s.n_invalid_other,
       s.sum_speed, s.sum_speed_sq, s.sum_travel_time, s.min_speed, s.max_speed, s.avg_speed, s.quality_flag,
       'speed' AS layer
FROM lakehouse.speed.traffic_5min_rt s
WHERE s.window_start_utc >= (SELECT t_batch FROM lakehouse.serving.v_lambda_t_batch);

-- ---------------------------------------------------------------- dimension link (vị trí, ô thời tiết, free-flow)
CREATE OR REPLACE VIEW lakehouse.serving.v_link_dim SECURITY INVOKER AS
SELECT l.link_id, l.link_name, l.borough, l.latitude, l.longitude, l.cell_id, l.cell_distance_km,
       b.free_flow_speed, b.n_samples AS free_flow_samples, b.baseline_version
FROM lakehouse.silver.link_info l
LEFT JOIN lakehouse.gold.link_baseline b ON b.link_id = l.link_id;

-- ---------------------------------------------------------------- thời tiết
-- Một bản / (ô, giờ): ưu tiên Silver (batch đã làm sạch), nếu chưa có thì bản realtime mới nhất (kafka_offset lớn nhất).
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

-- Thời tiết theo KHOẢNG [H, H+1):
--   nhiệt độ / độ ẩm / gió / gió giật: tức thời tại mốc H
--   mưa / tuyết / weather_code: bản ghi mốc H+1 (= tổng của giờ trước mốc) -> FINAL
--   chưa có mốc H+1 (realtime, giờ chưa kết thúc): tạm dùng của mốc H -> PROVISIONAL (tự thành FINAL khi mốc H+1 tới)
CREATE OR REPLACE VIEW lakehouse.serving.v_weather_interval SECURITY INVOKER AS
SELECT a.cell_id, a.time_utc AS interval_start,
       a.temperature_2m AS temperature_c, a.relative_humidity_2m AS relative_humidity_pct,
       a.wind_speed_10m AS wind_speed_kmh, a.wind_gusts_10m AS wind_gust_kmh,
       coalesce(b.precipitation, a.precipitation) AS precipitation_mm,
       coalesce(b.snowfall, a.snowfall) AS snowfall_cm,
       if(b.cell_id IS NOT NULL, b.weather_code, a.weather_code) AS weather_code,
       if(b.cell_id IS NOT NULL, 'FINAL', 'PROVISIONAL') AS weather_status
FROM lakehouse.serving.v_weather_hourly a
LEFT JOIN lakehouse.serving.v_weather_hourly b
       ON b.cell_id = a.cell_id AND b.time_utc = a.time_utc + INTERVAL '1' HOUR;

-- ---------------------------------------------------------------- 5 phút + trạng thái giao thông + thời tiết
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_5min_enriched SECURITY INVOKER AS
SELECT u.link_id, u.window_start_utc, u.window_end_utc, u.layer,
       u.n_records, u.n_valid, u.n_zero, u.n_invalid_other, u.sum_speed, u.sum_speed_sq, u.sum_travel_time,
       u.min_speed, u.max_speed, u.avg_speed, u.quality_flag,
       d.link_name, d.borough, d.cell_id, d.free_flow_speed,
       CASE WHEN d.link_id IS NULL OR d.cell_id IS NULL THEN 'NO_MAPPING'
            WHEN d.free_flow_speed IS NULL OR d.free_flow_speed <= 0 THEN 'NO_BASELINE'
            WHEN u.n_valid = 0 THEN 'NO_VALID_DATA'
            WHEN u.avg_speed / d.free_flow_speed < 0.5 THEN 'CONGESTED'
            WHEN u.avg_speed / d.free_flow_speed < 0.8 THEN 'SLOW'
            ELSE 'FREE_FLOW' END AS traffic_state,
       w.temperature_c, w.relative_humidity_pct, w.wind_speed_kmh, w.wind_gust_kmh,
       w.precipitation_mm, w.snowfall_cm, w.weather_code,
       CASE WHEN d.cell_id IS NULL THEN 'NO_MAPPING'
            WHEN w.cell_id IS NULL THEN 'MISSING'
            ELSE w.weather_status END AS weather_status
FROM lakehouse.serving.v_traffic_5min_unified u
LEFT JOIN lakehouse.serving.v_link_dim d ON d.link_id = u.link_id
LEFT JOIN lakehouse.serving.v_weather_interval w
       ON w.cell_id = d.cell_id AND w.interval_start = date_trunc('hour', u.window_start_utc);

-- ---------------------------------------------------------------- 1 giờ hợp nhất (OLAP)
-- Giờ nằm trọn trước T_batch: lấy thẳng gold.traffic_hourly (đã tính sẵn). Giờ chứa T_batch trở đi: cộng thành phần
-- từ 5 phút hợp nhất (giờ ranh giới có thể vừa batch vừa speed). Không AVG(AVG).
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_hourly_unified SECURITY INVOKER AS
SELECT h.link_id, h.hour_utc, h.n_records, h.n_valid, h.n_zero, h.n_invalid_other, h.sum_speed, h.sum_speed_sq,
       h.sum_travel_time, h.min_speed, h.max_speed, h.avg_speed, h.n_windows, 'batch' AS layers
FROM lakehouse.gold.traffic_hourly FOR VERSION AS OF 'published' h
WHERE h.hour_utc < date_trunc('hour', (SELECT t_batch FROM lakehouse.serving.v_lambda_t_batch))
UNION ALL
SELECT link_id, hour_utc, n_records, n_valid, n_zero, n_invalid_other, sum_speed, sum_speed_sq, sum_travel_time,
       min_speed, max_speed, if(n_valid > 0, sum_speed / n_valid) AS avg_speed, n_windows, layers
FROM (
    SELECT link_id, date_trunc('hour', window_start_utc) AS hour_utc,
           sum(n_records) AS n_records, sum(n_valid) AS n_valid, sum(n_zero) AS n_zero,
           sum(n_invalid_other) AS n_invalid_other, sum(sum_speed) AS sum_speed, sum(sum_speed_sq) AS sum_speed_sq,
           sum(sum_travel_time) AS sum_travel_time, min(min_speed) AS min_speed, max(max_speed) AS max_speed,
           CAST(count(*) AS integer) AS n_windows, array_join(array_sort(array_distinct(array_agg(layer))), ',') AS layers
    FROM lakehouse.serving.v_traffic_5min_unified
    WHERE window_start_utc >= date_trunc('hour', (SELECT t_batch FROM lakehouse.serving.v_lambda_t_batch))
    GROUP BY link_id, date_trunc('hour', window_start_utc));

-- ---------------------------------------------------------------- trạng thái mới nhất từng link
-- Đồng hồ sự kiện = event_time lớn nhất đã nhận trên toàn mạng (dữ liệu phát lại năm 2025, không dùng giờ hệ thống).
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_latest SECURITY INVOKER AS
WITH clock AS (SELECT max(obs_event_time) AS event_clock FROM lakehouse.speed.traffic_latest)
SELECT d.link_id, d.link_name, d.borough, d.free_flow_speed,
       l.valid_event_time AS last_valid_event_time, l.speed, l.travel_time,
       l.obs_event_time AS last_observation_time, l.obs_is_valid AS last_observation_valid,
       l.obs_invalid_reason AS last_invalid_reason, c.event_clock,
       date_diff('minute', l.valid_event_time, c.event_clock) AS valid_age_min,
       CASE WHEN l.link_id IS NULL THEN 'NO_DATA'
            WHEN l.valid_event_time IS NULL THEN 'NO_VALID_DATA'
            WHEN l.obs_event_time > l.valid_event_time AND NOT l.obs_is_valid THEN 'NO_VALID_DATA'
            WHEN date_diff('minute', l.valid_event_time, c.event_clock) > 15 THEN 'STALE'
            WHEN d.free_flow_speed IS NULL OR d.free_flow_speed <= 0 THEN 'NO_BASELINE'
            WHEN l.speed / d.free_flow_speed < 0.5 THEN 'CONGESTED'
            WHEN l.speed / d.free_flow_speed < 0.8 THEN 'SLOW'
            ELSE 'FREE_FLOW' END AS traffic_state,
       l.valid_kafka_ts AS kafka_ts, l.updated_at
FROM lakehouse.serving.v_link_dim d
LEFT JOIN lakehouse.speed.traffic_latest l ON l.link_id = d.link_id
CROSS JOIN clock c;
