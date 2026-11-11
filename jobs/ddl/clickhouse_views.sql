-- View phục vụ truy vấn (Query Layer). Mọi view đọc qua FINAL, không giả định merge nền đã khử trùng.
-- {stale_after_minutes} được runner điền từ config.STALE_AFTER_MINUTES.

-- Thời tiết theo KHOẢNG [H, H+1) (quy tắc nhánh khai, D-09):
--   nhiệt độ / độ ẩm / gió / gió giật: giá trị tức thời tại mốc H
--   mưa / tuyết / weather_code:        bản ghi mốc H+1 (= tổng của giờ trước mốc)  -> weather_status = FINAL
-- Realtime: khi giờ H chưa kết thúc thì chưa có mốc H+1. Khi đó dùng tạm mưa/tuyết của mốc H (giờ trước đó)
-- và đánh dấu PROVISIONAL. Batch recompute / truy vấn lại sau khi có mốc H+1 sẽ tự thành FINAL.
CREATE OR REPLACE VIEW traffic.v_weather_interval AS
WITH w AS (
    SELECT cell_id, time_utc, temperature_2m, relative_humidity_2m, precipitation, snowfall,
           wind_speed_10m, wind_gusts_10m, weather_code
    FROM traffic.weather_hourly FINAL
)
SELECT a.cell_id                                              AS cell_id,
       a.time_utc                                             AS interval_start,
       a.temperature_2m                                       AS temperature_c,
       a.relative_humidity_2m                                 AS relative_humidity_pct,
       a.wind_speed_10m                                       AS wind_speed_kmh,
       a.wind_gusts_10m                                       AS wind_gust_kmh,
       if(b.present = 1, b.precipitation, a.precipitation)    AS precipitation_mm,
       if(b.present = 1, b.snowfall, a.snowfall)              AS snowfall_cm,
       if(b.present = 1, b.weather_code, a.weather_code)      AS weather_code,
       if(b.present = 1, 'FINAL', 'PROVISIONAL')              AS weather_status
FROM (SELECT *, time_utc + INTERVAL 1 HOUR AS next_time FROM w) AS a
LEFT JOIN (SELECT cell_id, time_utc, precipitation, snowfall, weather_code, toUInt8(1) AS present FROM w) AS b
       ON b.cell_id = a.cell_id AND b.time_utc = a.next_time;

-- T_batch hiện hành: 1970-01-01 nếu chưa publish lần nào (=> toàn bộ lấy từ speed layer)
CREATE OR REPLACE VIEW traffic.v_lambda_t_batch AS
SELECT max(t_batch) AS t_batch FROM traffic.lambda_handoff WHERE status = 'PUBLISHED';

-- Hợp nhất Lambda ở grain 5 phút: batch cho window_start < T_batch, speed cho window_start >= T_batch (không chồng lấn)
CREATE OR REPLACE VIEW traffic.v_traffic_5min_unified AS
SELECT link_id, window_start, window_end, n_records, n_valid, n_zero, n_invalid_other,
       sum_speed, sum_speed_sq, sum_travel_time, min_speed, max_speed, 'batch' AS layer
FROM traffic.traffic_5min_batch FINAL
WHERE window_start < (SELECT t_batch FROM traffic.v_lambda_t_batch)
UNION ALL
SELECT link_id, window_start, window_end, n_records, n_valid, n_zero, n_invalid_other,
       sum_speed, sum_speed_sq, sum_travel_time, min_speed, max_speed, 'speed' AS layer
FROM traffic.traffic_5min_rt FINAL
WHERE window_start >= (SELECT t_batch FROM traffic.v_lambda_t_batch);

-- 5 phút + borough + trạng thái giao thông (D-18) + thời tiết theo khoảng giờ
CREATE OR REPLACE VIEW traffic.v_traffic_5min_enriched AS
SELECT u.link_id AS link_id, u.window_start AS window_start, u.window_end AS window_end, u.layer AS layer,
       u.n_records AS n_records, u.n_valid AS n_valid, u.n_zero AS n_zero, u.n_invalid_other AS n_invalid_other,
       u.sum_speed AS sum_speed, u.sum_speed_sq AS sum_speed_sq, u.sum_travel_time AS sum_travel_time,
       u.min_speed AS min_speed, u.max_speed AS max_speed,
       if(u.n_valid > 0, u.sum_speed / u.n_valid, NULL)                    AS avg_speed,
       d.link_name AS link_name, d.borough AS borough, d.cell_id AS cell_id, d.free_flow_speed AS free_flow_speed,
       multiIf(d.cell_id = '', 'NO_MAPPING',
               d.free_flow_speed IS NULL OR d.free_flow_speed <= 0, 'NO_BASELINE',
               u.n_valid = 0, 'NO_VALID_DATA',
               (u.sum_speed / u.n_valid) / d.free_flow_speed < 0.5, 'CONGESTED',
               (u.sum_speed / u.n_valid) / d.free_flow_speed < 0.8, 'SLOW',
               'FREE_FLOW')                                                 AS traffic_state,
       w.temperature_c AS temperature_c, w.relative_humidity_pct AS relative_humidity_pct,
       w.wind_speed_kmh AS wind_speed_kmh, w.wind_gust_kmh AS wind_gust_kmh,
       w.precipitation_mm AS precipitation_mm, w.snowfall_cm AS snowfall_cm, w.weather_code AS weather_code,
       multiIf(d.cell_id = '', 'NO_MAPPING', w.cell_id = '', 'MISSING', w.weather_status) AS weather_status
FROM (SELECT *, toStartOfHour(window_start) AS hour_start FROM traffic.v_traffic_5min_unified) AS u
LEFT JOIN (SELECT * FROM traffic.link_dim FINAL) AS d ON d.link_id = u.link_id
LEFT JOIN traffic.v_weather_interval AS w ON w.cell_id = d.cell_id AND w.interval_start = u.hour_start;

-- Roll-up 1 giờ từ grain 5 phút hợp nhất: chỉ cộng thành phần, tính lại trung bình (không AVG(AVG))
-- (tên cột tầng trong có hậu tố _h để alias không che tên cột gốc bên trong hàm sum)
CREATE OR REPLACE VIEW traffic.v_traffic_hourly_unified AS
SELECT link_id, hour_utc,
       n_records_h AS n_records, n_valid_h AS n_valid, n_zero_h AS n_zero, n_invalid_other_h AS n_invalid_other,
       sum_speed_h AS sum_speed, sum_speed_sq_h AS sum_speed_sq, sum_travel_time_h AS sum_travel_time,
       min_speed_h AS min_speed, max_speed_h AS max_speed,
       if(n_valid_h > 0, sum_speed_h / n_valid_h, NULL) AS avg_speed,
       layers
FROM (
    SELECT link_id, toStartOfHour(window_start) AS hour_utc,
           sum(n_records) AS n_records_h, sum(n_valid) AS n_valid_h, sum(n_zero) AS n_zero_h, sum(n_invalid_other) AS n_invalid_other_h,
           sum(sum_speed) AS sum_speed_h, sum(sum_speed_sq) AS sum_speed_sq_h, sum(sum_travel_time) AS sum_travel_time_h,
           min(min_speed) AS min_speed_h, max(max_speed) AS max_speed_h, groupUniqArray(layer) AS layers
    FROM traffic.v_traffic_5min_unified
    GROUP BY link_id, hour_utc
);

-- Trạng thái mới nhất từng link (grain 1 link). Đồng hồ sự kiện = event_time lớn nhất đã nhận (không dùng giờ hệ thống 2026).
CREATE OR REPLACE VIEW traffic.v_traffic_latest AS
WITH (SELECT max(event_time) FROM traffic.traffic_latest_any FINAL) AS event_clock
SELECT d.link_id AS link_id, d.link_name AS link_name, d.borough AS borough, d.free_flow_speed AS free_flow_speed,
       if(v.link_id = 0, NULL, v.event_time)        AS last_valid_event_time,
       if(v.link_id = 0, NULL, v.speed)             AS speed,
       if(v.link_id = 0, NULL, v.travel_time)       AS travel_time,
       if(a.link_id = 0, NULL, a.event_time)        AS last_observation_time,
       if(a.link_id = 0, NULL, a.is_valid)          AS last_observation_valid,
       a.invalid_reason                             AS last_invalid_reason,
       event_clock,
       if(v.link_id = 0, NULL, dateDiff('minute', v.event_time, event_clock)) AS valid_age_min,
       multiIf(v.link_id = 0 AND a.link_id = 0, 'NO_DATA',
               v.link_id = 0, 'NO_VALID_DATA',
               a.event_time > v.event_time AND a.is_valid = 0, 'NO_VALID_DATA',
               dateDiff('minute', v.event_time, event_clock) > {stale_after_minutes}, 'STALE',
               d.free_flow_speed IS NULL OR d.free_flow_speed <= 0, 'NO_BASELINE',
               v.speed / d.free_flow_speed < 0.5, 'CONGESTED',
               v.speed / d.free_flow_speed < 0.8, 'SLOW',
               'FREE_FLOW')                         AS traffic_state,
       v.updated_at AS updated_at, v.kafka_ts AS kafka_ts
FROM (SELECT * FROM traffic.link_dim FINAL) AS d
LEFT JOIN (SELECT * FROM traffic.traffic_latest FINAL) AS v ON v.link_id = d.link_id
LEFT JOIN (SELECT * FROM traffic.traffic_latest_any FINAL) AS a ON a.link_id = d.link_id;
