-- Q5. Đoạn đường Manhattan lúc 17:00 (giờ New York) ngày lễ 04/07/2025: tốc độ, trạng thái, thời tiết.
-- Trạng thái = tốc độ / free_flow_speed_mph (p85 lúc khô, 22–05h): Ùn tắc < 0,5; Chậm < 0,8; Thông thoáng ≥ 0,8.
-- ĐÂY LÀ QUY TẮC PHÂN TÍCH (D-18), không phải tiêu chuẩn chính thức.
SELECT r.link_name, d.holiday_name, t.hour_label,
       round(f.sum_speed / NULLIF(f.n_valid, 0), 1)  AS avg_speed_mph,
       f.n_valid,
       round(r.free_flow_speed_mph, 1)               AS free_flow_mph,
       CASE WHEN f.n_valid < 6 OR r.free_flow_speed_mph IS NULL THEN 'Không xác định'
            WHEN f.sum_speed / f.n_valid < 0.5 * r.free_flow_speed_mph THEN 'Ùn tắc'
            WHEN f.sum_speed / f.n_valid < 0.8 * r.free_flow_speed_mph THEN 'Chậm'
            ELSE 'Thông thoáng' END                  AS traffic_status,
       c.description_vi                              AS weather,
       f.temperature_c, f.precipitation_mm, f.wind_gust_kmh
FROM fact_traffic_weather_hourly f
JOIN dim_road_link r          ON r.road_key = f.road_key
JOIN dim_date d               ON d.date_key = f.date_key
JOIN dim_time t               ON t.time_key = f.time_key
JOIN dim_weather_condition c  ON c.condition_key = f.condition_key
WHERE r.borough = 'Manhattan' AND t.time_key = 17 AND d.full_date = DATE '2025-07-04'
ORDER BY avg_speed_mph NULLS LAST;
