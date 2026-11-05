-- Q4. Ngày thường / cuối tuần / ngày lễ theo khung giờ (slice theo day_type)
SELECT d.day_type, t.day_part,
       round(sum(f.sum_speed) / sum(f.n_valid), 2) AS avg_speed_mph,
       count(DISTINCT d.full_date)                 AS n_days
FROM fact_traffic_hourly f
JOIN dim_road_link r ON r.road_key = f.road_key
JOIN dim_date d      ON d.date_key = f.date_key
JOIN dim_time t      ON t.time_key = f.time_key
WHERE NOT r.is_low_quality
GROUP BY d.day_type, t.day_part
ORDER BY t.day_part, d.day_type;
