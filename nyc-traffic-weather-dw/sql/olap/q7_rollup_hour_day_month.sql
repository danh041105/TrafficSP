-- Q7. Roll-up giờ → ngày → tháng (toàn mạng, tháng 12/2025). Các mức đều tính SUM(sum_speed)/SUM(n_valid).
SELECT d.month, d.full_date, t.time_key,
       round(sum(f.sum_speed) / NULLIF(sum(f.n_valid), 0), 2) AS avg_speed_mph,
       sum(f.n_valid)                                          AS n_valid_readings,
       grouping(d.month, d.full_date, t.time_key)              AS level   -- 0 = giờ, 1 = ngày, 3 = tháng
FROM fact_traffic_hourly f
JOIN dim_road_link r ON r.road_key = f.road_key
JOIN dim_date d      ON d.date_key = f.date_key
JOIN dim_time t      ON t.time_key = f.time_key
WHERE NOT r.is_low_quality AND d.month = 12
GROUP BY ROLLUP (d.month, d.full_date, t.time_key)
ORDER BY d.full_date NULLS FIRST, t.time_key NULLS FIRST;
