-- Q3. Top 10 đoạn đường có tốc độ trung bình thấp nhất năm 2025 (link đạt chất lượng, ≥ 10 000 lần đo hợp lệ)
SELECT r.link_id, r.link_name, r.borough,
       round(sum(f.sum_speed) / sum(f.n_valid), 2) AS avg_speed_mph,
       sum(f.n_valid)                              AS n_valid_readings,
       round(r.free_flow_speed_mph, 1)             AS free_flow_mph
FROM fact_traffic_hourly f JOIN dim_road_link r ON r.road_key = f.road_key
WHERE NOT r.is_low_quality
GROUP BY ALL
HAVING sum(f.n_valid) >= 10000
ORDER BY avg_speed_mph
LIMIT 10;
