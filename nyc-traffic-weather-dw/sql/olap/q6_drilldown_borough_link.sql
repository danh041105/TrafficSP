-- Q6. Drill-down borough → road link (ROLLUP). Dòng link_name = NULL là tổng của borough; dòng borough = NULL là toàn mạng.
SELECT r.borough, r.link_name,
       round(sum(f.sum_speed) / sum(f.n_valid), 2) AS avg_speed_mph,
       sum(f.n_valid)                              AS n_valid_readings,
       grouping(r.borough, r.link_name)            AS level   -- 3 = NYC, 1 = borough, 0 = link
FROM fact_traffic_hourly f JOIN dim_road_link r ON r.road_key = f.road_key
WHERE NOT r.is_low_quality
GROUP BY ROLLUP (r.borough, r.link_name)
ORDER BY r.borough NULLS FIRST, level DESC, avg_speed_mph;
