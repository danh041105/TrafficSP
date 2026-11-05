-- Q1. Tốc độ trung bình theo borough và tháng (trung bình có trọng số theo số lần đo; bỏ link chất lượng thấp D-07)
SELECT r.borough, d.month,
       round(sum(f.sum_speed) / NULLIF(sum(f.n_valid), 0), 2) AS avg_speed_mph,
       sum(f.n_valid)                                          AS n_valid_readings,
       count(DISTINCT f.road_key)                              AS n_links
FROM fact_traffic_hourly f
JOIN dim_road_link r ON r.road_key = f.road_key
JOIN dim_date d      ON d.date_key = f.date_key
WHERE NOT r.is_low_quality
GROUP BY r.borough, d.month
ORDER BY r.borough, d.month;
