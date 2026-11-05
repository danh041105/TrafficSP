-- Q2. Tốc độ khi có mưa / tuyết so với lúc khô, theo khung giờ (dice: điều kiện × khung giờ). Ngày thường, link đạt chất lượng, giờ có ≥ 6 lần đo hợp lệ.
WITH m AS (
  SELECT f.*, t.day_part,
         CASE WHEN f.snowfall_cm > 0          THEN '4. Tuyết'
              WHEN f.precipitation_mm >= 1    THEN '3. Mưa ≥ 1 mm/h'
              WHEN f.precipitation_mm >= 0.1  THEN '2. Mưa nhỏ 0,1–1 mm/h'
              ELSE '1. Khô' END AS weather_class
  FROM fact_traffic_weather_hourly f
  JOIN dim_road_link r ON r.road_key = f.road_key
  JOIN dim_date d      ON d.date_key = f.date_key
  JOIN dim_time t      ON t.time_key = f.time_key
  WHERE NOT r.is_low_quality AND d.day_type = 'Weekday' AND f.n_valid >= 6 AND NOT f.is_network_outage
)
SELECT day_part, weather_class,
       round(sum(sum_speed) / sum(n_valid), 2) AS avg_speed_mph,
       count(*)                                AS link_hours,
       round(100 * (sum(sum_speed) / sum(n_valid)) /
             (sum(sum(sum_speed)) FILTER (WHERE weather_class = '1. Khô') OVER (PARTITION BY day_part)
              / sum(sum(n_valid)) FILTER (WHERE weather_class = '1. Khô') OVER (PARTITION BY day_part)) - 100, 1) AS pct_vs_dry
FROM m
GROUP BY day_part, weather_class
ORDER BY day_part, weather_class;
