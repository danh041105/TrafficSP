-- Q8. Drill-across: ghép hai fact khác quy trình qua dimension dùng chung (dim_date) mà không dùng mart.
-- Lượng mưa khu vực = trung bình của tổng ngày từng ô (KHÔNG cộng qua link/ô).
WITH traffic AS (
  SELECT f.date_key, sum(f.sum_speed) / sum(f.n_valid) AS avg_speed_mph
  FROM fact_traffic_hourly f JOIN dim_road_link r USING (road_key)
  WHERE NOT r.is_low_quality GROUP BY 1),
weather AS (
  SELECT date_key, avg(p) AS precip_mm_day, avg(s) AS snow_cm_day, avg(tmin) AS tmin_c
  FROM (SELECT date_key, weather_loc_key, sum(precipitation_mm) p, sum(snowfall_cm) s, min(temperature_c) tmin
        FROM fact_weather_hourly GROUP BY 1, 2) GROUP BY 1)
SELECT d.full_date, d.day_type, round(t.avg_speed_mph, 2) avg_speed_mph,
       round(w.precip_mm_day, 1) precip_mm, round(w.snow_cm_day, 1) snow_cm, round(w.tmin_c, 1) tmin_c
FROM traffic t JOIN weather w USING (date_key) JOIN dim_date d USING (date_key)
ORDER BY w.snow_cm_day DESC, w.precip_mm_day DESC
LIMIT 10;
