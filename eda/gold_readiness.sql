-- Read-only Trino EDA. Execute against lakehouse; no writes or Gold pipeline.
-- Q1: baseline, units/ranges, key/null readiness (one scan per source).
SELECT 'traffic' AS source, count(*) AS rows,
  count(DISTINCT link_id) AS links, min(traffic_utc) AS min_utc, max(traffic_utc) AS max_utc,
  min(date) AS min_local_date, max(date) AS max_local_date, count(DISTINCT date) AS local_days,
  count_if(link_id IS NULL OR traffic_utc IS NULL) AS null_keys,
  count_if(speed IS NULL) AS null_speed, count_if(speed = 0) AS zero_speed,
  count_if(speed < 0) AS negative_speed, min(speed) AS min_speed_mph, max(speed) AS max_speed_mph,
  count_if(travel_time IS NULL) AS null_travel_time, count_if(travel_time <= 0) AS nonpositive_travel_time,
  min(travel_time) AS min_travel_time_sec, max(travel_time) AS max_travel_time_sec
FROM lakehouse.silver.traffic_cleaned;

-- Q2: dependency coverage only. Exact DISTINCT over all ~9.5M traffic keys
-- exceeded the local Trino per-node memory budget; month checks are Q8 below.
SELECT count(*) AS rows,
  count_if(l.link_id IS NULL) AS no_link_info,
  count_if(l.cell_id IS NULL) AS no_grid_assignment,
  count_if(w.cell_id IS NULL) AS no_weather_at_traffic_hour
FROM lakehouse.silver.traffic_cleaned t
LEFT JOIN lakehouse.silver.link_info l ON t.link_id=l.link_id
LEFT JOIN lakehouse.silver.weather_cleaned w
 ON l.cell_id=w.cell_id AND date_trunc('hour',t.traffic_utc)=w.weather_utc;

-- Q3: weather coverage, nullability and ranges.
SELECT count(*) AS rows, count(DISTINCT cell_id) AS grids,
  count(*) - count(DISTINCT ROW(cell_id,weather_utc)) AS duplicate_keys,
  min(weather_utc) AS min_utc, max(weather_utc) AS max_utc,
  count_if(cell_id IS NULL OR weather_utc IS NULL) AS null_keys,
  count_if(minute(weather_utc)<>0 OR second(weather_utc)<>0) AS non_hourly_timestamps,
  count_if(latitude IS NULL OR longitude IS NULL) AS missing_coordinates,
  count_if(temperature_2m IS NULL OR precipitation IS NULL OR snowfall IS NULL
     OR wind_speed_10m IS NULL OR weather_code IS NULL OR relative_humidity_2m IS NULL) AS incomplete_metrics,
  min(temperature_2m) AS min_temp_c,max(temperature_2m) AS max_temp_c,
  min(precipitation) AS min_precip_mm,max(precipitation) AS max_precip_mm,
  min(snowfall) AS min_snow_cm,max(snowfall) AS max_snow_cm,
  min(wind_speed_10m) AS min_wind_kmh,max(wind_speed_10m) AS max_wind_kmh,
  min(relative_humidity_2m) AS min_humidity_pct,max(relative_humidity_2m) AS max_humidity_pct
FROM lakehouse.silver.weather_cleaned;

-- Q4: grid SCD1 consistency and temporal gaps.
SELECT cell_id,count(*) AS hours,
  count(DISTINCT ROW(latitude,longitude)) AS coordinate_versions,
  min(latitude) AS latitude,min(longitude) AS longitude,
  min(weather_utc) AS min_utc,max(weather_utc) AS max_utc,
  date_diff('hour',min(weather_utc),max(weather_utc))+1-count(DISTINCT weather_utc) AS missing_hours
FROM lakehouse.silver.weather_cleaned GROUP BY cell_id ORDER BY cell_id;

-- Q5: links, geometry availability; no historical road attributes exist here.
SELECT count(*) AS rows,count(DISTINCT link_id) AS distinct_links,
  count_if(link_name IS NULL) AS missing_name,count_if(borough IS NULL) AS missing_borough,
  count_if(link_points IS NULL OR trim(link_points)='') AS missing_geometry,
  count_if(cardinality(split(trim(link_points),' '))<2) AS fewer_than_two_points,
  count_if(cell_id IS NULL) AS missing_grid,
  min(cell_distance_km) AS min_grid_distance_km,max(cell_distance_km) AS max_grid_distance_km
FROM lakehouse.silver.link_info;

-- Q6: source enums and metadata consistency.
SELECT status,count(*) AS rows FROM lakehouse.silver.traffic_cleaned GROUP BY status ORDER BY status;
SELECT weather_code,count(*) AS rows FROM lakehouse.silver.weather_cleaned GROUP BY weather_code ORDER BY weather_code;
SELECT borough,count(*) AS links FROM lakehouse.silver.link_info GROUP BY borough ORDER BY borough;
SELECT link_id,count(DISTINCT transcom_id) AS transcom_versions,
 count_if(transcom_id IS NULL) AS missing_transcom
FROM lakehouse.silver.traffic_cleaned GROUP BY link_id
HAVING count(DISTINCT transcom_id)<>1 OR count_if(transcom_id IS NULL)>0 ORDER BY link_id LIMIT 20;

-- Q7: source cadence informs congestion weighting; summary only.
WITH gaps AS (
 SELECT date_diff('second',lag(traffic_utc) OVER (PARTITION BY link_id ORDER BY traffic_utc),traffic_utc) AS gap_sec
 FROM lakehouse.silver.traffic_cleaned
 WHERE date=DATE '2025-01-15'
)
SELECT count(*) AS readings,count_if(gap_sec IS NOT NULL) AS observed_gaps,
 min(gap_sec) AS min_gap_sec,approx_percentile(gap_sec,0.5) AS median_gap_sec,
 approx_percentile(gap_sec,0.95) AS p95_gap_sec,max(gap_sec) AS max_gap_sec FROM gaps;

-- Q8: exact traffic key uniqueness with partition pruning and bounded memory.
-- Run these nine statements sequentially, not as a parallel UNION.
-- A duplicate key cannot cross New York dates: date is derived from traffic_utc.
SELECT '2025-01' AS month,count(*) AS rows,count(*)-count(DISTINCT ROW(link_id,traffic_utc)) AS duplicate_keys
FROM lakehouse.silver.traffic_cleaned WHERE date>=DATE '2025-01-01' AND date<DATE '2025-02-01';
SELECT '2025-02' AS month,count(*) AS rows,count(*)-count(DISTINCT ROW(link_id,traffic_utc)) AS duplicate_keys
FROM lakehouse.silver.traffic_cleaned WHERE date>=DATE '2025-02-01' AND date<DATE '2025-03-01';
SELECT '2025-03' AS month,count(*) AS rows,count(*)-count(DISTINCT ROW(link_id,traffic_utc)) AS duplicate_keys
FROM lakehouse.silver.traffic_cleaned WHERE date>=DATE '2025-03-01' AND date<DATE '2025-04-01';
SELECT '2025-04' AS month,count(*) AS rows,count(*)-count(DISTINCT ROW(link_id,traffic_utc)) AS duplicate_keys
FROM lakehouse.silver.traffic_cleaned WHERE date>=DATE '2025-04-01' AND date<DATE '2025-05-01';
SELECT '2025-05' AS month,count(*) AS rows,count(*)-count(DISTINCT ROW(link_id,traffic_utc)) AS duplicate_keys
FROM lakehouse.silver.traffic_cleaned WHERE date>=DATE '2025-05-01' AND date<DATE '2025-06-01';
SELECT '2025-06' AS month,count(*) AS rows,count(*)-count(DISTINCT ROW(link_id,traffic_utc)) AS duplicate_keys
FROM lakehouse.silver.traffic_cleaned WHERE date>=DATE '2025-06-01' AND date<DATE '2025-07-01';
SELECT '2025-07' AS month,count(*) AS rows,count(*)-count(DISTINCT ROW(link_id,traffic_utc)) AS duplicate_keys
FROM lakehouse.silver.traffic_cleaned WHERE date>=DATE '2025-07-01' AND date<DATE '2025-08-01';
SELECT '2025-08' AS month,count(*) AS rows,count(*)-count(DISTINCT ROW(link_id,traffic_utc)) AS duplicate_keys
FROM lakehouse.silver.traffic_cleaned WHERE date>=DATE '2025-08-01' AND date<DATE '2025-09-01';
SELECT '2025-09' AS month,count(*) AS rows,count(*)-count(DISTINCT ROW(link_id,traffic_utc)) AS duplicate_keys
FROM lakehouse.silver.traffic_cleaned WHERE date>=DATE '2025-09-01' AND date<DATE '2025-10-01';

-- Q9: distinguish zero/negative travel time from zero speed before congestion.
SELECT count_if(travel_time=0) AS zero_travel_time,
 count_if(travel_time<0) AS negative_travel_time,
 count_if(speed=0 AND travel_time=0) AS both_zero,
 count_if(speed=0 AND travel_time>0) AS zero_speed_positive_time,
 count_if(speed>0 AND travel_time=0) AS positive_speed_zero_time,
 count_if(travel_time>0) AS positive_travel_time
FROM lakehouse.silver.traffic_cleaned;
