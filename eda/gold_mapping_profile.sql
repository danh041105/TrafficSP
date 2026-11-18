-- P01_status_quality
SELECT status,count(*) AS n,count(DISTINCT link_id) AS links,
 count_if(speed IS NULL) AS null_speed,count_if(speed=0) AS zero_speed,
 count_if(speed<0) AS negative_speed,count_if(travel_time IS NULL) AS null_time,
 count_if(travel_time=0) AS zero_time,count_if(travel_time<0) AS negative_time,
 count_if(speed=0 AND travel_time>0) AS zero_speed_positive_time,
 min(speed) AS min_speed,max(speed) AS max_speed,min(travel_time) AS min_time,max(travel_time) AS max_time,
 approx_percentile(speed,ARRAY[0.5,0.85,0.95,0.99]) AS speed_percentiles
FROM lakehouse.silver.traffic_cleaned GROUP BY status ORDER BY status;

-- P02_month_filter_impact
SELECT date_trunc('month',date) AS month,count(*) AS total,
 count_if(status=-101) AS excluded,count_if(status IS NULL OR status<>-101) AS retained,
 count_if((status IS NULL OR status<>-101) AND travel_time>0 AND speed>=0) AS congestion_candidates,
 count(DISTINCT IF(status IS NULL OR status<>-101,link_id,NULL)) AS retained_links
FROM lakehouse.silver.traffic_cleaned GROUP BY 1 ORDER BY 1;

-- P03_retained_key_time_coverage
SELECT count(*) AS retained_rows,count(DISTINCT t.link_id) AS retained_links,
 count(DISTINCT date) AS local_days,min(traffic_utc) AS first_utc,max(traffic_utc) AS last_utc,
 count_if(t.link_id IS NULL OR traffic_utc IS NULL) AS null_keys,
 count_if(l.link_id IS NULL) AS no_link,count_if(l.cell_id IS NULL) AS no_grid,
 count_if(w.cell_id IS NULL) AS no_weather,
 count_if(date<>CAST(at_timezone(traffic_utc,'America/New_York') AS date)) AS local_date_mismatch,
 count_if(hour<>hour(at_timezone(traffic_utc,'America/New_York'))) AS local_hour_mismatch,
 count_if(t.transcom_id IS NULL) AS missing_transcom
FROM lakehouse.silver.traffic_cleaned t
LEFT JOIN lakehouse.silver.link_info l ON t.link_id=l.link_id
LEFT JOIN lakehouse.silver.weather_cleaned w ON l.cell_id=w.cell_id AND date_trunc('hour',t.traffic_utc)=w.weather_utc
WHERE t.status IS NULL OR t.status<>-101;

-- P04_retained_link_quality
SELECT link_id,count(*) AS n,count_if(travel_time<=0) AS nonpositive_time,
 count_if(speed=0) AS zero_speed,count(DISTINCT transcom_id) AS transcom_versions,
 approx_percentile(IF(speed>0 AND travel_time>0,speed*0.44704*travel_time,NULL),ARRAY[0.1,0.5,0.9]) AS implied_length_m_percentiles,
 count_if(hour BETWEEN 0 AND 5 AND speed>0 AND travel_time>0) AS night_positive_n,
 approx_percentile(IF(hour BETWEEN 0 AND 5 AND speed>0 AND travel_time>0,speed,NULL),0.85) AS night_p85_mph
FROM lakehouse.silver.traffic_cleaned WHERE status IS NULL OR status<>-101
GROUP BY link_id ORDER BY link_id;

-- P05_retained_hour_coverage
WITH x AS (
 SELECT link_id,date_trunc('hour',traffic_utc) AS h,count(*) AS n,
 count_if(status IS NULL OR status<>-101) AS kept,
 count_if((status IS NULL OR status<>-101) AND speed>=0 AND travel_time>0) AS eligible
 FROM lakehouse.silver.traffic_cleaned GROUP BY 1,2
)
SELECT count(*) AS observed_link_hours,count_if(kept=0) AS hours_lost_to_status,
 count_if(kept>0) AS retained_link_hours,count_if(eligible>0) AS eligible_link_hours,
 approx_percentile(kept,ARRAY[0.1,0.5,0.9]) AS kept_readings_per_observed_hour,
 count_if(kept>0 AND kept<6) AS retained_hours_under_six_readings
FROM x;

-- P06_weather_profile
SELECT count(*) AS n,count(DISTINCT cell_id) AS grids,
 count(*)-count(DISTINCT ROW(cell_id,weather_utc)) AS duplicate_keys,
 min(weather_utc) AS first_utc,max(weather_utc) AS last_utc,
 count_if(temperature_2m IS NULL OR relative_humidity_2m IS NULL OR precipitation IS NULL OR snowfall IS NULL OR wind_speed_10m IS NULL OR weather_code IS NULL) AS missing_metrics,
 min(temperature_2m) AS min_temp,max(temperature_2m) AS max_temp,
 approx_percentile(precipitation,ARRAY[0.5,0.9,0.95,0.99]) AS precip_percentiles,
 approx_percentile(IF(precipitation>0,precipitation,NULL),ARRAY[0.25,0.5,0.75,0.9,0.95,0.99]) AS positive_precip_percentiles,
 count_if(precipitation>0) AS wet_hours,count_if(snowfall>0) AS snow_hours,
 min(wind_speed_10m/1.609344) AS min_wind_mph,max(wind_speed_10m/1.609344) AS max_wind_mph,
 count_if(precipitation<0 OR snowfall<0 OR wind_speed_10m<0 OR relative_humidity_2m<0 OR relative_humidity_2m>100) AS out_of_range
FROM lakehouse.silver.weather_cleaned;

-- P07_weather_codes
SELECT weather_code,count(*) AS n FROM lakehouse.silver.weather_cleaned GROUP BY 1 ORDER BY 1;

-- P08_geometry_and_grid
SELECT link_id,link_name,borough,link_points,latitude,longitude,cell_id,cell_distance_km
FROM lakehouse.silver.link_info ORDER BY link_id;

-- P09_runtime_grid_types
SELECT table_name,column_name,data_type FROM lakehouse.information_schema.columns
WHERE table_schema='gold' AND column_name IN ('grid_key','nearest_grid_key') ORDER BY 1,2;

-- P10_weather_grid_coordinates
SELECT cell_id,count(*) AS n,min(latitude) AS latitude,max(latitude) AS max_latitude,
 min(longitude) AS longitude,max(longitude) AS max_longitude,
 date_diff('hour',min(weather_utc),max(weather_utc))+1-count(DISTINCT weather_utc) AS missing_hours
FROM lakehouse.silver.weather_cleaned GROUP BY 1 ORDER BY 1;

-- P11_night_dry_freeflow_candidates
SELECT t.link_id,count(*) AS n,approx_percentile(t.speed,ARRAY[0.5,0.85,0.95]) AS speed_percentiles
FROM lakehouse.silver.traffic_cleaned t
JOIN lakehouse.silver.link_info l ON l.link_id=t.link_id
JOIN lakehouse.silver.weather_cleaned w ON w.cell_id=l.cell_id AND w.weather_utc=date_trunc('hour',t.traffic_utc)
WHERE (t.status IS NULL OR t.status<>-101) AND t.hour BETWEEN 0 AND 5
AND t.speed>0 AND t.travel_time>0 AND w.precipitation=0
GROUP BY t.link_id ORDER BY t.link_id;

-- P12_calendar_bounds
SELECT min(d) AS min_local_date,max(d) AS max_local_date FROM (
 SELECT CAST(at_timezone(traffic_utc,'America/New_York') AS date) AS d
 FROM lakehouse.silver.traffic_cleaned WHERE status IS NULL OR status<>-101
 UNION ALL
 SELECT CAST(at_timezone(weather_utc,'America/New_York') AS date) AS d FROM lakehouse.silver.weather_cleaned
);

-- P13_status_by_local_hour
SELECT hour AS local_hour,count(*) AS total,count_if(status=-101) AS excluded,
 count_if(status IS NULL OR status<>-101) AS retained
FROM lakehouse.silver.traffic_cleaned GROUP BY 1 ORDER BY 1;

-- P14_status_by_borough
SELECT l.borough,count(*) AS total,count_if(t.status=-101) AS excluded,
 count_if(t.status IS NULL OR t.status<>-101) AS retained,
 count(DISTINCT t.link_id) AS total_links,
 count(DISTINCT IF(t.status IS NULL OR t.status<>-101,t.link_id,NULL)) AS retained_links
FROM lakehouse.silver.traffic_cleaned t JOIN lakehouse.silver.link_info l ON t.link_id=l.link_id
GROUP BY 1 ORDER BY 1;

-- P15_draft_weather_band_combinations
WITH bands AS (
 SELECT CASE WHEN precipitation=0 THEN 'dry' WHEN precipitation<2.5 THEN 'light' WHEN precipitation<7.5 THEN 'moderate' ELSE 'heavy' END AS precip_band,
 CASE WHEN snowfall=0 THEN 'none' WHEN snowfall<0.5 THEN 'light' WHEN snowfall<1 THEN 'moderate' ELSE 'heavy' END AS snow_band,
 CASE WHEN wind_speed_10m/1.609344<5 THEN 'low' WHEN wind_speed_10m/1.609344<15 THEN 'moderate' ELSE 'high' END AS wind_band,
 CASE WHEN temperature_2m<0 THEN 'freezing' WHEN temperature_2m<10 THEN 'cold' WHEN temperature_2m<25 THEN 'mild' ELSE 'hot' END AS temp_band,
 CASE WHEN weather_code IN (0,1) THEN 'clear' WHEN weather_code IN (2,3) THEN 'cloudy'
 WHEN weather_code IN (45,48) THEN 'fog' WHEN weather_code IN (51,53,55,56,57) THEN 'drizzle'
 WHEN weather_code IN (61,63,65,66,67,80,81,82) THEN 'rain'
 WHEN weather_code IN (71,73,75,77,85,86) THEN 'snow'
 WHEN weather_code IN (95,96,99) THEN 'thunderstorm' ELSE 'unknown' END AS weather_group
 FROM lakehouse.silver.weather_cleaned
)
SELECT precip_band,snow_band,wind_band,temp_band,weather_group,count(*) AS n
FROM bands GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5;
