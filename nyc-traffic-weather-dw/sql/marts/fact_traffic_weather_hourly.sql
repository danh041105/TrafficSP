-- Mart OLAP: fact_traffic_hourly + thoi tiet cua o duoc gan (N:1 qua dim_road_link, N:1 qua (o, gio)).
-- Tham so: {batch_id}
CREATE OR REPLACE TABLE fact_traffic_weather_hourly AS
SELECT t.road_key, t.hour_utc, t.date_key, t.time_key,
       r.weather_loc_key,
       coalesce(w.condition_key, -1)::SMALLINT AS condition_key,
       t.n_records, t.n_valid, t.n_zero, t.n_invalid_other,
       t.sum_speed, t.sum_speed_sq, t.sum_travel_time, t.min_speed, t.max_speed, t.median_speed, t.coverage_ratio,
       t.has_stuck, t.is_network_outage, t.is_dst_ambiguous_hour,
       w.temperature_c, w.relative_humidity_pct, w.wind_speed_kmh, w.wind_gust_kmh, w.precipitation_mm, w.snowfall_cm,
       '{batch_id}' AS load_batch_id,
       t.month_utc
FROM fact_traffic_hourly t
JOIN dim_road_link r ON r.road_key = t.road_key
LEFT JOIN fact_weather_hourly w ON w.weather_loc_key = r.weather_loc_key AND w.interval_start_utc = t.hour_utc;
