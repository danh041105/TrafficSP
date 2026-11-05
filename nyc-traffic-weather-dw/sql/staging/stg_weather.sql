-- Staging thoi tiet.
-- stg_weather_hourly: 1 o x 1 moc gio API (UTC), dung nhu JSON goc (bang tam _wx_long do Python nap tu JSON).
-- stg_weather_interval: 1 o x khoang [H, H+1) UTC. Bien tuc thoi lay @H; bien tich luy (mua, tuyet) va weather_code lay @H+1.
-- Tham so: {start_utc}, {end_utc_exclusive}, {batch_id}
CREATE OR REPLACE TABLE stg_weather_hourly AS
SELECT weather_location_id, time_utc, temperature_2m, relative_humidity_2m, precipitation, snowfall,
       wind_speed_10m, wind_gusts_10m, weather_code, model, grid_lat, grid_lon, elevation, source_file, api_downloaded_utc,
       '{batch_id}' AS load_batch_id
FROM _wx_long;

CREATE OR REPLACE TABLE stg_weather_interval AS
SELECT a.weather_location_id,
       a.time_utc                          AS interval_start_utc,
       a.time_utc + INTERVAL 1 HOUR        AS interval_end_utc,
       a.temperature_2m                    AS temperature_c,          -- @H (tuc thoi)
       a.relative_humidity_2m              AS relative_humidity_pct,  -- @H
       a.wind_speed_10m                    AS wind_speed_kmh,         -- @H
       a.wind_gusts_10m                    AS wind_gust_kmh,          -- @H ("of the indicated hour")
       b.precipitation                     AS precipitation_mm,       -- @H+1 ("sum of the preceding hour")
       b.snowfall                          AS snowfall_cm,            -- @H+1
       CAST(b.weather_code AS SMALLINT)    AS weather_code,           -- @H+1 (thuc nghiem 99.99%)
       a.model,
       a.source_file,
       b.time_utc IS NOT NULL              AS has_next_hour,
       '{batch_id}'                        AS load_batch_id
FROM stg_weather_hourly a
LEFT JOIN stg_weather_hourly b
       ON b.weather_location_id = a.weather_location_id
      AND b.time_utc = a.time_utc + INTERVAL 1 HOUR
WHERE a.time_utc >= TIMESTAMPTZ '{start_utc}' AND a.time_utc < TIMESTAMPTZ '{end_utc_exclusive}';
