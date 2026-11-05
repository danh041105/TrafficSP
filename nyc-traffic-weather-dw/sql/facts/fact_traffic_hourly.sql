-- fact_traffic_hourly: luoi day du link x gio UTC trong [gio dau, gio cuoi] link co du lieu.
-- Measures cong duoc (n_*, sum_*), semi (min/max), non-additive (median, coverage_ratio).
-- Tham so: {expected_per_hour}, {outage_share}, {amb_date}, {amb_hour}, {batch_id}
CREATE OR REPLACE TABLE _agg AS
SELECT road_key,
       date_trunc('hour', reading_ts_utc)                                   AS hour_utc,
       count(*)                                                             AS n_records,
       count(*) FILTER (WHERE is_valid)                                     AS n_valid,
       count(*) FILTER (WHERE speed_mph = 0)                                AS n_zero,
       count(*) FILTER (WHERE NOT is_valid AND speed_mph <> 0)              AS n_invalid_other,
       sum(speed_mph) FILTER (WHERE is_valid)                               AS sum_speed,
       sum(speed_mph * speed_mph) FILTER (WHERE is_valid)                   AS sum_speed_sq,
       sum(travel_time_s) FILTER (WHERE is_valid)                           AS sum_travel_time,
       min(speed_mph) FILTER (WHERE is_valid)                               AS min_speed,
       max(speed_mph) FILTER (WHERE is_valid)                               AS max_speed,
       median(speed_mph) FILTER (WHERE is_valid)                            AS median_speed,
       bool_or(is_stuck)                                                    AS has_stuck
FROM fact_traffic_reading
WHERE reading_ts_utc IS NOT NULL          -- ban ghi gio DST kep khong gan duoc gio UTC (D-03), duoc dem trong doi chieu
GROUP BY 1, 2;

CREATE OR REPLACE TABLE _grid AS
SELECT road_key, unnest(generate_series(min_h, max_h, INTERVAL 1 HOUR)) AS hour_utc
FROM (SELECT road_key, min(hour_utc) AS min_h, max(hour_utc) AS max_h FROM _agg GROUP BY 1);

CREATE OR REPLACE TABLE _full AS
SELECT g.road_key, g.hour_utc,
       coalesce(a.n_records, 0)::SMALLINT       AS n_records,
       coalesce(a.n_valid, 0)::SMALLINT         AS n_valid,
       coalesce(a.n_zero, 0)::SMALLINT          AS n_zero,
       coalesce(a.n_invalid_other, 0)::SMALLINT AS n_invalid_other,
       a.sum_speed, a.sum_speed_sq, a.sum_travel_time::BIGINT AS sum_travel_time,
       a.min_speed, a.max_speed, a.median_speed,
       coalesce(a.has_stuck, FALSE)             AS has_stuck,
       timezone('America/New_York', g.hour_utc) AS hour_local
FROM _grid g LEFT JOIN _agg a USING (road_key, hour_utc);

CREATE OR REPLACE TABLE fact_traffic_hourly AS
WITH hr AS (
    SELECT hour_utc,
           (CAST(any_value(hour_local) AS DATE) = DATE '{amb_date}' AND hour(any_value(hour_local)) = {amb_hour}) AS is_dst_ambiguous_hour,
           count(*) AS active_links,
           count(*) FILTER (WHERE n_records = 0) AS links_no_record
    FROM _full GROUP BY 1
)
SELECT f.road_key, f.hour_utc,
       strftime(f.hour_local, '%Y%m%d')::INTEGER     AS date_key,
       hour(f.hour_local)::TINYINT                   AS time_key,
       f.n_records, f.n_valid, f.n_zero, f.n_invalid_other,
       f.sum_speed, f.sum_speed_sq, f.sum_travel_time, f.min_speed, f.max_speed, f.median_speed,
       f.n_valid / {expected_per_hour}::DOUBLE       AS coverage_ratio,
       f.has_stuck,
       (NOT hr.is_dst_ambiguous_hour AND hr.links_no_record >= {outage_share} * hr.active_links) AS is_network_outage,
       hr.is_dst_ambiguous_hour,
       '{batch_id}'                                  AS load_batch_id,
       strftime(f.hour_utc, '%Y-%m')                 AS month_utc
FROM _full f JOIN hr USING (hour_utc);
