-- Staging giao thong: giu toan bo ban ghi nguon, khong sua gia tri goc, chi them cot chuan hoa + co.
-- Tham so: {raw_glob}, {batch_id}, {amb_date}, {amb_hour}, {nonexist_date}, {nonexist_hour}, {speed_max}, {stuck_min}
CREATE OR REPLACE TABLE stg_traffic_reading AS
WITH src AS (
    SELECT link_id,
           id                         AS source_internal_id,
           CAST(data_as_of AS TIMESTAMP) AS reading_ts_local,   -- ns -> us (gia tri goc chi den giay)
           speed                      AS speed_mph,
           travel_time                AS travel_time_s,
           status                     AS source_status,
           parse_filename(filename)   AS source_file
    FROM read_parquet('{raw_glob}', filename = true)
),
flag AS (
    SELECT *,
           (CAST(reading_ts_local AS DATE) = DATE '{amb_date}'      AND hour(reading_ts_local) = {amb_hour})      AS is_dst_ambiguous,
           (CAST(reading_ts_local AS DATE) = DATE '{nonexist_date}' AND hour(reading_ts_local) = {nonexist_hour}) AS is_dst_nonexistent
    FROM src
),
conv AS (
    SELECT *,
           CASE WHEN is_dst_ambiguous OR is_dst_nonexistent THEN NULL
                ELSE timezone('America/New_York', reading_ts_local) END AS reading_ts_utc
    FROM flag
),
brk AS (    -- chuoi toc do dung yen (cung gia tri > 0 lien tiep)
    SELECT *,
           CASE WHEN speed_mph > 0
                 AND speed_mph = lag(speed_mph) OVER (PARTITION BY link_id ORDER BY reading_ts_local) THEN 0 ELSE 1 END AS is_break
    FROM conv
),
runs AS (
    SELECT *,
           sum(is_break) OVER (PARTITION BY link_id ORDER BY reading_ts_local ROWS UNBOUNDED PRECEDING) AS run_grp
    FROM brk
)
SELECT link_id, source_internal_id, reading_ts_local, reading_ts_utc,
       CAST(round((epoch(reading_ts_local) - epoch(reading_ts_utc)) / 60) AS SMALLINT) AS utc_offset_min,
       speed_mph, travel_time_s, source_status,
       CASE WHEN speed_mph IS NULL OR travel_time_s IS NULL      THEN 'null_value'
            WHEN speed_mph < 0                                   THEN 'speed_negative'
            WHEN speed_mph = 0 AND travel_time_s > 0             THEN 'speed_zero_tt_positive'
            WHEN speed_mph = 0                                   THEN 'speed_zero'
            WHEN speed_mph > {speed_max}                         THEN 'speed_gt_max'
            WHEN travel_time_s <= 0                              THEN 'travel_time_nonpositive'
            WHEN is_dst_nonexistent                              THEN 'nonexistent_local_time'
       END AS invalid_reason,
       is_dst_ambiguous, is_dst_nonexistent,
       (speed_mph > 0 AND count(*) OVER (PARTITION BY link_id, run_grp) >= {stuck_min}) AS is_stuck,
       source_file,
       substr(source_file, 1, 7) AS source_month,
       '{batch_id}' AS load_batch_id
FROM runs;
