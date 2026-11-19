-- H01_peak_hour_profile
-- HƯỚNG DẪN CHUNG CHO BỐN TRUY VẤN
-- Chỉ đọc Gold, phạm vi 01/01–30/09/2025 theo America/New_York, timestamp gốc giữ UTC.
-- Đơn vị quan sát cơ sở là một road version × một giờ UTC thực tế, không phải từng reading.
-- TTI của tuyến–giờ = sum_travel_time_sec / sum_free_flow_time_sec, chỉ nhận mẫu số dương.
-- TTI = 2 nghĩa là thời gian gấp 2 lần baseline free-flow của cùng tập reading.
-- Tốc độ mỗi tuyến–giờ = sum_distance_m / sum_travel_time_sec / 0.44704, đơn vị mph.
-- H01/H02 dùng trung bình TTI tuyến–giờ, không dùng tỷ số tổng toàn kỳ thay cho trung bình đó.
-- NULL thời tiết là thiếu dữ liệu, không biến thành dry hoặc 0 và không điền giờ thiếu bằng 0.
-- Weather tại mốc H có precipitation/snowfall của giờ trước [H−1h,H), không LAG thêm.
-- Baseline đã hiệu chuẩn trên cả chín tháng nên các kết quả mô tả lịch sử, không kiểm định dự báo.
--
-- H01 hỏi: giờ nào trong ngày có mức congestion trung bình cao nhất?
-- Một dòng = giờ New York 0–23, phù hợp biểu đồ cột hoặc đường theo giờ.
-- Mỗi tuyến–giờ quan sát có trọng số ngang nhau trong mean_road_hour_tti và mean_road_hour_speed_mph.
-- observed_roads/days/road_hours là số tuyến, ngày và tuyến–giờ hỗ trợ trung bình.
-- Ngưỡng TTI 1.5 chỉ phục vụ dashboard và sửa được trong params, không phải rule loại dữ liệu ETL.
-- congested_road_hour_pct là % tuyến–giờ có TTI đạt ngưỡng trong số tuyến–giờ quan sát.
WITH params AS (
    SELECT CAST(1.5 AS DOUBLE) AS jam_tti_threshold
), road_hours AS (
    SELECT
        c.road_link_key,
        r.link_id,
        c.date_key,
        h.hour_24,
        c.sum_travel_time_sec / c.sum_free_flow_time_sec AS road_hour_tti,
        c.sum_distance_m / c.sum_travel_time_sec / 0.44704 AS road_hour_speed_mph
    FROM lakehouse.gold.fact_congestion_hourly c
    JOIN lakehouse.gold.dim_road_link r ON r.road_link_key = c.road_link_key
    JOIN lakehouse.gold.dim_hour h ON h.hour_key = c.hour_key
    WHERE c.hour_start_utc >= TIMESTAMP '2025-01-01 05:00:00 UTC'
      AND c.hour_start_utc < TIMESTAMP '2025-10-01 04:00:00 UTC'
      AND c.sum_free_flow_time_sec > 0
      AND c.sum_travel_time_sec > 0
)
SELECT
    x.hour_24 AS hour_24_new_york,
    avg(x.road_hour_tti) AS mean_road_hour_tti,
    avg(x.road_hour_speed_mph) AS mean_road_hour_speed_mph,
    count(*) AS observed_road_hours,
    count(DISTINCT x.link_id) AS observed_roads,
    count(DISTINCT x.date_key) AS observed_days,
    max(p.jam_tti_threshold) AS jam_tti_threshold,
    count_if(x.road_hour_tti >= p.jam_tti_threshold) AS congested_road_hours,
    100.0 * count_if(x.road_hour_tti >= p.jam_tti_threshold) / nullif(count(*), 0)
        AS congested_road_hour_pct
FROM road_hours x
CROSS JOIN params p
GROUP BY x.hour_24
ORDER BY mean_road_hour_tti DESC, hour_24_new_york;

-- H02_matched_weather_by_hour
-- H02 hỏi: theo từng giờ, wet hoặc snow liên hệ với congestion khác dry như thế nào?
-- Một dòng = giờ New York × trạng thái wet/snow, phù hợp biểu đồ so sánh với dry.
-- Ghép so sánh trong cùng road version × giờ New York × thứ để hạn chế khác biệt tuyến/khung giờ.
-- Trong mỗi nhóm ghép, lấy trung bình TTI của các tuyến–giờ, rồi cho mỗi nhóm ghép trọng số ngang nhau.
-- mean_dry_tti/mean_weather_tti là trung bình các trung bình nhóm ghép, không gộp theo số reading.
-- tti_relative_change_pct so hai trung bình cuối, số dương nghĩa là TTI cao hơn khi wet/snow.
-- Params yêu cầu tối thiểu 10 mẫu dry và 3 mẫu wet hoặc snow cho từng nhóm ghép, có thể chỉnh.
-- Giờ/trạng thái không có dòng kết quả nghĩa là không đủ nhóm ghép đạt ngưỡng, không phải ảnh hưởng bằng 0.
-- Trong kỳ này mỗi UTC road-hour ứng với một ngày tại giờ/nhóm thứ đó, không có giờ mùa thu lặp.
-- Snow ưu tiên khi snowfall > 0, còn lại wet nếu precipitation > 0, dry nếu cả hai bằng 0.
-- NULL/giá trị âm của hai biến tích lũy là unknown, không được đưa vào hai phía so sánh.
-- unknown_weather_road_hours_excluded_at_hour đếm mẫu unknown toàn giờ, trước lọc nhóm đủ hỗ trợ.
-- dry/weather_road_hour_support chỉ đếm mẫu thuộc nhóm ghép được giữ, không phải toàn bộ dữ liệu giờ.
-- Dry có thể được dùng lại cho hai phép so wet và snow, không cộng support giữa hai dòng trạng thái.
-- Đây là liên hệ quan sát, chưa kiểm soát mùa, ngày lễ, sự cố hoặc thiên lệch do thiếu mẫu.
WITH params AS (
    SELECT 10 AS min_dry_observations, 3 AS min_weather_observations
), road_hours AS (
    SELECT
        c.road_link_key,
        h.hour_24,
        d.day_of_week,
        c.sum_travel_time_sec / c.sum_free_flow_time_sec AS road_hour_tti,
        CASE
            WHEN w.grid_key IS NULL OR w.precipitation_mm IS NULL OR w.snowfall_cm IS NULL
              OR w.precipitation_mm < 0 OR w.snowfall_cm < 0 THEN 'unknown'
            WHEN w.snowfall_cm > 0 THEN 'snow'
            WHEN w.precipitation_mm > 0 THEN 'wet'
            ELSE 'dry'
        END AS weather_state
    FROM lakehouse.gold.fact_congestion_hourly c
    JOIN lakehouse.gold.dim_hour h ON h.hour_key = c.hour_key
    JOIN lakehouse.gold.dim_date d ON d.date_key = c.date_key
    LEFT JOIN lakehouse.gold.fact_weather_hourly w
      ON w.grid_key = c.grid_key AND w.hour_start_utc = c.hour_start_utc
    WHERE c.hour_start_utc >= TIMESTAMP '2025-01-01 05:00:00 UTC'
      AND c.hour_start_utc < TIMESTAMP '2025-10-01 04:00:00 UTC'
      AND c.sum_free_flow_time_sec > 0
      AND c.sum_travel_time_sec > 0
), hour_support AS (
    SELECT
        hour_24,
        count(*) AS all_observed_road_hours_at_hour,
        count_if(weather_state = 'unknown') AS unknown_weather_road_hours_excluded_at_hour
    FROM road_hours
    GROUP BY hour_24
), strata AS (
    SELECT
        road_link_key,
        hour_24,
        day_of_week,
        count_if(weather_state = 'dry') AS dry_n,
        count_if(weather_state = 'wet') AS wet_n,
        count_if(weather_state = 'snow') AS snow_n,
        avg(road_hour_tti) FILTER (WHERE weather_state = 'dry') AS dry_mean_tti,
        avg(road_hour_tti) FILTER (WHERE weather_state = 'wet') AS wet_mean_tti,
        avg(road_hour_tti) FILTER (WHERE weather_state = 'snow') AS snow_mean_tti
    FROM road_hours
    GROUP BY road_link_key, hour_24, day_of_week
), paired AS (
    SELECT
        s.road_link_key,
        s.hour_24,
        s.day_of_week,
        e.weather_state,
        s.dry_n,
        CASE WHEN e.weather_state = 'wet' THEN s.wet_n ELSE s.snow_n END AS weather_n,
        s.dry_mean_tti,
        CASE WHEN e.weather_state = 'wet' THEN s.wet_mean_tti ELSE s.snow_mean_tti END
            AS weather_mean_tti
    FROM strata s
    CROSS JOIN params p
    CROSS JOIN UNNEST(ARRAY['wet', 'snow']) AS e(weather_state)
    WHERE s.dry_n >= p.min_dry_observations
      AND CASE WHEN e.weather_state = 'wet' THEN s.wet_n ELSE s.snow_n END
            >= p.min_weather_observations
)
SELECT
    m.hour_24 AS hour_24_new_york,
    m.weather_state,
    count(*) AS matched_strata,
    count(DISTINCT m.road_link_key) AS matched_road_versions,
    count(DISTINCT m.day_of_week) AS matched_days_of_week,
    sum(m.dry_n) AS dry_road_hour_support,
    sum(m.weather_n) AS weather_road_hour_support,
    avg(m.dry_mean_tti) AS mean_dry_tti,
    avg(m.weather_mean_tti) AS mean_weather_tti,
    100.0 * (avg(m.weather_mean_tti) / nullif(avg(m.dry_mean_tti), 0) - 1)
        AS tti_relative_change_pct,
    max(u.all_observed_road_hours_at_hour) AS all_observed_road_hours_at_hour,
    max(u.unknown_weather_road_hours_excluded_at_hour)
        AS unknown_weather_road_hours_excluded_at_hour
FROM paired m
JOIN hour_support u ON u.hour_24 = m.hour_24
GROUP BY m.hour_24, m.weather_state
ORDER BY hour_24_new_york, m.weather_state;

-- H03_weather_when_congested
-- H03 hỏi: vào mỗi giờ, weather_group nào thường xuất hiện cùng congestion?
-- Một dòng = giờ New York × nhóm WMO, phù hợp bảng hoặc cột chồng theo thời tiết khi tắc.
-- Ngưỡng TTI 1.5 sửa được trong params, không phải định nghĩa cố định của nguồn.
-- p_congested_given_weather_pct là P(tắc | nhóm thời tiết) = số giờ tắc / mọi giờ thuộc nhóm đó.
-- p_weather_given_congested_pct là P(nhóm thời tiết | tắc) = số giờ tắc của nhóm / mọi giờ tắc cùng giờ.
-- Hai xác suất có mẫu số khác nhau, không đọc tỷ trọng khi tắc thành nguy cơ tắc của thời tiết.
-- Mẫu số cửa sổ được tính từ tất cả nhóm trước khi chỉ hiển thị nhóm có ít nhất một giờ tắc.
-- Không áp ngưỡng support làm mất nhóm khỏi mẫu số, hãy đọc observed/congested_road_hours kèm %.
-- Weather_group là nhóm WMO tại mốc H, precipitation/snowfall là tích lũy giờ trước H.
-- WMO clear không đồng nghĩa dry vì trạng thái tức thời có thể khác lượng tích lũy giờ trước.
-- mean_*_when_congested chỉ lấy các tuyến–giờ đạt ngưỡng, không SUM precipitation qua nhiều tuyến.
-- Đơn vị tương ứng là mm, cm, °C, mph và %, số trung bình có trọng số tuyến–giờ quan sát.
-- Nhiều tuyến có thể cùng grid, đây không phải thời tiết trung bình theo diện tích thành phố.
-- Nhóm unknown giữ trong tổng nếu thiếu WMO/weather, NULL metric không được đổi thành 0.
-- Các tỷ lệ là mô tả quan sát, không chứng minh nhóm thời tiết gây tắc đường.
WITH params AS (
    SELECT CAST(1.5 AS DOUBLE) AS jam_tti_threshold
), road_hours AS (
    SELECT
        h.hour_24,
        c.sum_travel_time_sec / c.sum_free_flow_time_sec AS road_hour_tti,
        CASE WHEN w.grid_key IS NULL OR w.weather_code IS NULL THEN 'unknown'
             ELSE coalesce(wc.weather_group, 'unknown') END AS weather_group,
        w.precipitation_mm,
        w.snowfall_cm,
        w.temperature_c,
        w.wind_speed_mph,
        w.relative_humidity_pct
    FROM lakehouse.gold.fact_congestion_hourly c
    JOIN lakehouse.gold.dim_hour h ON h.hour_key = c.hour_key
    LEFT JOIN lakehouse.gold.fact_weather_hourly w
      ON w.grid_key = c.grid_key AND w.hour_start_utc = c.hour_start_utc
    LEFT JOIN lakehouse.gold.dim_weather_condition wc ON wc.condition_key = w.condition_key
    WHERE c.hour_start_utc >= TIMESTAMP '2025-01-01 05:00:00 UTC'
      AND c.hour_start_utc < TIMESTAMP '2025-10-01 04:00:00 UTC'
      AND c.sum_free_flow_time_sec > 0
      AND c.sum_travel_time_sec > 0
), grouped AS (
    SELECT
        x.hour_24,
        x.weather_group,
        max(p.jam_tti_threshold) AS jam_tti_threshold,
        count(*) AS observed_road_hours,
        count_if(x.road_hour_tti >= p.jam_tti_threshold) AS congested_road_hours,
        avg(x.road_hour_tti) AS mean_road_hour_tti,
        avg(x.road_hour_tti) FILTER (WHERE x.road_hour_tti >= p.jam_tti_threshold)
            AS mean_tti_when_congested,
        avg(x.precipitation_mm) FILTER (WHERE x.road_hour_tti >= p.jam_tti_threshold)
            AS mean_precip_prev_1h_mm_when_congested,
        avg(x.snowfall_cm) FILTER (WHERE x.road_hour_tti >= p.jam_tti_threshold)
            AS mean_snow_prev_1h_cm_when_congested,
        avg(x.temperature_c) FILTER (WHERE x.road_hour_tti >= p.jam_tti_threshold)
            AS mean_temperature_c_when_congested,
        avg(x.wind_speed_mph) FILTER (WHERE x.road_hour_tti >= p.jam_tti_threshold)
            AS mean_wind_speed_mph_when_congested,
        avg(x.relative_humidity_pct) FILTER (WHERE x.road_hour_tti >= p.jam_tti_threshold)
            AS mean_relative_humidity_pct_when_congested,
        count(x.precipitation_mm) FILTER (WHERE x.road_hour_tti >= p.jam_tti_threshold)
            AS congested_road_hours_with_precip_measure
    FROM road_hours x
    CROSS JOIN params p
    GROUP BY x.hour_24, x.weather_group
), denominators AS (
    SELECT *,
        sum(observed_road_hours) OVER (PARTITION BY hour_24) AS all_weather_road_hours_at_hour,
        sum(congested_road_hours) OVER (PARTITION BY hour_24) AS all_congested_road_hours_at_hour
    FROM grouped
)
SELECT
    hour_24 AS hour_24_new_york,
    weather_group,
    jam_tti_threshold,
    observed_road_hours,
    congested_road_hours,
    all_weather_road_hours_at_hour,
    all_congested_road_hours_at_hour,
    100.0 * congested_road_hours / nullif(observed_road_hours, 0) AS p_congested_given_weather_pct,
    100.0 * congested_road_hours / nullif(all_congested_road_hours_at_hour, 0)
        AS p_weather_given_congested_pct,
    mean_road_hour_tti,
    mean_tti_when_congested,
    mean_precip_prev_1h_mm_when_congested,
    mean_snow_prev_1h_cm_when_congested,
    mean_temperature_c_when_congested,
    mean_wind_speed_mph_when_congested,
    mean_relative_humidity_pct_when_congested,
    congested_road_hours_with_precip_measure
FROM denominators
WHERE congested_road_hours > 0
ORDER BY hour_24_new_york, p_weather_given_congested_pct DESC, weather_group;

-- H04_congested_road_hours
-- H04 hỏi: tại những tuyến–giờ tắc cụ thể, weather quan sát được như thế nào?
-- Một dòng = một road version × một timestamp UTC, giữ thêm timestamp theo New York để đọc.
-- Chỉ lấy TTI >= ngưỡng trong params, sắp TTI cao nhất và giới hạn 200 dòng để xem chi tiết.
-- Đây là mẫu cực trị có chủ đích, không dùng 200 dòng này để tính thống kê chung cho cả kỳ.
-- road_hour_speed_mph là tốc độ của từng tuyến–giờ, không phải trung bình tốc độ raw reading.
-- weather_observation_ts_utc phải cùng H với giờ traffic nếu có weather, không lấy weather giờ sau.
-- precip_prev_1h_mm/snow_prev_1h_cm lần lượt là mm/cm của giờ trước, các biến còn lại tại mốc H.
-- Weather NULL được giữ nguyên, weather_group thiếu ghi unknown, không tự suy dry.
-- weather_grid_latitude/longitude là tọa độ grid weather, không phải điểm đại diện của tuyến.
WITH params AS (
    SELECT CAST(1.5 AS DOUBLE) AS jam_tti_threshold
), road_hours AS (
    SELECT
        c.road_link_key,
        c.hour_start_utc,
        c.grid_key,
        c.sum_travel_time_sec / c.sum_free_flow_time_sec AS road_hour_tti,
        c.sum_distance_m / c.sum_travel_time_sec / 0.44704 AS road_hour_speed_mph
    FROM lakehouse.gold.fact_congestion_hourly c
    WHERE c.hour_start_utc >= TIMESTAMP '2025-01-01 05:00:00 UTC'
      AND c.hour_start_utc < TIMESTAMP '2025-10-01 04:00:00 UTC'
      AND c.sum_free_flow_time_sec > 0
      AND c.sum_travel_time_sec > 0
)
SELECT
    x.road_link_key,
    r.link_id,
    r.link_name,
    r.borough,
    x.hour_start_utc,
    at_timezone(x.hour_start_utc, 'America/New_York') AS hour_start_new_york,
    x.road_hour_tti,
    x.road_hour_speed_mph,
    p.jam_tti_threshold,
    x.grid_key,
    g.latitude AS weather_grid_latitude,
    g.longitude AS weather_grid_longitude,
    w.hour_start_utc AS weather_observation_ts_utc,
    CASE WHEN w.grid_key IS NULL OR w.weather_code IS NULL THEN 'unknown'
         ELSE coalesce(wc.weather_group, 'unknown') END AS weather_group,
    w.precipitation_mm AS precip_prev_1h_mm,
    w.snowfall_cm AS snow_prev_1h_cm,
    w.temperature_c,
    w.wind_speed_mph,
    w.relative_humidity_pct
FROM road_hours x
CROSS JOIN params p
JOIN lakehouse.gold.dim_road_link r ON r.road_link_key = x.road_link_key
LEFT JOIN lakehouse.gold.dim_weather_grid g ON g.grid_key = x.grid_key
LEFT JOIN lakehouse.gold.fact_weather_hourly w
  ON w.grid_key = x.grid_key AND w.hour_start_utc = x.hour_start_utc
LEFT JOIN lakehouse.gold.dim_weather_condition wc ON wc.condition_key = w.condition_key
WHERE x.road_hour_tti >= p.jam_tti_threshold
ORDER BY x.road_hour_tti DESC, x.hour_start_utc, x.road_link_key
LIMIT 200;
