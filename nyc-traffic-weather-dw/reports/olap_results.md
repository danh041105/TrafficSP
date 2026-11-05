# Phase 7 – Minh họa OLAP

Chạy trên `data/nyc_dw.duckdb` (view trên Parquet). SQL: `sql/olap/`. Kết quả đầy đủ: `reports/olap/*.csv`.
Mọi tốc độ trung bình đều tính `SUM(sum_speed) / SUM(n_valid)` (không dùng AVG của AVG). Ngưỡng trạng thái giao thông là quy tắc phân tích (D-18).

## Q1. Tốc độ trung bình theo borough và tháng (trung bình có trọng số theo số lần đo; bỏ link chất lượng thấp D-07)

```sql
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
```

60 dòng kết quả (hiển thị 60):

| borough | month | avg_speed_mph | n_valid_readings | n_links |
|---|---|---|---|---|
| Bronx | 1.0 | 42.59 | 183899.0 | 21 |
| Bronx | 2.0 | 41.67 | 168582.0 | 21 |
| Bronx | 3.0 | 40.86 | 181952.0 | 21 |
| Bronx | 4.0 | 39.62 | 179920.0 | 21 |
| Bronx | 5.0 | 38.11 | 185162.0 | 21 |
| Bronx | 6.0 | 38.39 | 171515.0 | 21 |
| Bronx | 7.0 | 39.22 | 182576.0 | 21 |
| Bronx | 8.0 | 38.47 | 182948.0 | 21 |
| Bronx | 9.0 | 36.62 | 167464.0 | 21 |
| Bronx | 10.0 | 35.61 | 175173.0 | 21 |
| Bronx | 11.0 | 40.52 | 156338.0 | 21 |
| Bronx | 12.0 | 38.77 | 139864.0 | 21 |
| Brooklyn | 1.0 | 32.06 | 52533.0 | 6 |
| Brooklyn | 2.0 | 23.32 | 47243.0 | 6 |
| Brooklyn | 3.0 | 26.53 | 51330.0 | 6 |
| Brooklyn | 4.0 | 30.61 | 35970.0 | 6 |
| Brooklyn | 5.0 | 29.7 | 35355.0 | 6 |
| Brooklyn | 6.0 | 32.22 | 44877.0 | 8 |
| Brooklyn | 7.0 | 33.87 | 65538.0 | 8 |
| Brooklyn | 8.0 | 34.23 | 70730.0 | 8 |
| Brooklyn | 9.0 | 33.83 | 68252.0 | 8 |
| Brooklyn | 10.0 | 34.45 | 69513.0 | 8 |
| Brooklyn | 11.0 | 34.62 | 68192.0 | 8 |
| Brooklyn | 12.0 | 32.79 | 70662.0 | 8 |
| Manhattan | 1.0 | 28.79 | 131949.0 | 17 |
| Manhattan | 2.0 | 27.42 | 110884.0 | 17 |
| Manhattan | 3.0 | 27.53 | 123240.0 | 17 |
| Manhattan | 4.0 | 28.56 | 108486.0 | 17 |
| Manhattan | 5.0 | 27.22 | 106654.0 | 17 |
| Manhattan | 6.0 | 28.03 | 104559.0 | 17 |
| Manhattan | 7.0 | 27.28 | 136770.0 | 17 |
| Manhattan | 8.0 | 26.85 | 138779.0 | 17 |
| Manhattan | 9.0 | 25.14 | 136650.0 | 17 |
| Manhattan | 10.0 | 25.22 | 133936.0 | 17 |
| Manhattan | 11.0 | 25.96 | 120043.0 | 17 |
| Manhattan | 12.0 | 26.3 | 126903.0 | 17 |
| Queens | 1.0 | 40.79 | 324889.0 | 37 |
| Queens | 2.0 | 40.41 | 288554.0 | 37 |
| Queens | 3.0 | 39.83 | 319341.0 | 37 |
| Queens | 4.0 | 38.15 | 312574.0 | 37 |
| Queens | 5.0 | 36.4 | 311460.0 | 37 |
| Queens | 6.0 | 36.32 | 297806.0 | 37 |
| Queens | 7.0 | 37.12 | 319106.0 | 37 |
| Queens | 8.0 | 37.11 | 319990.0 | 37 |
| Queens | 9.0 | 36.66 | 308494.0 | 37 |
| Queens | 10.0 | 35.7 | 312562.0 | 37 |
| Queens | 11.0 | 36.88 | 285019.0 | 37 |
| Queens | 12.0 | 36.17 | 303585.0 | 37 |
| Staten Island | 1.0 | 52.96 | 153875.0 | 18 |
| Staten Island | 2.0 | 48.31 | 94613.0 | 18 |
| Staten Island | 3.0 | 48.09 | 141284.0 | 18 |
| Staten Island | 4.0 | 47.58 | 148156.0 | 18 |
| Staten Island | 5.0 | 44.24 | 140791.0 | 18 |
| Staten Island | 6.0 | 46.05 | 134593.0 | 18 |
| Staten Island | 7.0 | 47.73 | 117624.0 | 18 |
| Staten Island | 8.0 | 49.92 | 139196.0 | 18 |
| Staten Island | 9.0 | 51.43 | 141289.0 | 18 |
| Staten Island | 10.0 | 51.08 | 146014.0 | 18 |
| Staten Island | 11.0 | 51.66 | 138842.0 | 18 |
| Staten Island | 12.0 | 50.62 | 145124.0 | 18 |

## Q2. Tốc độ khi có mưa / tuyết so với lúc khô, theo khung giờ (dice: điều kiện × khung giờ). Ngày thường, link đạt chất lượng, giờ có ≥ 6 lần đo hợp lệ.

```sql
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
```

20 dòng kết quả (hiển thị 20):

| day_part | weather_class | avg_speed_mph | link_hours | pct_vs_dry |
|---|---|---|---|---|
| Ban ngày | 1. Khô | 33.84 | 98396 | 0.0 |
| Ban ngày | 2. Mưa nhỏ 0,1–1 mm/h | 32.53 | 14419 | -3.9 |
| Ban ngày | 3. Mưa ≥ 1 mm/h | 31.76 | 3573 | -6.1 |
| Ban ngày | 4. Tuyết | 37.01 | 600 | 9.4 |
| Cao điểm chiều | 1. Khô | 28.72 | 80164 | 0.0 |
| Cao điểm chiều | 2. Mưa nhỏ 0,1–1 mm/h | 27.21 | 8798 | -5.2 |
| Cao điểm chiều | 3. Mưa ≥ 1 mm/h | 26.21 | 3300 | -8.7 |
| Cao điểm chiều | 4. Tuyết | 32.06 | 1258 | 11.6 |
| Cao điểm sáng | 1. Khô | 35.13 | 81306 | 0.0 |
| Cao điểm sáng | 2. Mưa nhỏ 0,1–1 mm/h | 32.76 | 7572 | -6.7 |
| Cao điểm sáng | 3. Mưa ≥ 1 mm/h | 31.27 | 2866 | -11.0 |
| Cao điểm sáng | 4. Tuyết | 33.87 | 993 | -3.6 |
| Tối | 1. Khô | 39.34 | 100399 | 0.0 |
| Tối | 2. Mưa nhỏ 0,1–1 mm/h | 38.13 | 9266 | -3.1 |
| Tối | 3. Mưa ≥ 1 mm/h | 36.35 | 4257 | -7.6 |
| Tối | 4. Tuyết | 37.86 | 2150 | -3.7 |
| Đêm | 1. Khô | 46.28 | 119711 | 0.0 |
| Đêm | 2. Mưa nhỏ 0,1–1 mm/h | 44.38 | 9117 | -4.1 |
| Đêm | 3. Mưa ≥ 1 mm/h | 43.12 | 3911 | -6.8 |
| Đêm | 4. Tuyết | 45.3 | 1121 | -2.1 |

## Q3. Top 10 đoạn đường có tốc độ trung bình thấp nhất năm 2025 (link đạt chất lượng, ≥ 10 000 lần đo hợp lệ)

```sql
-- Q3. Top 10 đoạn đường có tốc độ trung bình thấp nhất năm 2025 (link đạt chất lượng, ≥ 10 000 lần đo hợp lệ)
SELECT r.link_id, r.link_name, r.borough,
       round(sum(f.sum_speed) / sum(f.n_valid), 2) AS avg_speed_mph,
       sum(f.n_valid)                              AS n_valid_readings,
       round(r.free_flow_speed_mph, 1)             AS free_flow_mph
FROM fact_traffic_hourly f JOIN dim_road_link r ON r.road_key = f.road_key
WHERE NOT r.is_low_quality
GROUP BY ALL
HAVING sum(f.n_valid) >= 10000
ORDER BY avg_speed_mph
LIMIT 10;
```

10 dòng kết quả (hiển thị 10):

| link_id | link_name | borough | avg_speed_mph | n_valid_readings | free_flow_mph |
|---|---|---|---|---|---|
| 4616340 | BQE N Atlantic Ave - MAN Bridge Manhattan Side | Manhattan | 5.39 | 72799.0 | 9.9 |
| 4616357 | Belt Pkwy W JFK Expressway - VWE N Jamaica Ave | Queens | 9.89 | 101629.0 | 22.4 |
| 4616276 | VWE S MP2.66 (Exit 2 Rockaway Blvd) - Belt Pkwy E 182nd St | Queens | 10.9 | 96909.0 | 19.9 |
| 4456494 | West St S Spring St - BBT Manhattan Portal outbound | Manhattan | 14.46 | 34339.0 | 18.6 |
| 4616272 | VWE S MP4.63 (Exit 6 Jamaica Ave) - MP2.66 (Exit 2 Roackaway Blvd) | Queens | 16.89 | 99937.0 | 41.0 |
| 4616342 | BKN Bridge Manhattan Side - FDR N Catherine Slip | Manhattan | 17.72 | 94952.0 | 34.2 |
| 4616259 | VWE S MP6.39 (Exit 11 Jewel Ave) - MP4.63 (Exit 6 Jamaica Ave) | Queens | 18.12 | 97385.0 | 43.5 |
| 4329472 | LINCOLN TUNNEL E SOUTH TUBE - NJ - NY | Manhattan | 18.49 | 69778.0 | 33.6 |
| 4616223 | GOW N 9TH STREET - ATLANTIC AVENUE | Brooklyn | 19.36 | 73280.0 | 46.6 |
| 4620314 | MDE S HARLEM RIVER PARK - GWB W AMSTERDAM AVENUE UPPER LEVEL | Bronx | 20.09 | 97137.0 | 38.5 |

## Q4. Ngày thường / cuối tuần / ngày lễ theo khung giờ (slice theo day_type)

```sql
-- Q4. Ngày thường / cuối tuần / ngày lễ theo khung giờ (slice theo day_type)
SELECT d.day_type, t.day_part,
       round(sum(f.sum_speed) / sum(f.n_valid), 2) AS avg_speed_mph,
       count(DISTINCT d.full_date)                 AS n_days
FROM fact_traffic_hourly f
JOIN dim_road_link r ON r.road_key = f.road_key
JOIN dim_date d      ON d.date_key = f.date_key
JOIN dim_time t      ON t.time_key = f.time_key
WHERE NOT r.is_low_quality
GROUP BY d.day_type, t.day_part
ORDER BY t.day_part, d.day_type;
```

15 dòng kết quả (hiển thị 15):

| day_type | day_part | avg_speed_mph | n_days |
|---|---|---|---|
| Holiday | Ban ngày | 38.12 | 11 |
| Weekday | Ban ngày | 33.58 | 250 |
| Weekend | Ban ngày | 36.56 | 104 |
| Holiday | Cao điểm chiều | 36.75 | 11 |
| Weekday | Cao điểm chiều | 28.49 | 250 |
| Weekend | Cao điểm chiều | 33.13 | 104 |
| Holiday | Cao điểm sáng | 44.86 | 11 |
| Weekday | Cao điểm sáng | 34.73 | 250 |
| Weekend | Cao điểm sáng | 45.58 | 104 |
| Holiday | Tối | 40.77 | 11 |
| Weekday | Tối | 39.03 | 250 |
| Weekend | Tối | 38.11 | 104 |
| Holiday | Đêm | 44.7 | 11 |
| Weekday | Đêm | 45.88 | 250 |
| Weekend | Đêm | 44.37 | 104 |

## Q5. Đoạn đường Manhattan lúc 17:00 (giờ New York) ngày lễ 04/07/2025: tốc độ, trạng thái, thời tiết.

```sql
-- Q5. Đoạn đường Manhattan lúc 17:00 (giờ New York) ngày lễ 04/07/2025: tốc độ, trạng thái, thời tiết.
-- Trạng thái = tốc độ / free_flow_speed_mph (p85 lúc khô, 22–05h): Ùn tắc < 0,5; Chậm < 0,8; Thông thoáng ≥ 0,8.
-- ĐÂY LÀ QUY TẮC PHÂN TÍCH (D-18), không phải tiêu chuẩn chính thức.
SELECT r.link_name, d.holiday_name, t.hour_label,
       round(f.sum_speed / NULLIF(f.n_valid, 0), 1)  AS avg_speed_mph,
       f.n_valid,
       round(r.free_flow_speed_mph, 1)               AS free_flow_mph,
       CASE WHEN f.n_valid < 6 OR r.free_flow_speed_mph IS NULL THEN 'Không xác định'
            WHEN f.sum_speed / f.n_valid < 0.5 * r.free_flow_speed_mph THEN 'Ùn tắc'
            WHEN f.sum_speed / f.n_valid < 0.8 * r.free_flow_speed_mph THEN 'Chậm'
            ELSE 'Thông thoáng' END                  AS traffic_status,
       c.description_vi                              AS weather,
       f.temperature_c, f.precipitation_mm, f.wind_gust_kmh
FROM fact_traffic_weather_hourly f
JOIN dim_road_link r          ON r.road_key = f.road_key
JOIN dim_date d               ON d.date_key = f.date_key
JOIN dim_time t               ON t.time_key = f.time_key
JOIN dim_weather_condition c  ON c.condition_key = f.condition_key
WHERE r.borough = 'Manhattan' AND t.time_key = 17 AND d.full_date = DATE '2025-07-04'
ORDER BY avg_speed_mph NULLS LAST;
```

26 dòng kết quả (hiển thị 26):

| link_name | holiday_name | hour_label | avg_speed_mph | n_valid | free_flow_mph | traffic_status | weather | temperature_c | precipitation_mm | wind_gust_kmh |
|---|---|---|---|---|---|---|---|---|---|---|
| BQE N Atlantic Ave - MAN Bridge Manhattan Side | Independence Day | 17:00 | 2.5 | 12 | 9.9 | Ùn tắc | Trời quang | 28.0 | 0.0 | 30.6 |
| FDR N 25th - 63rd St | Independence Day | 17:00 | 23.8 | 12 | 47.8 | Ùn tắc | Trời quang | 28.7 | 0.0 | 31.0 |
| LINCOLN TUNNEL E SOUTH TUBE - NJ - NY | Independence Day | 17:00 | 25.1 | 12 | 33.6 | Chậm | Trời quang | 28.2 | 0.0 | 32.0 |
| BBT E Manhattan Portal - Toll Plaza | Independence Day | 17:00 | 28.0 | 12 | 45.4 | Chậm | Trời quang | 28.2 | 0.0 | 32.0 |
| LINCOLN TUNNEL E CENTER TUBE NJ - NY | Independence Day | 17:00 | 33.1 | 7 | 34.8 | Thông thoáng | Trời quang | 28.2 | 0.0 | 32.0 |
| TBB W - FDR S MANHATTAN TRUSS - E116TH STREET | Independence Day | 17:00 | 34.8 | 12 | 38.5 | Thông thoáng | Trời quang | 28.3 | 0.0 | 28.4 |
| FDR N - TBB E 116TH STREET - MANHATTAN TRUSS | Independence Day | 17:00 | 34.9 | 12 | 37.9 | Thông thoáng | Trời quang | 28.3 | 0.0 | 28.4 |
| QMT E Manhattan Side - Toll Plaza | Independence Day | 17:00 | 35.3 | 12 | 37.3 | Thông thoáng | Trời quang | 28.7 | 0.0 | 31.0 |
| LINCOLN TUNNEL W NORTH TUBE NY - NJ | Independence Day | 17:00 | 36.1 | 12 | 42.3 | Thông thoáng | Trời quang | 28.2 | 0.0 | 32.0 |
| BBT W Toll Plaza - Manhattan Portal | Independence Day | 17:00 | 36.8 | 12 | 48.5 | Chậm | Trời quang | 28.2 | 0.0 | 32.0 |
| GWB E LOWER LEVEL PLAZA - CBE E LOWER LEVEL AMSTERDAM AVE | Independence Day | 17:00 | 41.6 | 12 | 51.0 | Thông thoáng | Trời quang | 28.2 | 0.0 | 29.5 |
| FDR S 63rd - 25th St | Independence Day | 17:00 | 44.4 | 12 | 49.7 | Thông thoáng | Trời quang | 28.7 | 0.0 | 31.0 |
| 12th Ave S 57th St - 45th St | Independence Day | 17:00 |  | 0 |  | Không xác định | Trời quang | 27.8 | 0.0 | 28.8 |
| LINCOLN TUNNEL W CENTER TUBE NY - NJ | Independence Day | 17:00 |  | 0 | 35.4 | Không xác định | Trời quang | 28.2 | 0.0 | 32.0 |
| FDR S Catherine Slip - BKN Bridge Manhattan Side | Independence Day | 17:00 |  | 0 | 34.8 | Không xác định | Trời quang | 28.2 | 0.0 | 32.0 |
| Westside Hwy S GWB - 57th St | Independence Day | 17:00 |  | 0 |  | Không xác định | Trời quang | 28.3 | 0.0 | 28.4 |
| BQE N Atlantic Ave - BKN Bridge Manhattan Side | Independence Day | 17:00 |  | 0 | 41.6 | Không xác định | Trời quang | 28.0 | 0.0 | 30.6 |
| 12th Ave N 40th - 57th St | Independence Day | 17:00 |  | 0 |  | Không xác định | Trời quang | 27.8 | 0.0 | 28.8 |
| 11th ave n ganservoort - 12th ave @ 40th st | Independence Day | 17:00 |  | 0 |  | Không xác định | Trời quang | 28.2 | 0.0 | 32.0 |
| FDR S 25th St - Catherine Slip | Independence Day | 17:00 |  | 0 | 51.0 | Không xác định | Trời quang | 28.7 | 0.0 | 31.0 |
| 12th ave @ 45th - 11 ave ganservoort st | Independence Day | 17:00 |  | 0 |  | Không xác định | Trời quang | 28.2 | 0.0 | 32.0 |
| West St S Spring St - BBT Manhattan Portal outbound | Independence Day | 17:00 |  | 0 | 18.6 | Không xác định | Trời quang | 28.2 | 0.0 | 32.0 |
| 11th ave s ganservoort - west st @ spring st | Independence Day | 17:00 |  | 0 |  | Không xác định | Trời quang | 28.2 | 0.0 | 32.0 |
| FDR N Catherine Slip - 25th St | Independence Day | 17:00 |  | 0 | 49.1 | Không xác định | Trời quang | 28.7 | 0.0 | 31.0 |
| BKN Bridge Manhattan Side - FDR N Catherine Slip | Independence Day | 17:00 |  | 0 | 34.2 | Không xác định | Trời quang | 28.2 | 0.0 | 32.0 |
| Westside Hwy N 57th St - GWB | Independence Day | 17:00 |  | 0 |  | Không xác định | Trời quang | 27.8 | 0.0 | 28.8 |

## Q6. Drill-down borough → road link (ROLLUP). Dòng link_name = NULL là tổng của borough; dòng borough = NULL là toàn mạng.

```sql
-- Q6. Drill-down borough → road link (ROLLUP). Dòng link_name = NULL là tổng của borough; dòng borough = NULL là toàn mạng.
SELECT r.borough, r.link_name,
       round(sum(f.sum_speed) / sum(f.n_valid), 2) AS avg_speed_mph,
       sum(f.n_valid)                              AS n_valid_readings,
       grouping(r.borough, r.link_name)            AS level   -- 3 = NYC, 1 = borough, 0 = link
FROM fact_traffic_hourly f JOIN dim_road_link r ON r.road_key = f.road_key
WHERE NOT r.is_low_quality
GROUP BY ROLLUP (r.borough, r.link_name)
ORDER BY r.borough NULLS FIRST, level DESC, avg_speed_mph;
```

107 dòng kết quả (hiển thị 12):

| borough | link_name | avg_speed_mph | n_valid_readings | level |
|---|---|---|---|---|
|  |  | 37.92 | 9579222.0 | 3 |
| Bronx |  | 39.21 | 2075393.0 | 1 |
| Bronx | MDE S HARLEM RIVER PARK - GWB W AMSTERDAM AVENUE UPPER LEVEL | 20.09 | 97137.0 | 0 |
| Bronx | MDE S HARLEM RIVER PARK - GWB W AMSTERDAM AVENUE LOWER LEVEL | 22.28 | 97138.0 | 0 |
| Bronx | CBE W MORRIS AVE - GWB W AMSTERDAM AVE (L/LVL) | 24.69 | 96105.0 | 0 |
| Bronx | CBE W MORRIS AVE - GWB W AMSTERDAM AVE (U/LVL) | 25.6 | 95628.0 | 0 |
| Bronx | CBE AMSTERDAM AVE (L/LVL) - MORRIS AVE | 28.08 | 102066.0 | 0 |
| Bronx | CBE E AMSTERDAM AVE(U/LVL) - MORRIS AVE | 28.91 | 101507.0 | 0 |
| Bronx | CBE W U/LEV AMSTERDAM AVE - I95 S EXP LNS | 30.65 | 89260.0 | 0 |
| Bronx | BRP N WATSON AVENUE - FORDHAM ROAD | 33.08 | 103606.0 | 0 |
| Bronx | CBE W CASTLE HILL AVENUE - TAYLOR AVENUE | 37.93 | 103707.0 | 0 |
| Bronx | BWB S Toll Plaza - Queens Anchorage | 40.24 | 97939.0 | 0 |

## Q7. Roll-up giờ → ngày → tháng (toàn mạng, tháng 12/2025). Các mức đều tính SUM(sum_speed)/SUM(n_valid).

```sql
-- Q7. Roll-up giờ → ngày → tháng (toàn mạng, tháng 12/2025). Các mức đều tính SUM(sum_speed)/SUM(n_valid).
SELECT d.month, d.full_date, t.time_key,
       round(sum(f.sum_speed) / NULLIF(sum(f.n_valid), 0), 2) AS avg_speed_mph,
       sum(f.n_valid)                                          AS n_valid_readings,
       grouping(d.month, d.full_date, t.time_key)              AS level   -- 0 = giờ, 1 = ngày, 3 = tháng
FROM fact_traffic_hourly f
JOIN dim_road_link r ON r.road_key = f.road_key
JOIN dim_date d      ON d.date_key = f.date_key
JOIN dim_time t      ON t.time_key = f.time_key
WHERE NOT r.is_low_quality AND d.month = 12
GROUP BY ROLLUP (d.month, d.full_date, t.time_key)
ORDER BY d.full_date NULLS FIRST, t.time_key NULLS FIRST;
```

777 dòng kết quả (hiển thị 20):

| month | full_date | time_key | avg_speed_mph | n_valid_readings | level |
|---|---|---|---|---|---|
|  | NaT | <NA> | 37.4 | 786138.0 | 7 |
| 12.0 | NaT | <NA> | 37.4 | 786138.0 | 3 |
| 12.0 | 2025-12-01 00:00:00 | <NA> | 39.66 | 18278.0 | 1 |
| 12.0 | 2025-12-01 00:00:00 | 0 | 46.13 | 940.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 1 | 46.46 | 927.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 2 | 47.9 | 906.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 3 | 49.66 | 915.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 4 | 50.18 | 928.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 5 | 46.18 | 930.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 6 | 34.69 | 925.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 7 | 29.44 | 925.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 8 | 26.64 | 918.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 9 | 31.84 | 929.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 10 | 34.92 | 927.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 11 | 36.68 | 919.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 12 | 36.47 | 997.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 13 | 36.11 | 1020.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 14 | 33.28 | 1027.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 15 | 31.36 | 257.0 | 0 |
| 12.0 | 2025-12-01 00:00:00 | 16 |  | 0.0 | 0 |

## Q8. Drill-across: ghép hai fact khác quy trình qua dimension dùng chung (dim_date) mà không dùng mart.

```sql
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
```

10 dòng kết quả (hiển thị 10):

| full_date | day_type | avg_speed_mph | precip_mm | snow_cm | tmin_c |
|---|---|---|---|---|---|
| 2025-12-14 00:00:00 | Weekend | 37.91 | 10.5 | 7.3 | -7.8 |
| 2025-02-08 00:00:00 | Weekend | 39.64 | 11.3 | 6.1 | -4.2 |
| 2025-12-26 00:00:00 | Weekday | 37.64 | 8.6 | 5.8 | -6.0 |
| 2025-01-19 00:00:00 | Weekend | 43.47 | 7.6 | 5.2 | -3.1 |
| 2025-02-06 00:00:00 | Weekday | 39.53 | 9.5 | 3.9 | -1.0 |
| 2025-12-27 00:00:00 | Weekend | 38.12 | 4.4 | 2.6 | -7.2 |
| 2025-02-12 00:00:00 | Weekday | 38.41 | 4.1 | 2.3 | -1.7 |
| 2025-02-15 00:00:00 | Weekend | 39.33 | 9.9 | 2.2 | -3.6 |
| 2025-02-11 00:00:00 | Weekday | 37.74 | 2.9 | 2.0 | -8.5 |
| 2025-02-02 00:00:00 | Weekend | 43.21 | 3.1 | 2.0 | -9.1 |

## Kiểm tra nhất quán roll-up / drill-down

| Kiểm tra | Kết quả |
|---|---|
| q6_borough_sum_equals_nyc | ĐẠT |
| q6_links_sum_equals_borough | ĐẠT |
| q7_days_sum_equals_month | ĐẠT |
| q7_hours_sum_equals_days | ĐẠT |
| q5_has_rows | ĐẠT |