# Phase 4 – Dimensions & atomic facts

Batch `phase4-20261102T103553Z`. Chi tiết: `reports/phase4_dw_atomic.json`.

| Tiêu chí | Kết quả |
|---|---|
| dim_date_365_plus_unknown | ĐẠT |
| dim_date_11_holidays | ĐẠT |
| dim_time_24_plus_unknown | ĐẠT |
| dim_road_link_124_plus_unknown_bk_unique | ĐẠT |
| dim_road_link_pk_unique | ĐẠT |
| dim_road_link_all_mapped | ĐẠT |
| dim_weather_location_16_plus_unknown | ĐẠT |
| dim_weather_condition_28_plus_unknown | ĐẠT |
| fact_traffic_reading_rows_equal_source | ĐẠT |
| fact_traffic_reading_pk_unique | ĐẠT |
| fact_traffic_reading_no_unknown_road | ĐẠT |
| fact_traffic_reading_fk_date_time | ĐẠT |
| fact_weather_hourly_rows_16x8760 | ĐẠT |
| fact_weather_hourly_pk_unique | ĐẠT |
| fact_weather_no_unknown_fk | ĐẠT |
| fact_weather_fk_date_time | ĐẠT |

- `dim_road_link`: {'links': '124.0', 'low_quality': '23.0', 'no_free_flow': '12.0', 'ff_min': '9.9', 'ff_med': '53.4', 'ff_max': '64.6'}
- `fact_traffic_reading`: {'n': 12714844, 'valid': 9911819, 'amb': 2468, 'stuck': 10753}
- `fact_weather_hourly` theo nhóm điều kiện (số ô-giờ): {'Cloudy': 60852, 'Clear': 59752, 'Drizzle': 13756, 'Rain': 3013, 'Snow': 2194, 'Showers': 495, 'Thunderstorm': 56, 'Fog': 42}
- Kiểm tra date_key/time_key theo giờ New York (ô key 1) – chú ý 02/11 có 2 giờ UTC cùng time_key = 1:

| interval_start_utc | date_key | time_key |
|---|---|---|
| 2025-01-01 05:00:00+00:00 | 20250101 | 0 |
| 2025-03-09 06:00:00+00:00 | 20250309 | 1 |
| 2025-03-09 07:00:00+00:00 | 20250309 | 3 |
| 2025-11-02 05:00:00+00:00 | 20251102 | 1 |
| 2025-11-02 06:00:00+00:00 | 20251102 | 1 |
| 2026-01-01 04:00:00+00:00 | 20251231 | 23 |

Ghi chú triển khai: `fact_traffic_reading` phân vùng theo tháng của `reading_ts_local` (tháng file nguồn), vì `reading_ts_utc` để trống ở giờ DST kép (làm rõ D-16).