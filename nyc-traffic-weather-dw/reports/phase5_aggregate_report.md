# Phase 5 – Aggregate fact & mart tích hợp

Batch `phase5-20261102T102937Z`. Chi tiết: `reports/phase5_aggregate.json`.

| Tiêu chí | Kết quả |
|---|---|
| grid_rows_equal_expected | ĐẠT |
| hourly_pk_unique | ĐẠT |
| sum_n_records_equal_atomic_assigned | ĐẠT |
| sum_n_valid_equal_atomic | ĐẠT |
| sum_speed_equal_atomic | ĐẠT |
| sum_travel_time_equal_atomic | ĐẠT |
| counts_partition_n_records | ĐẠT |
| dst_unassigned_equal_2468 | ĐẠT |
| dst_ambiguous_hours_flagged | ĐẠT |
| join_rows_equal_hourly | ĐẠT |
| join_pk_unique | ĐẠT |
| join_all_weather_matched | ĐẠT |
| fk_date_time_valid | ĐẠT |

- `fact_traffic_hourly`: 1 078 584 dòng (lưới kỳ vọng 1 078 584); Σ n_records = 12 712 376 = bản ghi atomic có giờ UTC (12 712 376); 2 468 bản ghi giờ DST kép không gán giờ.
- Giờ không có bản ghi: 12 278 · có bản ghi nhưng không có số đo hợp lệ: 205 028 · > 12 bản ghi (ghi bù): 206 · có ≥ 6 lần đo hợp lệ: 836 667 · có tốc độ đứng yên: 1 465.
- Giờ sự cố toàn mạng (≥ 90% link không có bản ghi, không tính 2 giờ DST kép): 34 giờ, theo ngày: [['2025-06-12', 1], ['2025-06-19', 9], ['2025-06-20', 10], ['2025-10-28', 2], ['2025-10-29', 8], ['2025-12-01', 4]].
- `fact_traffic_weather_hourly`: 1 078 584 dòng, phân vùng 13 tháng; 100% dòng có thời tiết.
- File DuckDB chỉ chứa view: `data/nyc_dw.duckdb`.

Ghi chú: thêm cột `is_dst_ambiguous_hour` vào hai fact theo giờ để 2 giờ UTC của giờ DST kép (05Z, 06Z ngày 02/11) không bị hiểu nhầm là sự cố.