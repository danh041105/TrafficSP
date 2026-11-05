# Data quality report

Bộ kiểm thử: 22 đạt, 0 lỗi (`tests/`, junit: `data/quality/pytest_junit.xml`). Chỉ số: `data/quality/dq_metrics.json`.

## 11 tiêu chí

| Tiêu chí | Kết quả | Kiểm thử |
|---|---|---|
| 1. Số bản ghi atomic khớp nguồn | ĐẠT | test_raw_rows_equal_expected, test_traffic_reading_values_identical_to_raw, test_fact_row_count_per_source_file |
| 2. Không trùng grain | ĐẠT | test_traffic_reading_grain_unique, test_weather_grain_unique, test_join_preserves_rows_and_grain |
| 3. Không mất dữ liệu không giải thích được | ĐẠT | test_unassigned_records_are_only_dst_ambiguous, etl_reconciliation.md |
| 4. PK/FK hợp lệ | ĐẠT | test_dimension_keys_unique_and_unknown_present, test_foreign_keys_resolve |
| 5. DST xử lý có căn cứ | ĐẠT | test_dst_handling_matches_tzdata |
| 6. Căn đúng khoảng giờ thời tiết | ĐẠT | test_instant_variables_taken_at_interval_start, test_accumulated_variables_taken_at_interval_end, test_weather_covers_local_year_exactly |
| 7. Mapping có khoảng cách và phiên bản | ĐẠT | test_every_link_mapped_with_distance_and_version |
| 8. Aggregate khớp atomic | ĐẠT | test_hourly_matches_atomic_per_link_and_month, test_no_record_vs_zero_speed_distinguished |
| 9. Số dòng trước/sau JOIN | ĐẠT | test_join_preserves_rows_and_grain, test_mart_weather_equals_weather_fact |
| 10. Measure tổng hợp đúng | ĐẠT | test_rollup_from_hourly_equals_atomic, test_std_from_components_matches_atomic |
| 11. Truy xuất nguồn | ĐẠT | test_raw_checksums_match_manifest, test_every_fact_row_traceable_to_source_file |

## Giao thông

**Phân loại bản ghi** (không xóa dòng nào):

| Loại | Số bản ghi | % |
|---|---|---|
| hợp lệ | 9 911 819 | 77.95 |
| speed_zero | 2 802 797 | 22.04 |
| speed_zero_tt_positive | 120 | 0.0 |
| speed_gt_max | 108 | 0.0 |

**Phân phối tốc độ trước và sau khi lọc** (mph):

| Nhóm | p5 | p25 | p50 | p75 | p95 | TB | max |
|---|---|---|---|---|---|---|---|
| tất cả bản ghi | 0.0 | 5.6 | 33.6 | 49.1 | 59.0 | 29.47 | 119.3 |
| hợp lệ | 6.2 | 24.2 | 42.3 | 51.6 | 60.3 | 37.8 | 99.4 |

**Link-giờ:** 1 078 584 dòng · không có bản ghi 12 278 · có bản ghi nhưng không có số đo hợp lệ 205 028 · ≥ 6 lần đo hợp lệ 836 667 (77.6%) · > 12 bản ghi (ghi bù) 206 · có tốc độ đứng yên 1 465 · giờ DST kép 248.

**Theo tháng (UTC):**

| Tháng | Link-giờ không bản ghi | % link-giờ có ≥ 6 lần đo hợp lệ |
|---|---|---|
| 2025-01 | 712 | 80.3 |
| 2025-02 | 672 | 75.1 |
| 2025-03 | 750 | 77.6 |
| 2025-04 | 1392 | 77.0 |
| 2025-05 | 1620 | 74.7 |
| 2025-06 | 3993 | 74.2 |
| 2025-07 | 808 | 78.5 |
| 2025-08 | 61 | 81.4 |
| 2025-09 | 0 | 82.0 |
| 2025-10 | 1240 | 80.1 |
| 2025-11 | 502 | 75.4 |
| 2025-12 | 528 | 74.1 |
| 2026-01 | 0 | 81.8 |

**Sự cố toàn mạng** (34 giờ): 2025-06-12 (1 giờ), 2025-06-19 (9 giờ), 2025-06-20 (10 giờ), 2025-10-28 (2 giờ), 2025-10-29 (8 giờ), 2025-12-01 (4 giờ)

**Link chất lượng thấp** (< 50% hợp lệ cả năm, 23 link; giữ trong DW, loại khi phân tích mặc định):

| link_id | Tên | Borough | % hợp lệ |
|---|---|---|---|
| 4616255 | MDE N RFK Bridge - 142nd St | Bronx | 0.0 |
| 4616323 | 12th Ave S 57th St - 45th St | Manhattan | 0.0 |
| 4616324 | 12th ave @ 45th - 11 ave ganservoort st | Manhattan | 0.0 |
| 4616325 | 11th ave s ganservoort - west st @ spring st | Manhattan | 0.0 |
| 4616337 | 11th ave n ganservoort - 12th ave @ 40th st | Manhattan | 0.0 |
| 4616338 | 12th Ave N 40th - 57th St | Manhattan | 0.0 |
| 4616346 | Westside Hwy S GWB - 57th St | Manhattan | 0.0 |
| 4620343 | Westside Hwy N 57th St - GWB | Manhattan | 0.0 |
| 4763649 | VNB W BROOKLYN GANTRY UPPER LEVEL - SI GANTRY UPPER LEVEL | Brooklyn | 0.0 |
| 4763652 | VNB E SI GANTRY UPPER LEVEL - BROOKLYN GANTRY UPPER LEVEL | Brooklyn | 0.0 |
| 4763655 | GOW S VNB W 92ND STREET - BKLYN GANTRY UPPER LEVEL | Brooklyn | 0.0 |
| 4763657 | GOW S VNB W 92ND STREET - BKLYN GANTRY LOWER LEVEL | Brooklyn | 0.0 |
| 4616203 | WSE S BLLOMINGDALE ROAD - TYRELLAN AVENUE | Staten Island | 1.1 |
| 4616215 | WSE N TYRELLAN AVENUE - BLOOMINGDALE ROAD | Staten Island | 1.1 |
| 4616216 | SIE E-MLK N RICHMOND AVENUE - WALKER STREET | Staten Island | 16.9 |
| 4329508 | LINCOLN TUNNEL W CENTER TUBE NY - NJ | Manhattan | 29.0 |
| 4616266 | BRP S FORDHAM ROAD - WATSON AVENUE | Bronx | 33.0 |
| 4616282 | ROUTE 169 S - MLK WALKER STREET | Staten Island | 34.1 |
| 4616217 | SIE W - MLK N WOOLEY AVENUE - WLAKER STREET | Staten Island | 38.3 |
| 4616196 | MLK S - SIE E WALKER STREET - WOOLEY AVENUE | Staten Island | 39.1 |
| 4329473 | LINCOLN TUNNEL E CENTER TUBE NJ - NY | Manhattan | 39.5 |
| 4620315 | CBE W L/LE V AMSTERDAM AVE - I 95 S LOC LNS | Bronx | 41.1 |
| 4616193 | SIE E BRADLEY AVENUE - CLOVE ROAD | Staten Island | 45.1 |

## Không gian

- Khoảng cách link → tâm ô: min 0.83 km, trung vị 4.41 km, max 6.75 km; vượt 6,7 km: 1 link; dùng 16 ô.
- Trùng với ô Open-Meteo tự chọn: 114/124. 10 link khác nhau vì API còn xét độ cao (cell_selection=land); giữ haversine theo D-11.

## Thời tiết

- Nhiệt độ -16.8 → 41.3 °C (TB 12.53); độ ẩm 18 → 100 %; gió max 59.9 km/h; gió giật max 119.5 km/h (trận gió 19/12/2025).
- Giá trị âm: 0. Mưa lớn nhất 21.4 mm/giờ; tuyết lớn nhất 3.29 cm/giờ. Giờ có tuyết khi nhiệt độ > 3 °C: 63 (cần lưu ý khi phân tích).
- Tổng năm theo ô: mưa 1021.0–1208.0 mm (trung vị 1098.0), tuyết 42.4–53.3 cm.

## Unknown member

Số dòng fact dùng khóa −1: 0.

## Hạn chế

- Thời tiết là giá trị mô hình (ECMWF IFS ~8 × 11 km), chưa đối chiếu trạm.
- `travel_time` là giây theo suy luận; metadata không ghi đơn vị.
- 12 link không đủ dữ liệu đêm khô để tính `free_flow_speed_mph` → trạng thái 'Không xác định'.