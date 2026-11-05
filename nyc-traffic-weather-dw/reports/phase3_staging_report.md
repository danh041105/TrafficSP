# Phase 3 – Staging

Batch `phase3-20261102T101958Z`. Chi tiết: `reports/phase3_staging.json`.

## Tiêu chí

| Tiêu chí | Kết quả |
|---|---|
| traffic_rows_equal_raw | ĐẠT |
| traffic_business_key_unique | ĐẠT |
| dst_ambiguous_flagged_2468 | ĐẠT |
| dst_nonexistent_zero | ĐẠT |
| utc_null_only_dst | ĐẠT |
| utc_offsets_est_edt_only | ĐẠT |
| all_124_links_rep_point | ĐẠT |
| all_links_mapped | ĐẠT |
| weather_intervals_16x8760 | ĐẠT |
| weather_no_nulls | ĐẠT |
| weather_equals_previous_staging | ĐẠT |

## Traffic (`data/staging/stg_traffic_reading/`, phân vùng theo tháng nguồn)

- 12 714 844 dòng, 124 link; khóa (`link_id`, `reading_ts_local`) duy nhất: 12 714 844.
- Mốc DST tính từ tzdata: giờ kép 2025-11-02 01:xx → 2468 bản ghi (`reading_ts_utc` NULL); giờ không tồn tại 2025-03-09 02:xx → 0 bản ghi.
- `utc_offset_min` chỉ nhận -300 (EST) và -240 (EDT). Theo tháng: {'2025-01': [-300, -300], '2025-02': [-300, -300], '2025-03': [-300, -240], '2025-04': [-240, -240], '2025-05': [-240, -240], '2025-06': [-240, -240], '2025-07': [-240, -240], '2025-08': [-240, -240], '2025-09': [-240, -240], '2025-10': [-240, -240], '2025-11': [-300, -240], '2025-12': [-300, -300]}.
- Phân loại: {'VALID': 9911819, 'speed_zero': 2802797, 'speed_zero_tt_positive': 120, 'speed_gt_max': 108}.
- Bản ghi thuộc chuỗi tốc độ đứng yên (≥ 12 lần): 10 753.
- Kiểm tra đổi giờ mùa xuân (1 link):

| local | UTC | offset |
|---|---|---|
| 2025-03-09 01:53:07 | 2025-03-09 06:53:07+00:00 | -300 |
| 2025-03-09 01:58:06 | 2025-03-09 06:58:06+00:00 | -300 |
| 2025-03-09 03:03:06 | 2025-03-09 07:03:06+00:00 | -240 |
| 2025-03-09 03:08:06 | 2025-03-09 07:08:06+00:00 | -240 |

## Road link (`stg_road_link.parquet`)

- 124 link, tất cả có điểm đại diện. 52 link có token lỗi (53 token); 6 điểm bị loại vì cách trung vị > 3 km (6 link).
- Mỗi link: tối đa 1 phiên bản hình học, 1 borough; 2 link có tên lỗi (đã chọn tên sạch phổ biến nhất).
- Link dùng chung hình học: [['4456477', '4456481', 'MDE S TBB EXIT RAMP - QUEENS ANCHORAGE'], ['4456481', '4456477', 'TBB S MANHATTAN LIFT SPAN - QUEENS ANCHORAGE'], ['4616196', '4616210', 'MLK S - SIE E WALKER STREET - WOOLEY AVENUE'], ['4616210', '4616196', 'MLK S - SIE W WALKER STREET - RICHMOND AVENUE']].
- Chiều dài sau làm sạch (km): {'count': 124.0, 'mean': 1.816, 'std': 0.746, 'min': 0.319, '25%': 1.192, '50%': 1.82, '75%': 2.29, 'max': 3.586}. Dài nhất: [['4616213', 'WSE N ARDEN AVENUE - VICTORY BLVD', 3.586], ['4616201', 'WSE S VICTORY BOULEVARD - ARDEN AVENUE', 3.54], ['4616272', 'VWE S MP4.63 (Exit 6 Jamaica Ave) - MP2.66 (Exit 2 Roackaway Blvd)', 3.348], ['4616310', 'CBE AMSTERDAM AVE (L/LVL) - MORRIS AVE', 3.277], ['4616352', 'CIP S Hempstead Tpk - Laurelton Pkwy @ SSP', 3.246]].

## Mapping link → ô (`road_weather_mapping.parquet`)

- 124/124 link được gán, dùng 16/16 ô. Khoảng cách (km): {'count': 124.0, 'mean': 4.27, 'std': 1.22, 'min': 0.83, '25%': 3.57, '50%': 4.41, '75%': 5.1, 'max': 6.75}.
- Xa hơn 6,7 km: 1 [['4616319', 6.747]]. Số ô chạm (đại diện/đầu/cuối): {1: 84, 2: 39, 3: 1}.
- Đối chiếu với ô Open-Meteo tự chọn (cell_selection=land): kiểm tra = True, trùng 114/124, ô ngoài tập 16: 0.
- Khác nhau: [['4362251', 'ifs_40.7381_-73.7872', 'ifs_40.8084_-73.7642', 5.199], ['4616196', 'ifs_40.5975_-74.0877', 'ifs_40.6678_-74.1927', 5.848], ['4616210', 'ifs_40.5975_-74.0877', 'ifs_40.6678_-74.1927', 5.848], ['4616216', 'ifs_40.5975_-74.0877', 'ifs_40.6678_-74.1927', 5.835], ['4616247', 'ifs_40.8084_-73.8921', 'ifs_40.8787_-73.8691', 5.287], ['4616250', 'ifs_40.8084_-73.8921', 'ifs_40.8787_-73.8691', 5.281], ['4616252', 'ifs_40.8084_-73.8921', 'ifs_40.8787_-73.8691', 3.977], ['4616257', 'ifs_40.6678_-73.9377', 'ifs_40.7381_-74.0425', 5.982], ['4616328', 'ifs_40.7381_-73.9149', 'ifs_40.6678_-73.9377', 5.624], ['4616339', 'ifs_40.6678_-73.9377', 'ifs_40.7381_-74.0425', 5.951]]

## Weather (`stg_weather_hourly.parquet`, `stg_weather_interval.parquet`)

- Theo giờ API: 140 928 dòng; khớp từng giá trị với staging trước đó (lệch: 0).
- Khoảng `[H, H+1)`: 140 160 dòng, 16 ô, từ 2025-01-01 05:00:00+00:00 đến 2026-01-01 04:00:00+00:00; null: 0; thiếu giờ H+1: 0.
- Căn giờ: nhiệt độ, độ ẩm, gió, gió giật @H; mưa, tuyết, weather_code @H+1 (D-09).