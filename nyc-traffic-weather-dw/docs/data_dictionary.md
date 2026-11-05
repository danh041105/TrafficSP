# Data Dictionary

Kiểu dữ liệu theo DuckDB/Parquet. "Nguồn" ghi cột gốc hoặc quy tắc tạo ra cột. ❓ = chờ xác nhận (`decisions.md`).

## Dimensions

### `dim_date`
| Cột | Kiểu | Mô tả | Nguồn / quy tắc |
|---|---|---|---|
| `date_key` | INTEGER PK | YYYYMMDD; −1 = unknown | sinh |
| `full_date` | DATE | Ngày theo lịch New York | 2025-01-01 … 2025-12-31 |
| `day_of_week` | TINYINT | 1 = Thứ Hai … 7 = Chủ nhật (ISO) | |
| `day_name_vi` | VARCHAR | "Thứ Hai" … "Chủ nhật" | |
| `is_weekend` | BOOLEAN | Thứ Bảy / Chủ nhật | |
| `is_holiday` | BOOLEAN | Ngày lễ theo lịch đã chọn | ❓ D-14 |
| `holiday_name` | VARCHAR | Tên ngày lễ, NULL nếu không | ❓ D-14 |
| `day_type` | VARCHAR | `Holiday` > `Weekend` > `Weekday` (ưu tiên theo thứ tự) | |
| `iso_week`, `month`, `month_name_vi`, `quarter`, `year` | TINYINT / VARCHAR / SMALLINT | Phân cấp thời gian | |
| `is_dst_transition_day` | BOOLEAN | 09/03/2025 và 02/11/2025 | tzdata |

### `dim_time`
| Cột | Kiểu | Mô tả | Nguồn / quy tắc |
|---|---|---|---|
| `time_key` | TINYINT PK | Giờ trong ngày 0–23 (giờ New York); −1 = unknown | |
| `hour_label` | VARCHAR | "17:00" | |
| `day_part` | VARCHAR | Khung giờ: Đêm (0–5), Cao điểm sáng (6–9), Ban ngày (10–14), Cao điểm chiều (15–18), Tối (19–23) | ⚠️ quy tắc phân tích, ❓ D-13 |

### `dim_road_link`
| Cột | Kiểu | Mô tả | Nguồn / quy tắc |
|---|---|---|---|
| `road_key` | INTEGER PK | Surrogate key; −1 = unknown | `key_registry` |
| `link_id` | VARCHAR | Business key (mã TRANSCOM) | `link_id` (= `transcom_id`) |
| `version` | SMALLINT | Phiên bản (2025: luôn 1) | SCD |
| `source_internal_id` | VARCHAR | Mã nội bộ của nguồn (1:1 với link) | `id` |
| `link_name` | VARCHAR | Tên đã sửa lỗi (SCD1) | `link_name`, chọn bản phổ biến nhất, bỏ bản có ký tự `\` lặp |
| `borough` | VARCHAR | Manhattan / Brooklyn / Queens / Bronx / Staten Island | `borough` |
| `city` | VARCHAR | "New York City" | hằng số |
| `owner` | VARCHAR | Đơn vị sở hữu cảm biến | `owner` |
| `geometry_wkt` | VARCHAR | LINESTRING (lon lat, …) từ các điểm hợp lệ | `link_points` đã làm sạch |
| `n_points_raw`, `n_points_valid` | SMALLINT | Số điểm trước/sau khi làm sạch | |
| `rep_lat`, `rep_lon` | DOUBLE | Điểm đại diện = trung vị điểm hợp lệ | |
| `weather_loc_key` | INTEGER FK | Ô thời tiết được gán (cho drill-across) | `road_weather_mapping` |
| `mapping_distance_km` | DOUBLE | Khoảng cách điểm đại diện → tâm ô | haversine |
| `mapping_version` | VARCHAR | `v1` | |
| `first_seen_utc`, `last_seen_utc` | TIMESTAMPTZ | Lần đầu / cuối có bản ghi trong 2025 | |
| `valid_share_2025` | DOUBLE | Tỷ lệ bản ghi hợp lệ cả năm | |
| `is_low_quality` | BOOLEAN | `valid_share_2025 < 0,5` | ❓ D-07 |
| `free_flow_speed_mph` | DOUBLE | Tốc độ thông thoáng tham chiếu (dùng để xếp trạng thái) | ⚠️ ❓ D-18 |
| `row_effective_from`, `row_effective_to`, `is_current` | TIMESTAMPTZ / BOOLEAN | Metadata SCD (sẵn sàng cho Type 2) | |

### `dim_weather_location`
| Cột | Kiểu | Mô tả | Nguồn / quy tắc |
|---|---|---|---|
| `weather_loc_key` | INTEGER PK | Surrogate key; −1 = unknown | `key_registry` |
| `weather_location_id` | VARCHAR | Business key, ví dụ `ifs_40.7381_-74.0425` | tên file JSON |
| `model` | VARCHAR | `ecmwf_ifs` | |
| `grid_lat`, `grid_lon`, `elevation_m` | DOUBLE | Tâm ô và độ cao do API trả về | JSON gốc |
| `n_links_mapped` | SMALLINT | Số link được gán vào ô | mapping |
| `primary_borough` | VARCHAR | Borough có nhiều link nhất trong ô (chỉ để tham khảo) | mapping |

### `dim_weather_condition`
| Cột | Kiểu | Mô tả | Nguồn / quy tắc |
|---|---|---|---|
| `condition_key` | SMALLINT PK | Surrogate key; −1 = unknown | |
| `weather_code` | SMALLINT | Mã WMO (0, 1, 2, 3, 45, 48, 51…99) | bảng mã WMO của Open-Meteo |
| `description_vi`, `description_en` | VARCHAR | Mô tả | |
| `category` | VARCHAR | Clear / Cloudy / Fog / Drizzle / Rain / Showers / Snow / Thunderstorm | |
| `precip_type` | VARCHAR | none / liquid / frozen | |
| `severity_rank` | TINYINT | 0 (quang) … 9 (dông có mưa đá) | quy tắc phân tích ⚠️ |

## Facts

### `fact_traffic_reading` (PK: `road_key`, `reading_ts_local`)
| Cột | Kiểu | Mô tả | Nguồn / quy tắc |
|---|---|---|---|
| `road_key` | INTEGER FK | | `link_id` → `dim_road_link` |
| `date_key`, `time_key` | INTEGER / TINYINT FK | Ngày, giờ theo giờ New York của bản ghi gốc | `data_as_of` |
| `reading_ts_local` | TIMESTAMP | Giá trị `data_as_of` gốc (không múi giờ) | `data_as_of` |
| `reading_ts_utc` | TIMESTAMPTZ | NULL nếu giờ DST kép | tzdata America/New_York |
| `utc_offset_min` | SMALLINT | −300 (EST) / −240 (EDT); NULL nếu giờ kép | |
| `speed_mph` | DOUBLE | Giá trị gốc, không sửa | `speed` |
| `travel_time_s` | INTEGER | Giá trị gốc, không sửa | `travel_time` |
| `is_valid` | BOOLEAN | `0 < speed ≤ 100` và `travel_time > 0` | ❓ D-05 |
| `invalid_reason` | VARCHAR | NULL / `speed_zero` / `speed_gt_max` / `travel_time_nonpositive` / `speed_zero_tt_positive` | |
| `is_dst_ambiguous` | BOOLEAN | Bản ghi 01:00–01:59 ngày 02/11/2025 | |
| `is_stuck` | BOOLEAN | Thuộc chuỗi ≥ 12 lần liên tiếp cùng speed > 0 | chỉ là cờ |
| `source_file` | VARCHAR | Ví dụ `2025-03.parquet` | lineage |
| `load_batch_id` | VARCHAR | Mã lần nạp | lineage |
| `source_month` | VARCHAR | Khóa phân vùng = tháng của file nguồn (= tháng `reading_ts_local`) | D-16 |

Cột nguồn **không** đưa vào fact (vẫn còn trong dữ liệu gốc): `status` (metadata: "Artifact – not useful"), `transcom_id` (trùng `link_id`), `encoded_poly_line*` (trùng `link_points`, có ký tự lỗi), `link_points` / `link_name` / `borough` / `owner` (thuộc dim).

### `fact_weather_hourly` (PK: `weather_loc_key`, `interval_start_utc`)
| Cột | Kiểu | Mô tả | Lấy từ dòng API |
|---|---|---|---|
| `weather_loc_key` | INTEGER FK | | |
| `interval_start_utc` | TIMESTAMPTZ | H; khoảng `[H, H+1)` | |
| `date_key`, `time_key` | FK | Theo giờ New York của H | |
| `condition_key` | SMALLINT FK | | `weather_code` @ H+1 |
| `temperature_c` | DOUBLE | Nhiệt độ 2 m | @ H |
| `relative_humidity_pct` | DOUBLE | Độ ẩm 2 m | @ H |
| `wind_speed_kmh` | DOUBLE | Gió 10 m | @ H |
| `wind_gust_kmh` | DOUBLE | Gió giật 10 m | @ H |
| `precipitation_mm` | DOUBLE | Tổng mưa trong khoảng | @ H+1 |
| `snowfall_cm` | DOUBLE | Tổng tuyết trong khoảng | @ H+1 |
| `model` | VARCHAR | `ecmwf_ifs` | |
| `source_file`, `load_batch_id` | VARCHAR | Lineage | |

### `fact_traffic_hourly` (PK: `road_key`, `hour_utc`)
| Cột | Kiểu | Mô tả |
|---|---|---|
| `road_key` | INTEGER FK | |
| `hour_utc` | TIMESTAMPTZ | Đầu khoảng giờ |
| `date_key`, `time_key` | FK | Theo giờ New York của `hour_utc` |
| `n_records`, `n_valid`, `n_zero`, `n_invalid_other` | SMALLINT | Đếm (additive) |
| `sum_speed`, `sum_speed_sq`, `sum_travel_time` | DOUBLE | Tổng trên bản ghi hợp lệ (additive) |
| `min_speed`, `max_speed` | DOUBLE | Semi-additive (MIN/MAX) |
| `median_speed` | DOUBLE | Non-additive, chỉ dùng ở cấp giờ |
| `coverage_ratio` | DOUBLE | `n_valid / 12`. Có thể > 1 ở giờ ghi bù (206 link-giờ) |
| `has_stuck` | BOOLEAN | Có ít nhất 1 bản ghi `is_stuck` |
| `is_network_outage` | BOOLEAN | Giờ có ≥ 90% link đang hoạt động không có bản ghi (D-08); không tính 2 giờ DST kép |
| `is_dst_ambiguous_hour` | BOOLEAN | 2 giờ UTC (05Z, 06Z ngày 02/11/2025) ứng với giờ DST kép: `n_records = 0` vì bản ghi không gán được giờ, **không phải mất dữ liệu** (D-25) |
| `load_batch_id` | VARCHAR | |
| `month_utc` | VARCHAR | Khóa phân vùng `YYYY-MM` theo UTC (13 giá trị: 2025-01 … 2026-01, vì tối 31/12 giờ NY là 01/2026 UTC) |

### `fact_traffic_weather_hourly` (PK: `road_key`, `hour_utc`)
Gồm toàn bộ cột của `fact_traffic_hourly`, cộng thêm `weather_loc_key`, `condition_key`, `temperature_c`, `relative_humidity_pct`, `wind_speed_kmh`, `wind_gust_kmh`, `precipitation_mm`, `snowfall_cm` (lấy từ `fact_weather_hourly` cùng ô, cùng khoảng giờ).

## Staging

| Bảng | Grain | Ghi chú |
|---|---|---|
| `stg_traffic_reading` | 1 dòng nguồn | Giữ `link_id`, `data_as_of`, `speed`, `travel_time`, `status` gốc + cột chuẩn hóa + lineage |
| `stg_road_link` | 1 link | `link_points` gốc, token lỗi, điểm hợp lệ, điểm đại diện, tên các phiên bản |
| `road_weather_mapping` | 1 link | `link_id`, `weather_location_id`, `distance_km`, `api_cell_agrees`, `cells_touched`, `mapping_method`, `mapping_version` |
| `stg_weather_hourly` | 1 ô × 1 mốc giờ API | Đúng như JSON gốc (UTC), trước khi căn khoảng |
