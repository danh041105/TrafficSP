# Dimensional Model

Phương pháp: Kimball & Ross, *The Data Warehouse Toolkit*. Mô hình hình sao (star schema), không snowflake, không bridge table. Sơ đồ: [`star_schema.png`](star_schema.png) ([SVG](star_schema.svg)). Mỗi fact được vẽ thành một ngôi sao riêng, nên các đường khóa ngoại không chồng chéo nhau.

Ký hiệu: ✅ đã kiểm chứng trên dữ liệu · ⚠️ quy tắc phân tích / giả định · ❓ chờ xác nhận (xem `decisions.md`).

---

## 1. Tổng quan bảng

| Bảng | Loại | Grain | Khóa chính (PK) | Số dòng dự kiến |
|---|---|---|---|---|
| `dim_date` | Dimension | 1 ngày theo lịch New York | `date_key` (YYYYMMDD) | 365 + 1 unknown |
| `dim_time` | Dimension | 1 giờ trong ngày (giờ New York) | `time_key` (0–23) | 24 + 1 unknown |
| `dim_road_link` | Dimension | 1 phiên bản đoạn đường | `road_key` (surrogate) · duy nhất (`link_id`, `version`) | 124 + 1 unknown |
| `dim_weather_location` | Dimension | 1 ô lưới thời tiết | `weather_loc_key` (surrogate) · duy nhất `weather_location_id` | 16 + 1 unknown |
| `dim_weather_condition` | Dimension | 1 mã WMO | `condition_key` (surrogate) · duy nhất `weather_code` | 28 + 1 unknown |
| `fact_traffic_reading` | Atomic transaction fact | 1 lần đo của 1 đoạn đường, đúng như nguồn gửi | (`road_key`, `reading_ts_local`) | 12 714 844 ✅ |
| `fact_weather_hourly` | Atomic periodic fact | 1 ô × 1 khoảng giờ UTC `[H, H+1)` | (`weather_loc_key`, `interval_start_utc`) | 16 × 8 760 = 140 160 |
| `fact_traffic_hourly` | Aggregate fact (lưới đầy đủ) | 1 đoạn đường × 1 khoảng giờ UTC, trong thời gian link hoạt động | (`road_key`, `hour_utc`) | ~1,08 triệu |
| `fact_traffic_weather_hourly` | Consolidated fact (mart OLAP) | Như `fact_traffic_hourly`, kèm thời tiết | (`road_key`, `hour_utc`) | bằng `fact_traffic_hourly` |

Staging (không thuộc DW nhưng giữ business key và lineage): `stg_traffic_reading`, `stg_weather_hourly`, `stg_road_link`, `road_weather_mapping`.

---

## 2. Grain và khóa từng fact

### 2.1 `fact_traffic_reading`
- **Grain:** mỗi dòng nguồn NYC DOT là một dòng fact, không gộp, không bỏ dòng nào.
- **Khóa nghiệp vụ:** (`link_id`, `data_as_of`) ✅ duy nhất trên 12 714 844 dòng.
- **PK:** (`road_key`, `reading_ts_local`). `reading_ts_local` chính là `data_as_of` gốc (không múi giờ). Không dùng `reading_ts_utc` làm khóa vì nó để trống ở giờ DST kép.
- **FK:** `road_key` → `dim_road_link`; `date_key` → `dim_date`; `time_key` → `dim_time`. Hai khóa ngày/giờ lấy theo **giờ New York của bản ghi gốc**, nên vẫn xác định được kể cả ở giờ kép.
- **Measures:** `speed_mph`, `travel_time_s` (giá trị gốc, không sửa). Các cờ: `is_valid`, `invalid_reason`, `is_dst_ambiguous`, `is_stuck`.

### 2.2 `fact_weather_hourly`
- **Grain:** 1 ô × khoảng `[H, H+1)` UTC, với H là giờ tròn. Chỉ gồm các khoảng có giờ bắt đầu thuộc năm 2025 theo giờ New York.
- **PK:** (`weather_loc_key`, `interval_start_utc`).
- **FK:** `weather_loc_key`, `date_key` / `time_key` (theo giờ New York của `interval_start_utc`), `condition_key`.
- **Căn giờ** ✅ (tài liệu Open-Meteo + kiểm chứng thực nghiệm):

| Measure | Lấy từ dòng API có mốc | Lý do |
|---|---|---|
| `temperature_c`, `relative_humidity_pct`, `wind_speed_kmh`, `wind_gust_kmh` | **H** | Tài liệu: giá trị tức thời của giờ được ghi |
| `precipitation_mm`, `snowfall_cm` | **H+1** | Tài liệu: "sum of the preceding hour" |
| `condition_key` (từ `weather_code`) | **H+1** | Thực nghiệm: mã tại T khớp 99,99% với mưa ghi tại T (giờ trước), chỉ 93,7% với giờ sau |

### 2.3 `fact_traffic_hourly`
- **Grain:** 1 link × 1 khoảng giờ UTC, **lưới đầy đủ** từ giờ đầu tiên đến giờ cuối cùng link có dữ liệu. Giờ không có bản ghi vẫn có dòng, với `n_records = 0`. Như vậy phân biệt được "không có bản ghi" (`n_records = 0`) với "có bản ghi nhưng speed = 0" (`n_zero > 0`).
- **Gán giờ:** `hour_utc = floor(reading_ts_utc, 1 giờ)`. Bản ghi giờ DST kép (`reading_ts_utc` trống) **không** được gán vào giờ nào, nhưng được đếm trong đối chiếu. ❓ D-03
- **PK:** (`road_key`, `hour_utc`). **FK:** `road_key`, `date_key`, `time_key` (theo giờ New York của `hour_utc`).

### 2.4 `fact_traffic_weather_hourly`
- **Grain:** giống `fact_traffic_hourly`, dòng đối dòng.
- **Cách tạo:** `fact_traffic_hourly` ⋈ `dim_road_link` (lấy `weather_loc_key`) ⋈ `fact_weather_hourly` trên (`weather_loc_key`, `hour_utc = interval_start_utc`). Ràng buộc: số dòng sau ghép = số dòng `fact_traffic_hourly`.
- **FK:** `road_key`, `date_key`, `time_key`, `weather_loc_key`, `condition_key`.

---

## 3. Measures và quy tắc tổng hợp

### 3.1 Giao thông

| Measure | Định nghĩa | Loại | Cách roll-up |
|---|---|---|---|
| `n_records` | số bản ghi trong giờ | Additive | `SUM` |
| `n_valid` | số bản ghi có `0 < speed ≤ 100` và `travel_time > 0` ❓ D-05 | Additive | `SUM` |
| `n_zero` | số bản ghi `speed = 0` | Additive | `SUM` |
| `n_invalid_other` | số bản ghi lỗi khác (speed > 100, travel_time ≤ 0 khi speed > 0) | Additive | `SUM` |
| `sum_speed` | tổng speed của các bản ghi hợp lệ | Additive | `SUM` |
| `sum_speed_sq` | tổng bình phương speed hợp lệ | Additive | `SUM` |
| `sum_travel_time` | tổng travel_time hợp lệ | Additive | `SUM` |
| `min_speed`, `max_speed` | nhỏ nhất / lớn nhất trong các bản ghi hợp lệ | Semi-additive | `MIN` / `MAX` (không cộng) |
| `median_speed` | trung vị trong giờ | Non-additive | **Không roll-up được.** Muốn trung vị ở cấp cao hơn thì tính lại từ `fact_traffic_reading` |
| `coverage_ratio` | `n_valid / 12` | Non-additive | Tính lại: `SUM(n_valid) / (12 × COUNT(*))` |

Công thức chuẩn khi roll-up (không bao giờ dùng `AVG(AVG(speed))`):
```sql
avg_speed  = SUM(sum_speed) / NULLIF(SUM(n_valid), 0)
std_speed  = SQRT( SUM(sum_speed_sq) / NULLIF(SUM(n_valid),0) - POWER(SUM(sum_speed)/NULLIF(SUM(n_valid),0), 2) )
avg_tt     = SUM(sum_travel_time) / NULLIF(SUM(n_valid), 0)
```
Trung bình là **trung bình theo số lần đo** (mỗi lần đo 5 phút có trọng số như nhau). Giờ có > 12 bản ghi (ghi bù, ✅ 330 link-giờ) sẽ nặng hơn một chút; cờ có thể suy ra từ `n_records > 12`.

### 3.2 Thời tiết

| Measure | Theo thời gian (cùng ô) | Theo không gian (nhiều ô) | Ghi chú |
|---|---|---|---|
| `temperature_c`, `relative_humidity_pct` | `AVG`, `MIN`, `MAX` | `AVG` theo ô | Non-additive |
| `precipitation_mm`, `snowfall_cm` | `SUM` | `AVG` của tổng từng ô | **Không cộng qua nhiều ô hoặc nhiều link** |
| `wind_speed_kmh` | `AVG`, `MAX` | `AVG` / `MAX` | Non-additive |
| `wind_gust_kmh` | `MAX` | `MAX` | Non-additive |
| `condition_key` | đếm số giờ theo nhóm; điều kiện "nặng nhất" theo `severity_rank` | như cột bên trái | Thuộc tính phân loại, không phải số |

**Bẫy ở fact hợp nhất:** một ô có thể ứng với 23 link, nên cột mưa bị lặp 23 lần. Không được `SUM(precipitation_mm)` trên `fact_traffic_weather_hourly`. Lượng mưa theo khu vực phải lấy từ `fact_weather_hourly`, hoặc trước hết `SELECT DISTINCT weather_loc_key, hour_utc` rồi mới tổng hợp.

---

## 4. Hierarchy

| Hierarchy | Các cấp | Nằm ở |
|---|---|---|
| Thời gian | Giờ (`dim_time.hour`) → Ngày (`dim_date.full_date`) → Tháng → Quý → Năm | `dim_time` + `dim_date`. Nhánh phụ: Ngày → Tuần ISO; Ngày → Loại ngày (ngày thường / cuối tuần / ngày lễ) |
| Giờ trong ngày | Giờ → Khung giờ (`day_part`) ⚠️ D-13 | `dim_time` |
| Địa lý (đường) | Road Link → Borough → NYC | `dim_road_link` (`link_name` → `borough` → `city`) |
| Địa lý (thời tiết) | Weather Location → NYC | `dim_weather_location`. Một ô có thể trùm nhiều borough, nên không gắn ô vào một borough cứng; chỉ có `primary_borough` để tham khảo |
| Điều kiện thời tiết | Code → Category → Precipitation type | `dim_weather_condition` |

---

## 5. Surrogate key, Unknown Member, SCD

**Surrogate key** là số nguyên. Bảng `key_registry` (lưu cùng dimension) ghi (business key, version) → key. Khi chạy lại, key cũ được giữ nguyên và chỉ cấp số mới cho bản ghi mới, nên key ổn định qua các lần nạp. Riêng `date_key` là YYYYMMDD và `time_key` là 0–23 (smart key, đúng khuyến nghị Kimball cho chiều ngày).

**Unknown member** (`key = -1`) có ở mỗi dimension, mục đích là không để FK trống và không che giấu lỗi:

| Khi nào dùng −1 | Đếm và báo cáo ở |
|---|---|
| Link không gán được ô thời tiết | `data_quality_report.md` (kỳ vọng 0) |
| `weather_code` trống hoặc không có trong dimension | như trên (kỳ vọng 0) |
| `link_id` không có trong `dim_road_link` | như trên (kỳ vọng 0) |

**SCD:**

| Thuộc tính `dim_road_link` | Kiểu | Ghi chú |
|---|---|---|
| `link_name` (sửa 2 tên lỗi), `owner`, `is_low_quality` | **Type 1** (ghi đè) | Sửa lỗi, không cần lịch sử |
| `geometry_wkt`, `borough`, `weather_loc_key` / `mapping_version` | Type 1 hiện tại; **ứng viên Type 2** | ✅ Không đổi trong 2025. Nếu sau này đổi thì tạo phiên bản mới |
| `version`, `row_effective_from`, `row_effective_to`, `is_current` | Metadata sẵn sàng cho Type 2 | Năm 2025 mọi link có `version = 1`, `row_effective_to = 9999-12-31` |

`dim_weather_location` và `dim_weather_condition` là tĩnh (Type 0/1).

---

## 6. Quy tắc ánh xạ

### 6.1 Thời gian
1. `reading_ts_local` = `data_as_of` gốc. Gắn múi giờ `America/New_York` theo căn cứ ✅: 0 bản ghi trong 02:00–02:59 các ngày chuyển giờ mùa xuân 2022–2025; hồ sơ tốc độ khớp giờ cao điểm địa phương; metadata không ghi offset.
2. Giờ không tồn tại (02:xx ngày 09/03/2025): kỳ vọng 0 dòng ✅. Nếu có thì `invalid_reason = 'nonexistent_local_time'`, không xóa.
3. Giờ kép (01:00–01:59 ngày 02/11/2025, ✅ 2 468 bản ghi): `is_dst_ambiguous = true`, `reading_ts_utc = NULL`. ❓ D-03
4. Thời tiết tải theo UTC (`timezone=GMT`). Tài liệu Open-Meteo xác nhận chế độ `timezone` khác dùng một offset cố định tại thời điểm gọi API và **bỏ qua DST** ✅.
5. `date_key` / `time_key` của các fact theo giờ được tính từ giờ UTC đổi sang New York. Ngày 02/11 sẽ có hai `hour_utc` cùng ứng với `time_key = 1`; điều này hợp lệ vì PK là `hour_utc`.

### 6.2 Không gian
1. Làm sạch `link_points`: bỏ token bị cắt hoặc dính, điểm ngoài khung NYC, điểm cách trung vị quá 3 km.
2. Điểm đại diện = trung vị các điểm hợp lệ. `geometry_wkt` = LINESTRING từ các điểm hợp lệ.
3. `weather_loc_key` = ô gần nhất theo haversine trong 16 ô đất liền đã tải. Đối chiếu với ô Open-Meteo tự chọn (`cell_selection=land`: ô đất liền có độ cao tương tự, theo tài liệu). Mọi khác biệt được báo cáo.
4. Lưu `mapping_distance_km`, `mapping_method = 'nearest_land_cell_haversine_median_point'`, `mapping_version = 'v1'`. Bật cờ nếu khoảng cách > 6,7 km.

---

## 7. Kiểm tra thiết kế bằng truy vấn mục tiêu

*"Các đoạn đường ở Manhattan lúc 17:00 ngày nghỉ: tốc độ, trạng thái giao thông, thời tiết"* (ví dụ ngày lễ 04/07/2025):

```sql
SELECT r.link_name,
       f.sum_speed / NULLIF(f.n_valid, 0)            AS avg_speed_mph,
       f.n_valid, f.coverage_ratio,
       CASE WHEN f.n_valid < 6 THEN 'Không đủ dữ liệu'
            WHEN f.sum_speed / f.n_valid < 0.5 * r.free_flow_speed_mph THEN 'Ùn tắc'      -- quy tắc phân tích ⚠️ D-18
            WHEN f.sum_speed / f.n_valid < 0.8 * r.free_flow_speed_mph THEN 'Chậm'
            ELSE 'Thông thoáng' END                   AS traffic_status,
       c.category_vi, f.temperature_c, f.precipitation_mm, f.snowfall_cm, f.wind_gust_kmh
FROM fact_traffic_weather_hourly f
JOIN dim_road_link r          ON r.road_key = f.road_key
JOIN dim_date d               ON d.date_key = f.date_key
JOIN dim_time t               ON t.time_key = f.time_key
JOIN dim_weather_condition c  ON c.condition_key = f.condition_key
WHERE r.borough = 'Manhattan' AND t.time_key = 17
  AND d.day_type IN ('Holiday', 'Weekend')            -- "ngày nghỉ" ❓ D-14
  AND d.full_date = DATE '2025-07-04'
ORDER BY avg_speed_mph;
```
Mọi cột cần cho truy vấn đều đã có trong mô hình: borough nằm ở dim đường, ngày lễ ở dim ngày, giờ 17 ở dim giờ, thời tiết ở fact hợp nhất. Riêng `free_flow_speed_mph` (tốc độ thông thoáng của từng link, ví dụ p85 lúc khô ban đêm) là thuộc tính phân tích ❓ D-18.

---

## 8. Ánh xạ sang Lakehouse sau này

Mọi bảng là Parquet phân vùng theo tháng UTC, khóa thời gian là UTC, có `load_batch_id` và `source_file`. Khi chuyển sang MinIO/Iceberg thì giữ nguyên schema; khi phát lại realtime thì `fact_traffic_reading` (sắp theo `reading_ts_utc`) chính là luồng sự kiện.
