# Decisions Log

Trạng thái: **Đã quyết** (có căn cứ) · **Đã duyệt** (người dùng duyệt ngày 02/11/2026, áp dụng theo đề xuất).

| ID | Chủ đề | Quyết định / đề xuất | Căn cứ | Trạng thái |
|---|---|---|---|---|
| D-01 | Vị trí dự án và dữ liệu | Dự án ở `D:\nyc-traffic-weather-dw`. Dữ liệu nguồn giữ nguyên chỗ cũ (`D:\DE_dataset_sizing`), chỉ đọc qua `config/data_sources.yaml`. Phase 2 có thể sao chép vào `data/raw` kèm checksum (53 + 7 MB) | Ổ C còn 8,5 GB; tổng dữ liệu nhỏ | Đã duyệt |
| D-02 | Lưu trữ và engine | Parquet (zstd) là nơi lưu chính thức. DuckDB làm engine biến đổi, kiểm thử và OLAP (đọc thẳng Parquet; có thể kèm file `.duckdb` chứa view). Cần cài `duckdb`, `pytest` | Tương thích Lakehouse về sau | Đã duyệt (theo đề xuất) |
| D-03 | Giờ DST kép (01:00–01:59, 02/11/2025, 2 468 bản ghi) | Giữ trong `fact_traffic_reading` với `reading_ts_utc = NULL`, `is_dst_ambiguous = true`. Không gán vào `fact_traffic_hourly`; đếm riêng trong đối chiếu (≈ 2 giờ × 124 link, 0,02%) | Không phân biệt được lần 1 / lần 2 (các cặp bản ghi xen kẽ, đã bị va chạm một phần) | Đã duyệt |
| D-04 | Múi giờ | DW dùng UTC cho mọi khóa thời gian; giờ New York chỉ là thuộc tính (qua `dim_date`, `dim_time`) | 0 bản ghi trong giờ không tồn tại 2022–2025; tài liệu Open-Meteo xác nhận chế độ `timezone` bỏ qua DST | Đã quyết |
| D-05 | Bản ghi hợp lệ | `0 < speed ≤ 100` và `travel_time > 0`. Mọi bản ghi khác vẫn giữ, chỉ gắn `invalid_reason` | Tháng 1: 22 dòng > 100 mph (max 111,8), 22 dòng `travel_time < 0` | Đã duyệt |
| D-06 | Ngưỡng độ phủ khi phân tích | `n_valid ≥ 6` (≥ 50% số lần đo trong giờ). Chỉ là bộ lọc phân tích, không xóa dữ liệu | | Đã duyệt |
| D-07 | Link chất lượng thấp | `valid_share_2025 < 0,5` → `is_low_quality = true` (23 link). Giữ trong DW, loại khi phân tích mặc định | | Đã duyệt |
| D-08 | Sự cố toàn mạng | Một giờ có ≥ 90% link đang hoạt động mà không có bản ghi nào → `is_network_outage` | 19–20/06, 29/10, 01/12 | Đã duyệt |
| D-09 | Căn giờ thời tiết | Khoảng `[H, H+1)`: nhiệt độ, độ ẩm, gió, gió giật lấy tại H; mưa, tuyết, `weather_code` lấy tại H+1 | Tài liệu Open-Meteo: mưa/tuyết là "sum of the preceding hour"; gió giật là "of the indicated hour". Thực nghiệm: `weather_code` tại T khớp 99,99% với mưa ghi tại T | Đã quyết |
| D-10 | Mô hình thời tiết | ECMWF IFS 9 km, không trộn mô hình | 16 ô cho 124 link (ERA5 chỉ 5 ô). Tài liệu: IFS dùng "most up-to-date version", nên có thể đổi phiên bản trong năm; với nghiên cứu nhiều năm, tài liệu khuyên ERA5 | Đã quyết (ghi hạn chế) |
| D-11 | Ánh xạ link → ô | Trung vị điểm hợp lệ → ô đất liền gần nhất trong 16 ô (haversine); đối chiếu với ô Open-Meteo tự chọn; cờ nếu > 6,7 km. Nếu có link rơi vào ô **ngoài** 16 ô đã tải → dừng và hỏi bạn trước khi tải thêm | | Đã duyệt |
| D-12 | Surrogate key | Số nguyên, giữ ổn định qua `key_registry`; `date_key` = YYYYMMDD, `time_key` = 0–23; −1 = unknown | Kimball | Đã duyệt |
| D-13 | `dim_time` và khung giờ | Grain giờ (24 dòng). Khung giờ: Đêm 0–5, Cao điểm sáng 6–9, Ban ngày 10–14, Cao điểm chiều 15–18, Tối 19–23. **Quy tắc phân tích**, không phải định nghĩa chính thức | Hồ sơ tốc độ ngày thường: chậm nhất 7–9h và 15–18h | Đã duyệt (theo đề xuất) |
| D-14 | Ngày lễ / "ngày nghỉ" | 11 ngày lễ liên bang Mỹ 2025 (theo danh sách của OPM: 01/01, 20/01, 17/02, 26/05, 19/06, 04/07, 01/09, 13/10, 11/11, 27/11, 25/12). "Ngày nghỉ" = `day_type IN ('Holiday', 'Weekend')`. Thư viện `holidays` chưa cài, nên nhập tay kèm nguồn | | Đã duyệt (theo đề xuất) |
| D-15 | Unknown member | Mỗi dimension có dòng −1; mọi FK = −1 đều được đếm trong báo cáo chất lượng | Kimball | Đã duyệt |
| D-16 | Phân vùng | `fact_traffic_hourly`, `fact_traffic_weather_hourly` phân vùng theo `month_utc`; `fact_traffic_reading` phân vùng theo `source_month` (tháng giờ NY, vì `reading_ts_utc` để trống ở giờ DST kép); `fact_weather_hourly` và các dim không phân vùng | Dữ liệu nhỏ | Đã duyệt (làm rõ khi triển khai) |
| D-17 | `road_weather_mapping` | Bảng chi tiết ở staging; `dim_road_link` chỉ mang 3 cột (`weather_loc_key`, `mapping_distance_km`, `mapping_version`) để drill-across | Tránh bridge table; ánh xạ là 1 link → 1 ô | Đã duyệt |
| D-18 | Trạng thái giao thông | Tính ở truy vấn/mart, không lưu cứng: `free_flow_speed_mph` = p85 tốc độ hợp lệ lúc khô, 22:00–05:00, của từng link; Ùn tắc < 50%, Chậm 50–80%, Thông thoáng ≥ 80%. **Quy tắc phân tích, không phải tiêu chuẩn chính thức** | | Đã duyệt (theo đề xuất) |
| D-19 | Định nghĩa tốc độ trung bình | Mặc định: trung bình theo số lần đo, `SUM(sum_speed)/SUM(n_valid)`. Có thể suy ra tốc độ theo thời gian đi qua `sum_travel_time` nếu cần | | Đã quyết |
| D-20 | Lưới giờ của `fact_traffic_hourly` | Mỗi link từ giờ UTC đầu tiên đến giờ cuối cùng có dữ liệu; giờ trống có `n_records = 0`. Link 4456494 ngừng từ 06/01 rồi hoạt động lại, nên khoảng trống dài vẫn hiện trong lưới | Phân biệt "không có bản ghi" với "speed = 0" | Đã duyệt |
| D-21 | Gán bản ghi vào giờ | `floor(reading_ts_utc)`. `data_as_of` là thời điểm **cuối** chu kỳ đo, nên khoảng 1/12 bản ghi ở biên giờ có phần đo thuộc giờ trước. Chấp nhận; việc dịch −2,5 phút để sau | Metadata: "Last time data was received" | Đã duyệt |
| D-22 | Cột nguồn bỏ khỏi DW | `status`, `transcom_id`, `encoded_poly_line*` chỉ giữ ở dữ liệu gốc / staging | Metadata: status "Artifact – not useful"; `transcom_id` trùng 100% | Đã duyệt |
| D-23 | Dữ liệu thời tiết cũ | Không dùng `D:\DE_dataset_sizing\open_meteo_2025` (offset cố định −4h) | Phase 0 | Đã quyết |
| D-25 | Cờ giờ DST kép ở fact theo giờ | Thêm `is_dst_ambiguous_hour` vào `fact_traffic_hourly` và mart; 2 giờ này không tính là sự cố toàn mạng | Nếu không có cờ, 05Z/06Z ngày 02/11 có `n_records = 0` cho cả 124 link và bị gắn nhầm là sự cố | Đã quyết (Phase 5) |
| D-26 | Khác biệt ô Open-Meteo tự chọn | 10/124 link: ô gần nhất theo haversine khác ô API chọn (API còn xét độ cao). Giữ haversine (D-11), lưu `api_weather_location_id` trong `road_weather_mapping` để đối chiếu | Phase 3 | Đã quyết |
| D-24 | Hình học dùng chung | 124 link nhưng chỉ có 122 chuỗi `link_points` khác nhau. Giữ nguyên (mỗi link một dòng dim); kiểm tra ở Phase 3 xem có phải hai chiều của cùng một đoạn không | Phase 0 | Ghi nhận |

## Các hạn chế đã biết
- `travel_time` là giây theo suy luận (`speed × travel_time` gần như hằng số theo link); metadata không ghi đơn vị.
- Thời tiết là giá trị mô hình theo ô ~8 × 11 km, chưa đối chiếu với trạm quan trắc.
- IFS là mô hình vận hành, có thể nâng cấp phiên bản trong năm 2025.
- `link_points` của 49/122 link có token lỗi; vị trí đại diện có sai số tới vài trăm mét.
