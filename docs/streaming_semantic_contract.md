# Hợp đồng ngữ nghĩa Streaming — TrafficSP

Hợp đồng chung cho Speed Layer, Batch Layer và Query Layer. Mọi quy tắc nằm trong code dùng chung
(`jobs/common/time_rules.py`, `quality_rules.py`, `aggregations.py`) và **được cả hai lớp gọi**. Không có bản sao quy tắc riêng.

## 1. Thời gian

| Khái niệm | Định nghĩa |
|---|---|
| `data_as_of` | Chuỗi giờ **New York** của nguồn NYC DOT, không kèm offset. Payload Kafka giữ nguyên chuỗi gốc. |
| `event_time_utc` | `data_as_of` đổi sang UTC theo tzdata. **NULL** nếu giờ mơ hồ (lùi giờ, 2025-11-02 01:00–01:59) hoặc không tồn tại (tiến giờ, 2025-03-09 02:00–02:59). Mốc DST tính từ `zoneinfo`, không viết cứng. |
| `kafka_ts` | Thời điểm message vào Kafka, theo giờ hệ thống thật. Chỉ dùng để đo độ trễ, **không** dùng để gom nhóm. |
| Đồng hồ sự kiện | `max(event_time)` đã nhận trên toàn mạng. Dùng để xác định STALE và chọn T_batch tự động. Không dùng giờ hệ thống, vì dữ liệu phát lại là năm 2025 còn máy chạy năm 2026. |
| Mọi gom nhóm | Theo `event_time_utc`; cửa sổ `[start, start + 5')` căn theo epoch UTC, giờ `[H, H + 1)` UTC. |

Bản ghi DST mơ hồ/không tồn tại:
- **Speed**: bị loại khỏi `traffic_5min_rt` và `traffic_latest`; vẫn được đếm trong `speed_batch_stats.n_dst_ambiguous` / `n_dst_nonexistent`.
- **Batch**: không vào `silver.traffic_cleaned`, được ghi vào `silver.traffic_quarantine` (`reason = dst_ambiguous | dst_nonexistent | missing_key`) và đếm trong `batch_run_stats.n_dst_excluded`. Con số này đếm **trước** khử trùng nên có thể lớn hơn số bản gốc khi Bronze chứa bản lặp (vòng 2: 2.572 dòng quarantine so với 2.468 bản gốc).

## 2. Chất lượng

- Hợp lệ khi `0 < speed ≤ 100` (mph) **và** `travel_time > 0`.
- Lý do không hợp lệ, xét theo thứ tự ưu tiên: `null_value`, `speed_negative`, `speed_zero_tt_positive`, `speed_zero`, `speed_gt_max`, `travel_time_nonpositive`.
- Không sửa giá trị gốc; chỉ thêm `is_valid` và `invalid_reason`.
- `speed = 0` nghĩa là "có quan trắc nhưng không có số đo". Điều này **khác** với "không có quan trắc" (thiếu bản ghi). Hệ thống không bao giờ suy ra speed = 0 từ việc thiếu dữ liệu.

## 3. Grain và thước đo

| Bảng | Grain | Thước đo |
|---|---|---|
| `traffic_latest` | 1 dòng / link: bản **hợp lệ** có `event_time` lớn nhất | speed, travel_time, event_time, kafka_ts |
| `traffic_latest_any` | 1 dòng / link: bản **bất kỳ** có `event_time` lớn nhất | thêm is_valid, invalid_reason |
| `traffic_5min_rt`, `traffic_5min_batch` | (link_id, window_start) | `n_records, n_valid, n_zero, n_invalid_other, sum_speed, sum_speed_sq, sum_travel_time, min_speed, max_speed` (thống kê speed chỉ tính trên bản hợp lệ) |
| `traffic_hourly` | (link_id, hour_utc) | Tổng các thành phần 5 phút. Trung bình = `sum_speed / n_valid` |

Không lưu và không cộng trung bình của trung bình. Roll-up chỉ cộng các thành phần cộng được, sau đó mới tính lại trung bình.

## 4. Khử trùng và tính idempotent

- **Nguồn**: khoá nghiệp vụ là `(link_id, data_as_of)`. Bản phát lặp (producer gửi lại, chạy lại với `run_id` khác, bản sao skew) phải cho cùng kết quả.
- **Speed 5 phút**: `dropDuplicates([link_id, data_as_of, event_time_utc])` trong watermark.
- **Silver**: `deduplicate(link_id, traffic_utc)`.
- **Sink**: mỗi INSERT gắn `insert_deduplication_token = <query>:<batchId>`. Bảng là ReplacingMergeTree theo grain và mọi đọc đi qua `FINAL`.
- Bảo đảm: Spark xử lý **at-least-once** (chạy lại micro-batch từ checkpoint). Nhờ sink idempotent, kết quả phục vụ là **exactly-once hiệu quả theo grain**. Đã kiểm chứng: kill -9 + ClickHouse ngừng 40 giây → 35.092/35.092 cửa sổ khớp, 0 dòng vật lý trùng.
- Latest: `ReplacingMergeTree(event_time)` nên bản cũ đến muộn không ghi đè bản mới hơn.

## 5. Dữ liệu muộn

| Lớp | Chính sách |
|---|---|
| Speed 5 phút | Watermark = `SPEED_WATERMARK` (mặc định 10 phút event-time) tính từ event_time lớn nhất. Cửa sổ chỉ phát khi watermark vượt `window_end` và **không cập nhật lại** (append). Bản muộn hơn watermark bị loại, đếm ở `stream_progress.rows_dropped_by_watermark`. |
| Speed latest | Không có watermark. Bản đến muộn vẫn được ghi, nhưng chỉ thắng nếu `event_time` lớn hơn bản đang có. |
| Batch | Recompute **cả ngày UTC** từ Silver, nên mọi bản muộn đã vào Bronze/Silver đều được tính. Bản muộn đến sau khi đã chốt: chạy lại `batch_views.py` cho ngày bị ảnh hưởng (REPLACE PARTITION); T_batch không đổi. |

## 6. Hợp nhất Lambda

- `T_batch = max(t_batch)` của các dòng `PUBLISHED` trong `lambda_handoff` (1970-01-01 nếu chưa publish lần nào). Luôn căn theo bội 5 phút.
- `v_traffic_5min_unified` lấy batch cho `window_start < T_batch` và speed cho `window_start ≥ T_batch`. Hai tập rời nhau theo định nghĩa: không chồng lấn và không hở.
- Điều kiện publish: batch đủ so với Silver, không trùng grain, và phủ mọi cửa sổ realtime trong `[T_old, T_new)` (`covers_realtime`; bỏ qua được bằng `--allow-rt-without-batch`).
- `--until auto`: `T_new = floor5(max event_time của Silver − watermark)`.

## 7. Trạng thái giao thông (D-18)

- Free-flow của mỗi link là p85 của speed hợp lệ lúc trời khô, khung 22:00–05:59 giờ New York, cần ≥ 100 mẫu (`link_dim.free_flow_speed`).
- Gọi `ratio = speed / free_flow`. `CONGESTED` khi ratio < 0,5; `SLOW` khi ratio < 0,8; còn lại `FREE_FLOW`.
- Trạng thái đặc biệt:
  - `NO_BASELINE`: chưa có free-flow;
  - `NO_VALID_DATA`: có bản ghi nhưng `n_valid = 0`;
  - `NO_MAPPING`: link chưa gán ô thời tiết;
  - `NO_DATA`: không có bản ghi;
  - `STALE`: chỉ ở latest, khi bản hợp lệ cuối cũ hơn đồng hồ sự kiện quá `STALE_AFTER_MINUTES` (15).
- Latest: nếu quan trắc mới nhất **không hợp lệ** và mới hơn bản hợp lệ cuối thì trạng thái là `NO_VALID_DATA`, không hiển thị tốc độ cũ như hiện hành.

## 8. Thời tiết theo khoảng giờ

Bản ghi Open-Meteo mốc `T` chứa nhiệt độ/độ ẩm/gió **tức thời tại T**, và mưa/tuyết/weather_code **của giờ (T−1h, T]**.
Khoảng giao thông `[H, H+1)` ghép như sau:
- nhiệt độ, độ ẩm, gió, gió giật: lấy mốc **H**;
- mưa, tuyết, weather_code: lấy mốc **H+1**, khi đó `weather_status = FINAL`;
- nếu chưa có mốc H+1 (realtime, giờ chưa kết thúc): tạm dùng mưa/tuyết của mốc H với `weather_status = PROVISIONAL`. Khi mốc H+1 đến, view tự chuyển sang FINAL mà không phải ghi lại;
- link không có ô thời tiết: `NO_MAPPING`; có ô nhưng thiếu bản ghi: `MISSING`.

Producer phát bản ghi mốc T **sớm nhất tại T** (`--weather-delay-min` ≥ 0), vì trước T giá trị mưa của giờ (T−1h, T] chưa quan sát được.
