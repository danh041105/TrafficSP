# Hợp đồng ngữ nghĩa Streaming — TrafficSP (Iceberg + Trino)

Hợp đồng chung cho Speed Layer, Batch Layer và Serving Layer (Trino). Mọi quy tắc nằm trong code dùng chung (`jobs/common/time_rules.py`, `quality_rules.py`, `aggregations.py`) và được **cả hai lớp gọi**. View Trino dùng đúng các ngưỡng đó, viết lại bằng SQL.

## 1. Thời gian

| Khái niệm | Định nghĩa |
|---|---|
| `data_as_of` | Chuỗi giờ **New York** của NYC DOT, không có offset. Giữ nguyên trong payload Kafka. |
| `event_time_utc` | `data_as_of` đổi sang UTC theo tzdata. **NULL** nếu giờ mơ hồ (lùi giờ, 2025-11-02 01:00–01:59) hoặc không tồn tại (tiến giờ, 2025-03-09 02:00–02:59). Mốc DST lấy từ `zoneinfo`, không viết cứng. |
| `kafka_ts` | Thời điểm message vào Kafka (đồng hồ thật). Chỉ dùng để đo độ trễ. |
| Đồng hồ sự kiện | `max(obs_event_time)` trên toàn mạng. Dùng cho trạng thái STALE. Không dùng giờ hệ thống, vì dữ liệu phát lại là năm 2025. |
| Gom nhóm | Theo `event_time_utc`. Cửa sổ `[start, start + 5')` căn theo epoch UTC; giờ `[H, H+1)` UTC. |

Bản ghi DST mơ hồ hoặc không tồn tại **không bị xoá âm thầm**:
- Speed: loại khỏi cửa sổ 5 phút và khỏi latest; được đếm trong `speed_batch_stats.jsonl` (`n_dst_ambiguous`, `n_dst_nonexistent`).
- Batch: không vào `silver.traffic_cleaned`, được ghi sang `silver.traffic_quarantine` (`reason = dst_ambiguous | dst_nonexistent | missing_key`).

## 2. Chất lượng

- Hợp lệ khi `0 < speed ≤ 100` (mph) **và** `travel_time > 0`.
- Lý do không hợp lệ, theo thứ tự ưu tiên: `null_value`, `speed_negative`, `speed_zero_tt_positive`, `speed_zero`, `speed_gt_max`, `travel_time_nonpositive`.
- Không sửa giá trị gốc; chỉ thêm `is_valid` và `invalid_reason`.
- `speed = 0` nghĩa là "có quan trắc, không có số đo". Thiếu bản ghi nghĩa là "không có quan trắc". Hệ thống không bao giờ suy ra speed = 0 từ việc thiếu dữ liệu (missing data).

## 3. Bảng, grain và thước đo

| Bảng Iceberg | Ai ghi | Grain | Ghi chú |
|---|---|---|---|
| `speed.traffic_5min_rt` | Speed | `(link_id, window_start_utc)` | Append-only. Mỗi cửa sổ phát đúng một lần khi watermark vượt `window_end_utc`. |
| `speed.traffic_latest` | Speed | 1 dòng / `link_id` | `MERGE INTO`; nhóm `valid_*` (bản hợp lệ mới nhất) và `obs_*` (quan trắc mới nhất). |
| `speed.weather_obs` | Speed | `(cell_id, time_utc)`, có thể lặp | Append; view lấy bản `kafka_offset` lớn nhất. |
| `gold.traffic_5min_batch` | Batch | `(link_id, window_start_utc)` | `overwrite` theo khoảng ngày UTC; Trino đọc qua tag `published`. |
| `gold.traffic_hourly` | Batch | `(link_id, hour_utc)` | Roll-up từ 5 phút; có `n_windows`. |
| `gold.link_baseline` | Batch | `link_id` | Free-flow speed của từng link (mục 6). |
| `gold.lambda_control` | Handoff | 1 dòng / lần đổi trạng thái | Append-only (xem `lambda_handoff.md`). |

Thước đo 5 phút và 1 giờ chỉ gồm các thành phần **cộng được**:
`n_records, n_valid, n_zero, n_invalid_other, sum_speed, sum_speed_sq, sum_travel_time`, cùng `min_speed`, `max_speed`.

- `avg_speed = sum_speed / n_valid` (NULL khi `n_valid = 0`).
- `quality_flag` = `NO_VALID_DATA` khi `n_valid = 0`, ngược lại là `OK`.
- Roll-up giờ chỉ cộng thành phần rồi tính lại trung bình. **Không** dùng `AVG(AVG(speed))`.

## 4. Khử trùng và tính idempotent

| Điểm | Cơ chế |
|---|---|
| Nguồn | Khoá nghiệp vụ `(link_id, data_as_of)`. Producer gửi lại, phát lại với `run_id` khác, hay bản sao skew đều cho cùng kết quả. |
| Speed 5 phút | `dropDuplicates([link_id, data_as_of, event_time_utc])` trong watermark. Không dùng `dropDuplicatesWithinWatermark` vì Spark 3.5.0 có lỗi SPARK-46676. |
| Sink 5 phút / weather | Sink Iceberg gốc ghi `spark.sql.streaming.queryId` và `epochId` vào summary của snapshot. Khi chạy lại một micro-batch đã commit (sau crash), sink bỏ qua epoch đó. |
| Sink latest | `MERGE INTO` chỉ cập nhật khi bản nguồn **mới hơn** theo `(event_time, kafka_offset)`, nên chạy lại cùng batch không thay đổi gì. Cùng link luôn rơi vào cùng partition Kafka (key = link_id) nên `kafka_offset` dùng được để phân xử hai bản trùng `event_time`. |
| Silver | Xoá rồi ghi lại theo ngày New York; khử trùng theo `(link_id, traffic_utc)`. |
| Gold | Một commit `overwrite(<khoảng ngày UTC>)` cho mỗi bảng. |

Bảo đảm: Spark xử lý **at-least-once**. Kết quả trên Iceberg là **exactly-once theo grain**. Đã kiểm chứng sau `kill -9`, sau khi lỡ chạy song song 2 ứng dụng cùng checkpoint, và khi compaction chạy song song: 0 dòng trùng grain, khớp Parquet gốc.

## 5. Dữ liệu muộn

| Lớp | Chính sách |
|---|---|
| Speed 5 phút | Watermark `SPEED_WATERMARK` (10 phút event-time). Bản muộn hơn bị loại và đếm ở `rows_dropped_by_watermark`. Cửa sổ đã phát không được cập nhật lại. |
| Speed latest | Không có watermark; bản muộn chỉ thắng khi mới hơn bản đang có. |
| Batch | Recompute **cả ngày UTC** từ Silver nên mọi bản đã vào Silver đều được tính. Muốn sửa ngày đã chốt: dùng `--correction` (xem `lambda_handoff.md`). |

## 6. Trạng thái giao thông (D-18)

- Free-flow là p85 của tốc độ **hợp lệ**, lúc khô (mưa < 0,1 mm và không có tuyết trong khoảng giờ), khung 22:00–05:59 giờ New York, cần ≥ 100 mẫu. Chỉ dùng lịch sử giai đoạn 1 (01–09/2025), không nhìn trước dữ liệu giai đoạn 2.
- `ratio = speed / free_flow`: `CONGESTED` khi < 0,5; `SLOW` khi < 0,8; còn lại `FREE_FLOW`.
- Trạng thái đặc biệt:
  - `NO_MAPPING`: link chưa gán ô thời tiết;
  - `NO_BASELINE`: chưa có free-flow;
  - `NO_VALID_DATA`: có bản ghi nhưng `n_valid = 0`, hoặc ở latest: quan trắc mới nhất không hợp lệ và mới hơn bản hợp lệ cuối;
  - `STALE`: chỉ ở latest, khi bản hợp lệ cuối cũ hơn đồng hồ sự kiện quá 15 phút;
  - `NO_DATA`: không có bản ghi nào.

## 7. Thời tiết theo khoảng giờ

Bản ghi Open-Meteo mốc `T` chứa nhiệt độ, độ ẩm, gió **tức thời tại T**, và mưa, tuyết, weather_code **của giờ (T−1h, T]**. Khoảng giao thông `[H, H+1)` ghép như sau (`v_weather_interval`):

- nhiệt độ, độ ẩm, gió, gió giật: lấy mốc **H**;
- mưa, tuyết, weather_code: lấy mốc **H+1** → `weather_status = FINAL`;
- chưa có mốc H+1 (giờ chưa kết thúc): tạm dùng giá trị của mốc H → `PROVISIONAL`. View tự chuyển sang FINAL khi mốc H+1 tới, không cần ghi lại;
- `MISSING`: link có ô thời tiết nhưng không có bản ghi; `NO_MAPPING`: link không có ô.

Nguồn thời tiết (`v_weather_hourly`) ưu tiên `silver.weather_cleaned` (đã làm sạch); nếu chưa có thì lấy bản realtime mới nhất trong `speed.weather_obs`.
