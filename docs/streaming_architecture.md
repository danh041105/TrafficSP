# Kiến trúc Streaming / Lambda — TrafficSP

Tài liệu mô tả kiến trúc **đã chạy thật** trên nhánh `khai-streaming-speed-layer` (Docker Desktop + WSL2, 1 máy).
Speed Layer và Batch Layer đều chạy trên **Spark 3.5.0**. Hệ thống **không dùng Apache Flink**. Lớp phục vụ là **ClickHouse**; Redis không dùng.

## 1. Tổng quan

```
                          ┌──────────────── Batch Layer (Spark batch) ────────────────┐
 data/raw (Parquet NYC DOT│ Bronze (Iceberg) → Silver traffic_cleaned / quarantine     │
 + JSON Open-Meteo)       │      → batch_views.py (5 phút + 1 giờ) → Gold Iceberg       │
        │                 │      → ClickHouse traffic_5min_batch / traffic_hourly       │
        ▼                 │      (REPLACE PARTITION theo ngày UTC qua bảng *_staging)   │
 replay_producer.py       └──────────────────────────▲─────────────────────────────────┘
  (GD2: đồng hồ mô phỏng           stream_to_bronze  │           lambda_handoff.py
   event-time 2025)                 (Kafka → Bronze) │      (recompute → kiểm tra → publish T_batch)
        │                                            │                    │
        ▼                                            │                    ▼
   Kafka (KRaft) ──── traffic_raw (4 partition) ─────┤            traffic.lambda_handoff
                └──── weather_raw (1 partition) ─────┤
                                                     ▼
                     ┌──────────── Speed Layer (Spark Structured Streaming) ────────────┐
                     │ speed_latest : parse → event_time UTC/DST → chất lượng            │
                     │               → traffic_latest, traffic_latest_any (ReplacingMT)  │
                     │ speed_5min   : watermark 10' → dropDuplicates → window 5' (append)│
                     │               → traffic_5min_rt                                    │
                     │ speed_weather: → weather_hourly                                    │
                     └───────────────────────────────────────────────────────────────────┘
                                                     │
                                                     ▼
          ClickHouse `traffic` (Serving/Query Layer): view đọc qua FINAL
            v_traffic_latest · v_traffic_5min_unified · v_traffic_5min_enriched
            v_traffic_hourly_unified · v_weather_interval · v_lambda_t_batch
```

## 2. Thành phần và file

| Thành phần | File | Ghi chú |
|---|---|---|
| Producer GD2 | `scripts/replay_producer.py` | Phát tháng 10–12/2025 theo **đồng hồ event-time mô phỏng** (`--speedup`), không theo giờ hệ thống 2026. Có `--start/--end/--months`, `--burst-*`, `--skew-*`, `--ooo-*`, `--dup-fraction`, `--seed`, `--summary-file`. Producer idempotent (`enable.idempotence`). |
| Kafka → Bronze | `jobs/stream/stream_to_bronze.py` (có sẵn trên dev) | Dùng lại nguyên trạng. |
| Speed Layer | `jobs/stream/speed_layer.py` | 3 query độc lập, mỗi query một checkpoint, sink `foreachBatch` → ClickHouse idempotent. |
| Supervisor | `jobs/stream/run_speed_layer.sh` | Tự chạy lại khi ứng dụng thoát (vd ClickHouse ngừng quá ngân sách retry). |
| Quy tắc dùng chung | `jobs/common/time_rules.py`, `quality_rules.py`, `aggregations.py` | Batch và stream gọi **cùng** hàm → kết quả hai lớp so khớp được từng cửa sổ. |
| Ghi ClickHouse | `jobs/common/clickhouse_io.py` | `insert_deduplication_token`, retry + backoff (`CLICKHOUSE_RETRIES`, mặc định 8 ≈ 2 phút). |
| DDL Serving | `jobs/ddl/clickhouse_serving.sql`, `clickhouse_views.sql`, `clickhouse_serving.py` | Idempotent (`IF NOT EXISTS` / `CREATE OR REPLACE VIEW`). `CLICKHOUSE_SERVING_DB` cho phép tạo bộ bảng riêng (vd `traffic_bench`). |
| Silver | `jobs/batch/transformation/bronze_to_silver.py` | Bổ sung `silver.traffic_quarantine` (bản ghi giờ DST/thiếu khóa không bị xoá âm thầm). |
| Batch View | `jobs/batch/gold/batch_views.py` | 5 phút + 1 giờ; Iceberg `overwritePartitions` + ClickHouse `REPLACE PARTITION` theo ngày UTC. |
| Dimension phục vụ | `jobs/batch/gold/serving_dims.py` | `link_dim` (borough, cell, free-flow p85) + backfill `weather_hourly`. |
| Lambda handoff | `jobs/batch/gold/lambda_handoff.py` | Chốt T_batch ở ranh giới 5 phút. |
| Kiểm thử | `jobs/tests/test_streaming_rules.py`, `jobs/tests/integration/compare_5min.py` | Unit test + đối chiếu độc lập bằng DuckDB với Parquet gốc. |
| Benchmark | `jobs/tests/benchmark/summarize.py`, `scripts/benchmark/run_live.sh`, `run_sweep.sh` | Xem `streaming_benchmark.md`. |
| Môi trường dev | `docker-compose.dev.yml`, `conf/spark-defaults.dev.conf`, `.gitattributes` | Không có MinIO: warehouse/checkpoint trên volume `traffic_lakehouse_dev`. |

## 3. Speed Layer chi tiết

### 3.1 `speed_latest` (Latest Reading)
- Kafka `traffic_raw` → `from_json` → `with_event_time` (giờ New York → UTC; giờ DST mơ hồ/không tồn tại → `event_time_utc = NULL` + cờ) → `with_quality`.
- Không có state trong Spark. Mỗi micro-batch lấy bản có `event_time` lớn nhất theo link (`max_by`) rồi ghi:
  - `traffic_latest`: chỉ bản hợp lệ. Bảng dùng `ReplacingMergeTree(event_time)` nên bản cũ đến muộn **không ghi đè** bản mới.
  - `traffic_latest_any`: bản bất kỳ, kể cả không hợp lệ, để phân biệt "có quan trắc nhưng không hợp lệ" với "không có quan trắc".
- Thống kê mỗi batch được ghi vào `speed_batch_stats`: số dòng, số hợp lệ, số DST mơ hồ/không tồn tại, lý do không hợp lệ, khoảng event-time.

### 3.2 `speed_5min` (Aggregate 5 phút)
- `withWatermark(event_time_utc, 10 minutes)` → `dropDuplicates([link_id, data_as_of, event_time_utc])` → tumbling `window(5 minutes)` × `link_id`, output mode **append**.
- Mỗi cửa sổ được phát **đúng 1 lần**, khi watermark vượt `window_end`. Bản ghi đến muộn hơn watermark bị loại; số lượng hiện ở `numRowsDroppedByWatermark` (bảng `stream_progress`). Batch Layer sẽ sửa các cửa sổ này.
- Không dùng `dropDuplicatesWithinWatermark` vì Spark 3.5.0 lỗi `None.get` (SPARK-46676, đã sửa ở 3.5.1). Khoá khử trùng có sẵn cột event-time nên state vẫn được dọn theo watermark.
- Chỉ lưu thành phần cộng được (`n_records, n_valid, n_zero, n_invalid_other, sum_speed, sum_speed_sq, sum_travel_time, min, max`). Mọi trung bình đều tính lại từ tổng, không có `AVG(AVG)`.
- State store: RocksDB (`SPEED_STATE_STORE=rocksdb`).

### 3.3 `speed_weather`
- Kafka `weather_raw` → `weather_hourly` (`ReplacingMergeTree(updated_at)`). Ghép thời tiết theo khoảng giờ làm ở Query Layer (`v_weather_interval`).

### 3.4 Quan sát
- `StreamingQueryListener` ghi `stream_progress` (rows/s, thời lượng trigger, watermark, state rows/bytes, rows dropped, offsets behind) và tuỳ chọn file JSONL.
- `speed_latency`: phân vị `kafka_ts` của từng micro-batch và thời điểm ghi xong, dùng để đo độ trễ end-to-end.

## 4. Serving (ClickHouse)

| Bảng | Engine / khoá | Nguồn |
|---|---|---|
| `traffic_latest`, `traffic_latest_any` | ReplacingMergeTree(event_time), ORDER BY link_id | speed_latest |
| `traffic_5min_rt` | ReplacingMergeTree(updated_at), PARTITION BY ngày UTC, ORDER BY (link_id, window_start) | speed_5min |
| `traffic_5min_batch`, `traffic_hourly` (+ `_staging`) | ReplacingMergeTree(computed_at), PARTITION BY ngày UTC | batch_views |
| `weather_hourly` | ReplacingMergeTree(updated_at), ORDER BY (cell_id, time_utc) | speed_weather + backfill batch |
| `link_dim` | ReplacingMergeTree(updated_at) | serving_dims |
| `lambda_handoff` | MergeTree (log PUBLISHED/REJECTED) | lambda_handoff |
| `stream_progress`, `speed_batch_stats`, `speed_latency`, `batch_run_stats` | log vận hành | listener / sink / batch |

Mọi view đọc qua `FINAL`, không dựa vào merge nền để khử trùng. Các view chính:
- `v_traffic_latest`: trạng thái từng link. Đồng hồ sự kiện là `max(event_time)` toàn mạng (không dùng giờ hệ thống), dùng để xác định `STALE` (> `STALE_AFTER_MINUTES` = 15).
- `v_traffic_5min_unified`: lấy từ batch khi `window_start < T_batch` và từ speed khi `window_start ≥ T_batch`.
- `v_traffic_5min_enriched`: thêm borough, `traffic_state` theo free-flow và thời tiết theo khoảng giờ.
- `v_traffic_hourly_unified`: roll-up 1 giờ từ grain 5 phút hợp nhất.

## 5. Lambda handoff

`lambda_handoff.py` thực hiện các bước sau, mỗi bước chỉ chạy khi bước trước thành công:
1. Recompute Batch View cho các ngày UTC trong `[T_old, T_new)`.
2. Commit: Iceberg `overwritePartitions`, rồi ClickHouse ghi `*_staging`, kiểm tra số dòng và `REPLACE PARTITION`. Ngày không có dữ liệu Silver thì `DROP PARTITION` (ClickHouse mới từ chối REPLACE từ nguồn rỗng).
3. Kiểm tra trên `[T_old, T_new)`:
   - `complete`: tổng `n_records` của batch = số dòng Silver;
   - `unique_grain`: không trùng (link, window);
   - `covers_realtime`: mọi cửa sổ realtime đều có ở batch;
   - báo cáo thêm chênh lệch với realtime.
4. Publish: chèn 1 dòng `PUBLISHED` (một INSERT nên nguyên tử). Nếu kiểm tra không đạt thì ghi `REJECTED`, thoát với mã 1 và giữ nguyên T_batch.
5. Tuỳ chọn `--cleanup-rt-keep-days N`: dọn phân vùng realtime cũ. Mặc định **tắt** (không xoá dữ liệu).

Nếu tiến trình chết giữa chừng, T_batch chưa đổi nên view hợp nhất vẫn lấy realtime; chỉ cần chạy lại vì mọi bước đều idempotent (đã kiểm chứng bằng `--fail-at after_commit`).

### ClickHouse và Trino

Nhánh `dev` (commit `e782e9f`, `631f81e`) bổ sung **Trino 484** và CloudBeaver để truy vấn lakehouse Iceberg. Hai hệ thống chạy song song, mỗi bên một vai trò:

| | Trino | ClickHouse |
|---|---|---|
| Vai trò | Truy vấn ad-hoc / phân tích trên Iceberg (Bronze, Silver, Gold) | Serving tier của Lambda: dữ liệu realtime + view hợp nhất |
| Dữ liệu | Toàn bộ lịch sử trên Iceberg | `traffic_latest`, `traffic_5min_rt`, `traffic_5min_batch`, `traffic_hourly`, `lambda_handoff`, ... |
| Lý do | Đọc trực tiếp lakehouse, không cần chép dữ liệu | Ghi idempotent từng micro-batch (ReplacingMergeTree + dedup token), đọc độ trễ thấp |

Speed Layer không ghi qua Trino. Lý do: Iceberg không có engine thay-thế-theo-khoá như ReplacingMergeTree, nên mỗi micro-batch sẽ phải MERGE/commit snapshot riêng, tạo nhiều file nhỏ và tăng độ trễ.

## 6. Quyết định kỹ thuật chính

| Quyết định | Lý do |
|---|---|
| Spark Structured Streaming cho Speed Layer | Theo yêu cầu (không dùng Flink); dùng chung code quy tắc với batch. |
| 3 query, 3 checkpoint | Lỗi ở một query không làm hỏng offset/state của query khác; có thể khởi động lại riêng. |
| `foreachBatch` + `insert_deduplication_token` + ReplacingMergeTree | Exactly-once **hiệu quả** ở grain phục vụ: chạy lại micro-batch không nhân đôi dữ liệu. |
| Append mode cho 5 phút | Spark 3.5 chỉ hỗ trợ 2 toán tử stateful nối tiếp ở append; mỗi cửa sổ phát đúng 1 lần nên đơn giản hoá đối chiếu. |
| Watermark 10 phút | Dữ liệu NYC DOT cập nhật khoảng 5 phút/lần. Watermark 10 phút cân bằng giữa độ trễ phát cửa sổ và tỷ lệ bị loại; phần muộn hơn do Batch sửa. Xem phần quét tham số trong benchmark. |
| T_batch ở ranh giới 5 phút, ngày UTC làm đơn vị recompute | Khớp phân vùng Iceberg/ClickHouse; REPLACE PARTITION là nguyên tử theo ngày. |
| Giờ DST mơ hồ → không gán UTC, quarantine | Không tự đoán offset (quy tắc D-03/D-04 nhánh khai); bản ghi được đếm, không bị mất âm thầm. |
| ClickHouse làm Serving, không dùng Redis | Latest reading 108–124 link phục vụ tốt bằng ReplacingMergeTree + FINAL, không cần thêm hệ thống. |
| Dev không có MinIO | Image MinIO cộng đồng không còn phát hành (Docker Hub/quay trả 401). Iceberg dùng HadoopFileIO trên volume chung; production vẫn cấu hình s3a như cũ. |
