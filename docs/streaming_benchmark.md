# Benchmark Speed Layer + Serving — Spark → Iceberg → Trino

- Mọi số liệu là **đo thật** ngày 2026-11-13 trên nhánh local `khai-iceberg-trino`. Kết quả gốc nằm ở `logs/bench/` (không commit).
- **Không** dùng số liệu của bản ClickHouse cũ để kết luận cho Iceberg + Trino.
- Đo lại bằng:
  - `scripts/benchmark/run_live.sh` (kịch bản live);
  - `scripts/benchmark/run_sweep.sh` (quét tham số, cô lập);
  - `jobs/tests/benchmark/trino_queries.py` (truy vấn Trino).

## 1. Môi trường

| Hạng mục | Giá trị |
|---|---|
| Máy | Laptop Windows 10, 16 GB RAM, Docker Desktop + WSL2 (VM 7,6 GiB), page file 16–24 GB |
| Spark | 3.5.0 standalone, 1 worker 4 core / 4 GiB. Speed Layer: `cores.max=2`, executor 1500m, RocksDB state store, shuffle 8 |
| Iceberg | 1.10.1, JDBC catalog (PostgreSQL 17), HadoopFileIO trên volume Docker (dev, không MinIO) |
| Trino | 484, 1 node, `mem_limit` 2,5 GB |
| Kafka | 4.1 KRaft; `traffic_raw` 4 partition |
| Dữ liệu | GD2 NYC DOT tháng 12/2025, 124 link (~35.700 bản ghi/ngày ≈ **0,41 msg/s** ở tốc độ thật) |

Cách đo:
- **Micro-batch, lag, state, watermark**: lấy từ `StreamingQueryListener` (`speed_progress.jsonl`). `add_batch` của query 5 phút đã gồm thời gian ghi file và **commit Iceberg**.
- **E2E latest** = lúc MERGE xong − `kafka_ts`. p50 là trung vị các batch; p95 tính trên `kafka_ts_p05` của từng batch.
- **E2E 5 phút** = `$file_modified_time` của file Parquet − `kafka_ts_max` của cửa sổ (đọc qua Trino).
- Độ trễ đo bằng đồng hồ thật. Với `--speedup 1440`, 1 giây thật ứng với 24 phút dữ liệu, nên watermark 10' chỉ tương đương ~0,4 s thật.

## 2. Kịch bản live

| # | Kịch bản (trigger) | Producer | Trigger p50 / p95 (latest · 5') | MERGE p50 / p95 | E2E latest p50 / p95 / max | E2E 5' p50 / p95 | State 5' max | Bị loại (watermark) | Snapshot / commit |
|---|---|---|---|---|---|---|---|---|---|
| b1 | Baseline 1440× (10 s) | 35.712 msg, 340 msg/s | 8,2 / 9,1 s · 5,4 / 6,0 s | 2,6 / 3,9 s | 12,9 / 18,5 / 19,0 s | 20,0 / 24,9 s | 12.553 dòng / 10,5 MB | 0 | 8 / 7 / 7 |
| b2 | Tối đa (`--speedup 0`, 10 s) | 107.136 msg, **1.747 msg/s** | 6,6 / 17,9 s · 7,0 / 10,6 s | 4,0 / 11,9 s | 21,8 / 24,4 / 24,5 s | 19,6 / 22,3 s | 166.637 / 32,7 MB | 0 | 3 / 2 / 2 |
| b3 | Burst: 5 s ở 14.400× mỗi 20 s (10 s) | 35.712 msg | 7,1 / 10,8 s · 5,8 / 8,7 s | 4,2 / 7,4 s | 14,3 / 20,4 / 20,7 s | 24,1 / 27,5 s | 62.402 / 53,3 MB | 0 | 4 / 3 / 3 |
| b4 | Skew 3 link × 10 + 5% trùng (10 s) | 46.120 msg (8.640 skew, 1.768 dup) | 6,7 / 9,5 s · 4,0 / 6,1 s | 2,1 / 4,1 s | 10,4 / 17,7 / 18,2 s | 19,1 / 24,3 s | 12.554 / 66,0 MB | 0 | 8 / 7 / 7 |
| b5 | Muộn: 60×, 5% trễ tới 60' (10 s) | 8.928 msg, 453 bản muộn | 3,9 / 5,6 s · 3,1 / 4,3 s | 1,2 / 2,4 s | 9,5 / 14,5 / 19,8 s | 35,2 / 41,1 s | 1.114 dòng | **240** | 42 / 42 / 6 |
| b6 | Restart: `kill -9` giây thứ 30 (10 s) | 35.712 msg | — | — | — | — | — | 0 | — |
| b7 | Baseline 1440× (**30 s**) | 35.712 msg | 6,8 / 19,2 s · 5,2 / 13,2 s | 2,8 / 5,4 s | 24,5 / 35,3 / 36,8 s | 49,4 / 63,1 s | — | 0 | **4 / 3 / 3** |
| b8 | Baseline 1440× (**60 s**) | 35.712 msg | 7,0 / 24,2 s · 5,6 / 17,3 s | 3,5 / 6,5 s | 55,3 / 70,9 / 73,2 s | 90,4 / 112,3 s | — | 0 | **2 / 2 / 2** |

Cột cuối là số snapshot của `traffic_5min_rt / traffic_latest / weather_obs` trong lúc chạy kịch bản. Lag Kafka (`maxOffsetsBehindLatest`) lớn nhất = 0 ở mọi kịch bản.

Ghi chú:
- **E2E 5' max** có đuôi dài (132–761 s) ở các kịch bản ngay sau một lần phát trước. Nguyên nhân là cửa sổ cuối của lần phát trước nằm trong state chờ watermark (append mode), chỉ được phát khi dữ liệu mới tới. Đây không phải xử lý chậm.
- Khi dữ liệu chảy liên tục, độ trễ 5 phút ≈ trigger + thời gian watermark + 5' quy ra thời gian thật + commit.
- **b5**: 240 cửa sổ realtime bị thiếu vì bản ghi muộn hơn watermark. Sau `lambda_handoff`, batch bù đủ; view hợp nhất khớp Parquet gốc.
- **b6**: `kill -9` lúc 09:08:19. Supervisor ghi `rc=137` lúc 09:08:20, chạy lại lúc 09:08:35 (backoff 15 s). Micro-batch đầu tiên có dữ liệu xong lúc 09:08:57 cho latest và 09:09:02 cho 5 phút, tức **~38 s** sau khi kill. Cùng query ID nên đọc tiếp từ checkpoint.
- b7/b8 được đo lại (bản `_v2`) vì lần đầu lỡ chạy 2 supervisor cùng lúc (`CONCURRENT_STREAM_LOG_UPDATE`). Ngay cả lần lỗi đó dữ liệu vẫn đúng: 68.054/68.054 cửa sổ khớp, 0 trùng. Sau sự cố, supervisor đã có `flock`.

**Tính đúng** (`compare_5min.py`, đối chiếu độc lập bằng DuckDB với Parquet gốc, đọc qua Trino):

| Khoảng | Nội dung | Kết quả |
|---|---|---|
| 12-04 05:00 → 12-10 05:00 (b1–b4) | speed | 214.268 / 214.268 cửa sổ khớp tuyệt đối |
| 12-10 11:00 → 12-11 10:45 (b6, `kill -9`) | speed | 35.340 / 35.340 khớp |
| 12-04 05:00 → 12-16 10:45 | **view hợp nhất** sau 4 lượt Airflow | **435.093 / 435.093** khớp (đã gồm b5 được batch sửa) |
| 11-01 20:00 → 11-02 12:00 (có giờ DST mơ hồ) | hợp nhất | 20.708 / 20.708 khớp; 2.572 bản ghi `dst_ambiguous` ở quarantine |
| Thời tiết [H, H+1) | 35.712 cửa sổ ngày 12-10 | 0 sai nhiệt độ (mốc H), 0 sai mưa / weather_code (mốc H+1) |

**CPU / RAM đỉnh** (`docker stats` mỗi ~3 s):

| Container | CPU đỉnh | RAM đỉnh |
|---|---|---|
| `spark-master` (driver Speed Layer) | 166–654% | 1,05–1,24 GiB |
| `spark-worker-1` | 204–450% | 1,17–1,48 GiB |
| `kafka` | 163–258% | ~1,0 GiB |
| `trino` (lúc chỉ có view chạy) | 10–35% | 1,2–1,5 GiB |

## 3. Quét tham số (khối lượng cố định, cô lập)

- Workload: 240.000 message cuối của `traffic_raw`; offset được chốt một lần cho mọi cấu hình.
- Chạy `--available-now --queries latest,5min`, Spark `local[2]`. Mỗi cấu hình có namespace Iceberg riêng (`lakehouse.speed_bench_*`) và checkpoint riêng.

| Cấu hình | Wall | 5': rows/s | 5': trigger p95 | 5': add_batch p95 (gồm commit) | Bị loại | File dữ liệu 5' | KB / file |
|---|---|---|---|---|---|---|---|
| default (shuffle 8, maxOffsets 100k) | 71 s | 5.307 | 17,3 s | 17,0 s | 0 | 88 | 37,7 |
| maxOffsets 20k | 109 s | 3.156 | 8,1 s | 7,5 s | **23.548** | 160 | 22,8 |
| **shuffle 2** | **61 s** | **5.708** | 19,7 s | 18,6 s | 0 | **22** | **101,9** |

Nhận xét:
- **`spark.sql.shuffle.partitions` quyết định số file mỗi commit** của bảng 5 phút: mỗi phân vùng shuffle ghi 1 file cho mỗi ngày. Với 124 link, giảm 8 → 2 giảm số file 4 lần, file to hơn gần 3 lần, và chạy nhanh hơn. **Đề xuất `SPEED_SHUFFLE_PARTITIONS=2`** cho workload này.
  - Lưu ý: số phân vùng state được ghi cứng vào checkpoint. Đổi giá trị chỉ có tác dụng với checkpoint mới.
- **`maxOffsetsPerTrigger` đổi cả ngữ nghĩa**: batch nhỏ làm watermark tiến giữa các batch nên dữ liệu phát lại / lệch partition bị loại nhiều hơn (23.548 dòng). Batch Layer sửa phần này, nhưng khi so sánh cấu hình phải giữ cùng giá trị.
- Listener chỉ ghi nhận 2/3 micro-batch của query latest khi `availableNow` dừng (batch cuối kết thúc sau khi ứng dụng đóng). Vì vậy rows/s của latest trong file `sweep_summary.tsv` hơi thấp hơn thực tế.

## 4. Snapshot và file nhỏ

Mỗi micro-batch có dữ liệu sinh ra **1 snapshot cho mỗi bảng** và (shuffle × số ngày) file Parquet.

| Thời điểm | `traffic_5min_rt` | Ghi chú |
|---|---|---|
| Sau b1–b8 + Airflow (~4 giờ chạy) | 753 file, trung bình **13,3 KB**, 104 snapshot | 452.709 cửa sổ |
| Sau `iceberg_maintenance.py --compact` | **15 file, 274,9 KB** | Cùng 452.709 cửa sổ; Speed Layer vẫn ghi song song, không lỗi, không phải chạy lại |

| Truy vấn Trino (median 5 lần) | Trước compaction | Sau compaction |
|---|---|---|
| Quét toàn bộ `speed.traffic_5min_rt` | 1.034 ms, 758 split | **272 ms, 20 split** |
| Đọc metadata `$files` | 399 ms, 94 split | 165 ms, 10 split |
| `v_traffic_5min_unified`, 1 ngày (phần batch) | 937 ms | 762 ms |

Ước lượng khi chạy liên tục với trigger 30 s và shuffle 8: tối đa 2.880 commit/ngày/bảng, nhưng chỉ batch có dữ liệu mới mới commit. Nguồn cập nhật ~5 phút/lần nên thực tế khoảng 288 commit/ngày, tức khoảng 2.300 (288 × 8) tới 23.000 (2.880 × 8) file nhỏ mỗi ngày cho bảng 5 phút.

**Đề xuất:**
1. Compaction `speed.*` mỗi giờ (an toàn, chạy song song stream được).
2. `expire_snapshots` mỗi ngày, giữ 72 giờ và tối thiểu 100 snapshot. Bước này xoá file nên **chưa chạy**, chờ chấp thuận.
3. Shuffle 2 cho checkpoint mới.
4. Trigger 30 s.

## 5. Trigger: độ trễ đổi lấy số commit

| Trigger | E2E latest p50 | E2E 5' p50 | Snapshot / ngày dữ liệu (1440×) |
|---|---|---|---|
| 10 s | 12,9 s | 20,0 s | 8 / 7 / 7 |
| 30 s | 24,5 s | 49,4 s | 4 / 3 / 3 |
| 60 s | 55,3 s | 90,4 s | 2 / 2 / 2 |

Ở tốc độ thật (0,41 msg/s, nguồn cập nhật ~5 phút/lần), trigger 30 s cho latest trễ khoảng < 1 phút, nhanh hơn nhiều so với chu kỳ cập nhật của nguồn. Vì vậy **mặc định là 30 s**.

## 6. Trino trên Serving Layer

Đo bằng `trino_queries.py`, 1 lần làm nóng + 5 lần đo, trước compaction.

| Truy vấn | Median | Max | Dữ liệu quét | Split |
|---|---|---|---|---|
| `v_traffic_latest` (124 link) | **187 ms** | 191 ms | 0,13 MB | 46 |
| `v_lambda_status` | 575–644 ms | 976 ms | 9,1 MB | 759–784 |
| `v_traffic_5min_unified`, 1 link × 1 ngày | 348 ms | 418 ms | 0,69 MB | 91 |
| `v_traffic_5min_unified`, toàn mạng × 1 ngày | 423 ms | 516 ms | 0,69 MB | 95 |
| `v_traffic_5min_enriched`, borough × trạng thái × 1 ngày | 1.733 ms | 3.917 ms | 2,9 MB | 283 |
| `v_traffic_hourly_unified`, 1 tháng OLAP | 1.133 ms | 1.244 ms | 11,2 MB | 844 |
| `speed.traffic_5min_rt`, lọc 1 ngày | 89 ms | 101 ms | ~0 MB (pruning) | 53 |
| `speed.traffic_5min_rt`, quét toàn bộ | 534 ms | 572 ms | 9,2 MB | 726 |

- `EXPLAIN (TYPE IO)` xác nhận **predicate pushdown / partition pruning** theo `window_start_utc` trên cả `traffic_5min_batch` (đọc qua tag) và `traffic_5min_rt`.
- Điều kiện `>= (SELECT t_batch ...)` là subquery vô hướng nên chỉ được lọc lúc chạy (dynamic filtering), không pruning lúc lập kế hoạch. Đó là lý do `v_lambda_status` vẫn quét ~9 MB. Truy vấn có điều kiện thời gian cụ thể thì prune tốt.
- **Trino không đạt độ trễ dưới 100 ms như ClickHouse** cho truy vấn qua view (0,2–1,7 s). Truy vấn nặng nhất (`enriched`, hourly theo tháng) nên materialize thành bảng Gold bằng Spark nếu dashboard cần dưới 1 giây.

## 7. Chưa đo

- Thay đổi số partition Kafka (cần topic mới).
- Soak test nhiều giờ ở tốc độ thật.
- Độ trễ commit trên MinIO/S3 (môi trường này dùng file system local).
- Mỗi cấu hình quét chỉ chạy 1 lần, chưa có khoảng tin cậy.
