# Benchmark Speed Layer — TrafficSP

Mọi số liệu dưới đây là **đo thật** ngày 2026-11-11 trên nhánh `khai-streaming-speed-layer`. File kết quả gốc nằm ở `logs/bench/` (không commit).
Có thể đo lại bằng `scripts/benchmark/run_live.sh` (kịch bản live) và `scripts/benchmark/run_sweep.sh` (quét tham số).

## 1. Môi trường

| Hạng mục | Giá trị |
|---|---|
| Máy | Laptop Windows 10 Pro, Docker Desktop + WSL2, Docker dùng 7,6 GiB RAM |
| Spark | 3.5.0 standalone, 1 worker 4 core / 4 GiB. Driver chạy trên container `spark-master` (client mode) |
| Speed Layer | `spark.cores.max=2`, executor 1500m, trigger 5 s, watermark 10', `maxOffsetsPerTrigger` 100k, shuffle 8, RocksDB |
| Chạy song song | `stream_to_bronze --mode continuous` (2 core còn lại) |
| Kafka | KRaft 4.1, `traffic_raw` 4 partition, `weather_raw` 1 partition |
| Dữ liệu | GD2 NYC DOT tháng 11/2025, 108 link có dữ liệu (~35.464 bản ghi/ngày ≈ **0,41 msg/s** ở tốc độ thật) |

Cách đo:
- Số đo micro-batch lấy từ `StreamingQueryListener` → bảng `stream_progress`.
- Độ trễ end-to-end lấy từ bảng `speed_latency`: `done_at − kafka_ts`, với `done_at` là lúc ghi ClickHouse xong và `kafka_ts` là lúc message vào Kafka. p50 = trung vị các batch; p95 = phân vị 95 các batch của (done_at − kafka_ts_p05).
- CPU/RAM lấy từ `docker stats` mỗi ~3 giây (giá trị đỉnh).
- Độ trễ đo bằng **đồng hồ thật**; event-time là đồng hồ mô phỏng. Với `--speedup 1440`, 1 giây thật = 24 phút dữ liệu.

## 2. Kịch bản live (Speed Layer đang chạy, producer phát GD2)

| # | Kịch bản (event-time) | Producer | Micro-batch latest p50 / p95 | E2E latest p50 / p95 / max | E2E 5' p50 / p95 | State 5' max | Bị loại (watermark) |
|---|---|---|---|---|---|---|---|
| b1 | Baseline 1440×, 11-03 12:00 → 11-04 12:00 | 35.464 msg, 430 msg/s | 16,2 s / 47,9 s | 39,6 / 61,4 / 63,7 s | 44,6 s / 26,5 min ¹ | 39.060 dòng | 0 |
| b2 | Tốc độ tối đa (`--speedup 0`), 11-04 → 11-07 | 103.478 msg, **3.014 msg/s** | 27,1 s (2 batch, batch lớn nhất 99.998 = trần maxOffsets) | 34,9 / 41,6 / 42,5 s | 39,0 s / 17,8 min ¹ | 116.365 dòng | 0 |
| b3 | Burst: nền 1440×, mỗi 20 s có 5 s ở 14.400× | 35.039 msg, TB 904 msg/s | 6,2 s / 7,3 s | 9,3 / 13,2 / 13,4 s | 13,1 s / 16,3 s | 33.355 dòng / 50,9 MiB | 0 |
| b4 | Skew 3 link × 10 bản sao + 5% trùng | 45.647 msg (8.550 skew, 1.757 dup) | 5,8 s / 9,1 s | 9,9 / 17,0 / 17,4 s | 12,5 s / 96,5 s ¹ | 8.308 dòng / 60,3 MiB | 0 |
| b5 | Muộn: 60×, 5% bản ghi trễ tới 60' event-time | 8.928 msg, 453 bản muộn | 4,3 s / 6,1 s | 7,4 / 11,1 / 12,7 s | 26,7 s / 30,4 s | 932 dòng | **291** |
| b6 | Restart: 1440×, `kill -9` Speed Layer giây thứ 30 | 35.464 msg | — | — | — | — | 0 |

¹ Đuôi p95/max lớn của cửa sổ 5 phút **không phải** do xử lý chậm. Ở append mode, cửa sổ cuối của lần phát trước nằm chờ trong state cho tới khi có dữ liệu mới đẩy watermark vượt `window_end`. Khi kịch bản sau bắt đầu, các cửa sổ này mới được phát, và kafka_ts của chúng đã từ 18–27 phút trước (b1, b2) hoặc ~1,5 phút trước (b4: các cửa sổ cuối của b3 chờ hết thời gian drain 60 s).
Khi dữ liệu chảy liên tục (b3, b5), độ trễ 5 phút ≈ watermark + độ dài cửa sổ + trigger, quy đổi ra thời gian thật.
Ví dụ b5 ở 60×: 10' watermark + 5' cửa sổ = 15 s thật; cộng trigger và thời gian ghi được ~27 s.

**Restart (b6)**:
- `kill -9` lúc 17:09:06. Supervisor ghi `rc=137` lúc 17:09:07 và chạy lại lúc 17:09:22 (backoff 15 s). Micro-batch đầu tiên sau phục hồi ghi xong lúc 17:09:38, tức **32 s** từ lúc kill tới khi có dữ liệu mới, trong đó 15 s là backoff.
- Batch đang dở (`speed_latest` batch 133, 17.092 dòng) được chạy lại từ checkpoint. Tổng 17.092 + 18.372 = 35.464 = đúng số message.

**Tính đúng trong lúc benchmark** (`compare_5min.py`, đối chiếu độc lập với Parquet gốc):
- 11-03 12:00 → 11-09 12:00 (b1–b4: baseline, max rate, burst, skew/dup): 209.268/209.268 cửa sổ khớp tuyệt đối.
- 11-09 18:00 → 11-10 17:45 (b6, có `kill -9`): 35.092/35.092 cửa sổ khớp. Có 0 dòng vật lý trùng grain trong `traffic_5min_rt`, kể cả trước FINAL.
- 11-09 12:00 → 18:00 (b5): realtime thiếu đúng **291** cửa sổ, bằng số dòng bị loại vì watermark. Sau `lambda_handoff --until 2025-11-09T18:00`, batch bù đủ 291 cửa sổ đó. View hợp nhất từ 11-02 12:00 → 11-10 17:45 khớp 288.752/288.752 cửa sổ (chênh lệch sum_speed lớn nhất 1,4e-14).

**CPU / RAM đỉnh** (b1–b6):

| Container | CPU đỉnh | RAM đỉnh |
|---|---|---|
| `spark-master` (driver) | 270–545% | 2,0 GiB |
| `spark-worker-1` | 210–465% | 2,0 GiB |
| `kafka` | 220–280% | 1,0 GiB |
| `clickhouse` | 145–385% | 0,6 GiB |
| `producer` | ~110% (1 luồng Python) | 0,4 GiB |

## 3. Quét tham số (khối lượng cố định, cô lập)

Thiết lập:
- Workload: 240.000 message cuối của `traffic_raw` (60.000 / partition). Offset được chốt một lần cho mọi cấu hình: `{"0":253080,"1":221756,"2":284795,"3":294779}`.
- Chạy `--available-now --queries latest,5min`, Spark `local[2]`, checkpoint riêng, database ClickHouse riêng `traffic_bench`. Bảng và checkpoint đang chạy không bị đụng tới.
- Workload chứa cả lần phát lại tháng 10 (`run_id gd2-r2`) **sau** dữ liệu tháng 10 gốc, tức là có dữ liệu đến muộn thật.

| Cấu hình | Wall (s) | 5': batch | 5': rows/s | 5': p95 batch | 5': state max (dòng / MiB) | 5': bị loại | latest: rows/s |
|---|---|---|---|---|---|---|---|
| default (shuffle 8, maxOffsets 100k, wm 10') | 110 | 3 | 2.953 | 32,8 s | 194.630 / 31,7 | 0 | 2.869 |
| maxOffsets 20k | 135 | 12 | 2.316 | 24,9 s | 40.000 / 28,5 | **89.742** | 2.008 |
| shuffle 2 | 95 | 3 | 3.291 | 33,8 s | 194.630 / 29,3 | 0 | 3.231 |
| shuffle 32 | 97 | 3 | 3.321 | 30,3 s | 194.630 / 32,3 | 0 | 3.210 |
| watermark 2' | 85 | 3 | 3.659 | 28,8 s | 194.630 / 31,7 | 0 | 3.517 |
| watermark 30' | 82 | 3 | 3.874 | 25,0 s | 194.630 / 31,7 | 0 | 3.725 |

Nhận xét:
- **Mỗi cấu hình chỉ chạy một lần.** Chênh lệch thông lượng giữa default, shuffle và watermark (±15%) nằm trong mức nhiễu khởi động JVM / page cache: lần chạy đầu (default) chậm nhất, các lần sau nhanh dần. Không nên kết luận shuffle 2 hay 32 tốt hơn 8 với khối dữ liệu này (108 link/cửa sổ).
- **`maxOffsetsPerTrigger` quyết định ngữ nghĩa, không chỉ tốc độ.** Watermark chỉ tiến **giữa** các micro-batch. Batch 100k nuốt cả dữ liệu tháng 10 gốc lẫn bản phát lại trong cùng batch, nên không có gì bị loại và dropDuplicates gộp hết. Batch 20k làm watermark kịp tiến tới 10-03 trước khi bản phát lại tới, nên 89.742 dòng bị loại. Lần phát lại tháng 10 có 70.556 bản; khi chạy live, đúng 70.556 bản này bị loại. Với chúng, đây là hành vi mong muốn: kết quả phục vụ không đổi vì bản phát lại là bản trùng. Khoảng 19 nghìn dòng còn lại *có thể* đến từ lệch tiến độ giữa các partition: mỗi batch lấy đều theo offset từ 4 partition, nhưng các partition không ở cùng mốc event-time, trong khi watermark tính trên max toàn cục. Giả thuyết này chưa được phân tích chi tiết. Hệ quả là khi so sánh cấu hình phải dùng cùng `maxOffsetsPerTrigger`.
- Với batch rất lớn, watermark 2' / 10' / 30' cho kết quả như nhau. Ảnh hưởng của watermark chỉ thấy khi batch nhỏ so với độ trễ dữ liệu, như ở kịch bản b5: watermark 10' loại 291/453 bản muộn (trễ ngẫu nhiên đều 1 s – 60' event-time; lý thuyết ≈ 83% bản muộn vượt 10').
- Latest Reading không có state Spark nên không bị loại bản nào.

## 4. Đánh giá và giới hạn

- **Dư năng lực**: Speed Layer xử lý ổn định 2.300–3.900 rows/s/query (laptop, 2 core). Tốc độ dữ liệu thật là 0,41 msg/s, tức dư hơn **5.000 lần**. Toàn bộ GD2 (3,23 triệu bản ghi) ở tốc độ tối đa mất khoảng 15–25 phút.
- **Điểm nghẽn**:
  1. Phía driver: `foreachBatch` của latest chạy nhiều action (thống kê, lý do không hợp lệ, latest_any, latest, latency), rồi `toPandas` và HTTP insert. CPU `spark-master` đạt đỉnh 545%.
  2. Hai job streaming chia nhau 4 core của worker.
  Hướng cải thiện (chưa làm): gộp các action thống kê vào một lần `agg`; bật Arrow cho `toPandas`; thêm core/worker; tách Kafka→Bronze sang máy khác.
- **Chưa đo**:
  - Thay đổi số partition Kafka (cần topic mới, chưa tạo).
  - Thay đổi trigger interval (chỉ đo ở 5 s).
  - Soak test nhiều giờ.
  - Độ lặp lại thống kê: mỗi cấu hình chạy 1 lần.
