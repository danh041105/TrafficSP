# Kiến trúc Streaming / Lambda — TrafficSP (Kafka + Spark + Iceberg + Trino)

Kiến trúc **đã chạy thật** trên nhánh local `khai-iceberg-trino` (tạo từ `origin/dev` 8afef89), Docker Desktop + WSL2, 1 máy.

- **Không dùng** Apache Flink, ClickHouse hay Redis.
- Speed Layer và Batch Layer đều chạy **Spark 3.5.0**, lưu bảng bằng **Apache Iceberg 1.10.1** (catalog JDBC trên PostgreSQL), truy vấn qua **Trino 484**, điều phối bằng **Airflow 3.3.2**.

## 1. Sơ đồ

```
NYC DOT + Open-Meteo (data/raw)
            │
   scripts/replay_producer.py   (GD2: đồng hồ event-time mô phỏng 2025, --speedup)
            │
          Kafka  traffic_raw (4 partition, key = link_id) · weather_raw (1 partition)
            │
     ┌──────┴───────────────────────────────┐
     ▼                                      ▼
 BATCH LAYER (Airflow, mỗi giờ)          SPEED LAYER (chạy liên tục, run_speed_layer.sh)
 kafka_to_bronze (availableNow)          speed_latest : foreachBatch → MERGE INTO speed.traffic_latest
 bronze_to_silver (+ quarantine DST)     speed_5min   : watermark 10' → dropDuplicates → window 5'
 batch_views: gold.traffic_5min_batch                    → Iceberg sink → speed.traffic_5min_rt
              gold.traffic_hourly        speed_weather: Iceberg sink → speed.weather_obs
 dq_checks
 lambda_handoff → gold.lambda_control + tag 'published'
     │                                      │
     └──────────────► Iceberg (JDBC catalog trên PostgreSQL; MinIO hoặc volume dev) ◄──┘
                                   │
                                 Trino  (catalog lakehouse, chỉ đọc dữ liệu; chỉ tạo VIEW trong serving)
                                   │
     serving.v_traffic_latest · v_traffic_5min_unified · v_traffic_5min_enriched
     v_traffic_hourly_unified · v_lambda_status · v_weather_interval · v_link_dim
                                   │
                    Grafana (trino-datasource) / CloudBeaver
```

## 2. Thành phần và file

| Thành phần | File | Ghi chú |
|---|---|---|
| Producer GD2 | `scripts/replay_producer.py` | Đồng hồ event-time mô phỏng; `--start/--end/--months`, burst / skew / out-of-order / dup có seed; Kafka idempotent. |
| Kafka → Bronze | `jobs/batch/ingestion/kafka_to_bronze.py` (dev) | Giữ nguyên code của nhóm: Structured Streaming `availableNow`, chạy theo đợt. |
| Bronze → Silver | `jobs/batch/transformation/bronze_to_silver.py` (dev) | Bổ sung `silver.traffic_quarantine`. |
| Batch View | `jobs/batch/gold/batch_views.py` | 5 phút + 1 giờ, mỗi bảng 1 commit `overwrite(khoảng ngày UTC)`. |
| Free-flow | `jobs/batch/gold/serving_dims.py` | Ghi `gold.link_baseline`. |
| Kiểm tra chất lượng | `jobs/batch/gold/dq_checks.py` | 6 kiểm tra bắt buộc trước handoff. |
| Lambda handoff | `jobs/batch/gold/lambda_handoff.py`, `jobs/common/lambda_control.py` | Xem `lambda_handoff.md`. |
| Kiểm chứng Serving | `jobs/batch/gold/verify_serving.py`, `jobs/common/trino_http.py` | REST Trino, chỉ dùng thư viện chuẩn. |
| Lập kế hoạch | `jobs/batch/gold/plan_batch.py` | Khoảng xử lý tính từ dữ liệu, không từ ngày logic của Airflow. |
| Speed Layer | `jobs/stream/speed_layer.py` | 3 query, 3 checkpoint `speed_v2_*`. |
| Supervisor | `jobs/stream/run_speed_layer.sh`, `stop_speed_layer.sh` | `flock`: chỉ một instance; tự chạy lại khi lỗi. |
| DDL | `jobs/ddl/speed.py`, `jobs/ddl/gold.py` | Idempotent; tiến hoá schema, không DROP; tạo tag `published`. |
| View Trino | `jobs/serving/trino_views.sql` | `CREATE OR REPLACE VIEW ... SECURITY INVOKER`. |
| Phân quyền Trino | `conf/trino/access/lakehouse-rules.json` | Đọc mọi bảng; chỉ sở hữu `serving.v_*`. |
| Điều phối | `airflow/dags/trafficsp_lambda_batch.py` | Xem mục 5. |
| Bảo trì | `jobs/batch/maintenance/iceberg_maintenance.py` | Compaction (an toàn); expire snapshot phải xác nhận rõ. |
| Môi trường dev | `docker-compose.dev.yml`, `conf/spark-defaults.dev.conf`, `conf/trino/catalog-dev/` | Không có MinIO; warehouse nằm trên volume `traffic_lakehouse_dev`. |

## 3. Speed Layer

| Query | Nguồn → đích | Sink | Bảo đảm |
|---|---|---|---|
| `speed_5min` | `traffic_raw` → `speed.traffic_5min_rt` | Iceberg sink gốc, append, `fanout-enabled` | Mỗi cửa sổ phát một lần; chạy lại epoch đã commit thì bị bỏ qua. |
| `speed_latest` | `traffic_raw` → `speed.traffic_latest` | `foreachBatch` + `MERGE INTO` (copy-on-write) | Chỉ ghi khi bản mới hơn; chạy lại là no-op. |
| `speed_weather` | `weather_raw` → `speed.weather_obs` | Iceberg sink gốc, append | Như trên. |

**Latest Reading — vì sao chọn `MERGE INTO`** thay cho "log append-only + view chọn bản mới nhất":

| Tiêu chí | MERGE INTO (đã chọn) | Append log + view |
|---|---|---|
| Kích thước | Luôn ~124 dòng: copy-on-write ghi lại 1 file nhỏ, MERGE mất 1–4 s mỗi batch | Tăng ~35 nghìn dòng/ngày; cần retention + compaction |
| Truy vấn | Đọc 124 dòng (~190 ms) | Window function trên toàn log; càng lâu càng chậm |
| Idempotent | Điều kiện `(event_time, kafka_offset)` mới hơn | Cần quy tắc chọn bản khi trùng event_time |
| Xung đột commit | Chỉ một writer (`speed_latest`); compaction không đụng bảng này. Iceberg tự retry (`commit.retry.num-retries = 10`) | Không có |

Workload hiện tại có ít link và tốc độ ghi thấp nên MERGE đơn giản, nhanh hơn và không cần dọn dữ liệu.

**Quan sát:**
- `StreamingQueryListener` ghi `speed_progress.jsonl`: rows/s, thời lượng trigger, add_batch, watermark, state, số dòng bị loại vì watermark, lag Kafka.
- Sink latest ghi `speed_batch_stats.jsonl` (chất lượng, DST) và `speed_latency.jsonl` (kafka_ts, thời gian MERGE).
- Các số đo vận hành **không** ghi vào Iceberg, để không sinh thêm commit nhỏ.

## 4. Serving Layer (Trino)

| View | Nội dung |
|---|---|
| `v_lambda_t_batch`, `v_lambda_status` | T_batch, snapshot đã công bố so với tag, trạng thái handoff cuối, Speed đang đi trước Batch bao nhiêu phút |
| `v_traffic_5min_unified` | batch (tag `published`) cho `< T_batch`, speed cho `≥ T_batch` |
| `v_traffic_5min_enriched` | + borough, free-flow, `traffic_state`, thời tiết theo khoảng giờ, `weather_status` |
| `v_traffic_hourly_unified` | giờ trọn trước T_batch lấy từ `gold.traffic_hourly`; từ giờ chứa T_batch trở đi cộng thành phần từ 5 phút hợp nhất |
| `v_traffic_latest` | 1 dòng / link (124 link): trạng thái, bản hợp lệ và quan trắc cuối, STALE theo đồng hồ sự kiện |
| `v_link_dim`, `v_weather_hourly`, `v_weather_interval` | Dimension và thời tiết (Silver ưu tiên, realtime bù phần chưa có) |

- Iceberg 1.10 với JDBC catalog **hỗ trợ view** (đã kiểm chứng trên Trino 484). View được lưu trong catalog nên còn nguyên khi Trino khởi động lại.
- Phân quyền: Trino chỉ đọc dữ liệu (`INSERT`/`DELETE` vào bảng bị từ chối), chỉ tạo/thay được view `serving.v_*`, không tạo được bảng.

## 5. Airflow

```
kafka_to_bronze → plan → bronze_to_silver → [batch_5min, batch_hourly] → dq_checks → lambda_handoff → verify_serving
```

- Lịch `@hourly`, `max_active_runs = 1`, `retries = 2` với backoff (riêng `dq_checks` không retry).
- `plan` trả về exit 99 khi Bronze rỗng; Airflow coi đó là SKIPPED.
- Speed Layer **không** nằm trong DAG nên không bị khởi động lại theo lịch Batch.
- `lambda_handoff` chỉ chạy khi `dq_checks` thành công, và tự kiểm tra lại trên đúng snapshot trước khi công bố.
- Đã chạy thật 4 lượt (2 theo lịch, 2 thủ công): tất cả task `success`. T_batch đi từ 2025-12-02 10:00 → 2025-12-15 10:45 → 2025-12-16 10:45.

## 6. Quyết định kỹ thuật

| Quyết định | Lý do |
|---|---|
| Iceberg sink gốc cho 5 phút / weather | Idempotent theo epoch có sẵn, không phải tự viết; một commit mỗi micro-batch. |
| `MERGE INTO` cho latest | Xem mục 3. |
| Batch đọc qua tag `published` | Iceberg không có giao dịch nhiều bảng. Tag là cách công bố nguyên tử cho từng bảng; bản đã ghi nhưng chưa kiểm chứng không bao giờ lộ ra. |
| `lambda_control` append-only | Không UPDATE/DELETE nên không có xung đột commit; còn lịch sử đầy đủ để phục hồi. |
| PostgreSQL advisory lock | Dùng chính Postgres của catalog; khoá tự nhả khi tiến trình chết. |
| Trigger mặc định 30 s | Cân bằng giữa độ trễ và số snapshot / file nhỏ (số liệu trong `streaming_benchmark.md`). |
| Watermark 10 phút | NYC DOT cập nhật khoảng 5 phút/lần; phần muộn hơn do Batch sửa. |
| Checkpoint mới `speed_v2_*` | Không dùng lại checkpoint của bản sink ClickHouse cũ. Checkpoint cũ vẫn được giữ, không xoá. |
| Trino catalog dev dùng `fs.local.enabled` + `local.location=/` | Đọc trực tiếp đường dẫn `file:/opt/lakehouse/...` mà Spark (HadoopFileIO) đã ghi. |

## 7. Thành phần ClickHouse đã thay thế

| Trước (nhánh `khai-streaming-speed-layer`) | Sau |
|---|---|
| `common/clickhouse_io.py`, `clickhouse_connect` | Bỏ. Sink Iceberg gốc + `MERGE INTO`; `common/trino_http.py` để đọc. |
| `ddl/clickhouse_serving.{py,sql}`, `clickhouse_views.sql` | `ddl/speed.py`, `ddl/gold.py`, `serving/trino_views.sql` |
| ReplacingMergeTree + FINAL | Grain duy nhất nhờ idempotent theo epoch + MERGE có điều kiện |
| Bảng `lambda_handoff` + `REPLACE PARTITION` | `gold.lambda_control` + `overwrite` theo khoảng + tag `published` |
| `stream_progress`, `speed_latency`, `speed_batch_stats` (bảng ClickHouse) | File JSONL ở `OPS_LOG_DIR` |
| Service `clickhouse`, biến `CLICKHOUSE_*`, `pip clickhouse-connect` | Đã gỡ khỏi compose, config, Dockerfile và README. Container cũ chỉ dừng, volume `traffic_clickhouse_data` vẫn giữ. |
