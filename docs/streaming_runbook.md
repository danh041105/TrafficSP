# Runbook Streaming / Lambda — TrafficSP (Kafka + Spark + Iceberg + Trino + Airflow)

Mọi lệnh chạy từ thư mục gốc repo. Trên Git Bash (Windows) cần `export MSYS_NO_PATHCONV=1` để đường dẫn `/opt/...` không bị đổi.

```bash
# Máy dev không có MinIO (warehouse trên volume traffic_lakehouse_dev):
C="docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile tools"
# Môi trường chính có MinIO + minio.license:  C="docker compose --profile tools"
SUBMIT="docker exec traffic-spark-master /opt/spark/bin/spark-submit"
TRINO="docker exec traffic-trino trino --catalog lakehouse"
```

## 1. Khởi động

```bash
# .env: xem README mục 1 (không commit .env)
$C build spark-master producer
docker build -f dockerfiles/airflow.dockerfile -t ad/airflow:3.3.2 dockerfiles
$C up -d postgresql kafka kafka-init spark-master spark-worker-1 trino
$C up -d airflow-init && $C up -d airflow-apiserver airflow-scheduler airflow-dag-processor   # + grafana cloudbeaver nếu cần
```

Trước khi `up`, chạy `$C config --quiet` và `docker ps` để kiểm tra cấu hình và các service đang chạy. **Không** dùng `down -v` hay `volume prune`.

Tạo bảng và view (idempotent, chạy lại an toàn):

```bash
$SUBMIT /opt/spark/jobs/ddl/bronze.py && $SUBMIT /opt/spark/jobs/ddl/silver.py
$SUBMIT /opt/spark/jobs/ddl/gold.py      # gold.* + schema serving + tag 'published'
$SUBMIT /opt/spark/jobs/ddl/speed.py     # speed.*
$TRINO --file /opt/trino-sql/trino_views.sql      # 9 × CREATE VIEW
```

## 2. Lịch sử (GD1) và free-flow

```bash
$C run --rm producer python scripts/replay_producer.py --phase gd1
$SUBMIT /opt/spark/jobs/batch/ingestion/kafka_to_bronze.py
$SUBMIT /opt/spark/jobs/batch/transformation/bronze_to_silver.py --start-date 2025-01-01 --end-date 2025-09-30
$SUBMIT /opt/spark/jobs/batch/gold/serving_dims.py          # gold.link_baseline (p85 lúc khô, ban đêm)
```

## 3. Speed Layer (chạy liên tục, không do Airflow quản lý)

```bash
docker exec -d traffic-spark-master bash /opt/spark/jobs/stream/run_speed_layer.sh --starting-offsets latest --trigger "30 seconds"
docker exec traffic-spark-master bash /opt/spark/jobs/stream/stop_speed_layer.sh      # dừng, chờ tới khi nhả khoá
```

- Supervisor dùng `flock`, nên chỉ có **một** instance. Lần khởi động thứ hai thoát với thông báo "đã có supervisor khác đang chạy".
- Ứng dụng chết thì được chạy lại sau 15 giây.
- Checkpoint nằm ở `$CHECKPOINT_ROOT/speed_v2_{latest,5min,weather}`. `--starting-offsets` chỉ có tác dụng ở lần chạy đầu của checkpoint.
- Log: `/opt/lakehouse/logs/speed_layer.log`, `speed_supervisor.log`; số đo: `speed_progress.jsonl`, `speed_latency.jsonl`, `speed_batch_stats.jsonl`.
- Biến môi trường:

  | Biến | Mặc định |
  |---|---|
  | `SPEED_TRIGGER` | `30 seconds` |
  | `SPEED_WATERMARK` | `10 minutes` |
  | `SPEED_MAX_OFFSETS_PER_TRIGGER` | `100000` |
  | `SPEED_SHUFFLE_PARTITIONS` | `8` |
  | `SPEED_STATE_STORE` | `rocksdb` |
  | `SPEED_NAMESPACE` | `lakehouse.speed` |
  | `SPEED_CORES_MAX` | `2` |
  | `SPEED_EXECUTOR_MEMORY` | `1500m` |

- Sau khi Docker Desktop khởi động lại, Speed Layer **không** tự chạy lại: chạy lại lệnh trên.

## 4. Phát dữ liệu GD2

```bash
mkdir -p logs
$C run --rm producer python scripts/replay_producer.py --phase gd2 --start 2025-12-15T11:00 --end 2025-12-16T11:00 \
   --speedup 1440 --summary-file /app/logs/gd2.json
```

- `--speedup 1440`: 1 phút thật bằng 1 ngày dữ liệu. `--speedup 0`: không giới hạn tốc độ.
- Bất thường có seed: `--dup-fraction 0.05 --ooo-fraction 0.05 --ooo-max-delay-sec 3600 --skew-links 4616340 --skew-copies 10 --seed 7`.

## 5. Batch + Lambda handoff

**Qua Airflow** (khuyến nghị): DAG `trafficsp_lambda_batch` chạy `@hourly`; UI ở http://localhost:8080.

```bash
A="docker exec -u airflow traffic-airflow-scheduler airflow"
$A dags unpause trafficsp_lambda_batch
$A dags trigger trafficsp_lambda_batch
$A dags trigger trafficsp_lambda_batch --conf '{"from_day": "2025-12-01"}'   # backfill
$A dags list-runs trafficsp_lambda_batch
```

**Chạy tay:**

```bash
$SUBMIT /opt/spark/jobs/batch/ingestion/kafka_to_bronze.py
$SUBMIT /opt/spark/jobs/batch/transformation/bronze_to_silver.py --start-date 2025-12-15 --end-date 2025-12-16
$SUBMIT /opt/spark/jobs/batch/gold/lambda_handoff.py --until auto          # recompute + kiểm tra + công bố
docker exec -e PYTHONPATH=/opt/spark/jobs -e TRINO_URL=http://trino:8080 traffic-spark-master \
   python3 /opt/spark/jobs/batch/gold/verify_serving.py
```

Khi worker đang bị Speed Layer giữ core, thêm `--master "local[2]" --driver-memory 2g` vào lệnh submit.

## 6. Truy vấn và kiểm tra

```bash
$TRINO --execute "SELECT * FROM serving.v_lambda_status"
$TRINO --execute "SELECT traffic_state, count(*) FROM serving.v_traffic_latest GROUP BY 1"
$TRINO --execute "SELECT layer, count(*), min(window_start_utc), max(window_start_utc) FROM serving.v_traffic_5min_unified GROUP BY 1"
$TRINO --execute "SELECT count(*) - count(DISTINCT (link_id, window_start_utc)) FROM serving.v_traffic_5min_unified"   # = 0
```

Đối chiếu độc lập với Parquet gốc (trên host, cần `duckdb`, `pandas`):

```bash
docker exec traffic-trino trino --output-format CSV_HEADER_UNQUOTED --execute "SELECT link_id, CAST(to_unixtime(window_start_utc) AS bigint) ws, n_records, n_valid, n_zero, n_invalid_other, sum_speed, sum_travel_time, min_speed, max_speed FROM lakehouse.serving.v_traffic_5min_unified WHERE window_start_utc >= TIMESTAMP '2025-12-04 05:00 UTC'" > logs/u.csv
python -I jobs/tests/integration/compare_5min.py --csv logs/u.csv --start 2025-12-04T05:00 --end 2025-12-16T10:45 --months 12
```

Chỉ so trên khoảng đã thực sự được phát. `--end` phải nhỏ hơn watermark, vì các cửa sổ sau watermark chưa được phát ra.

## 7. Bảo trì Iceberg

```bash
$SUBMIT /opt/spark/jobs/batch/maintenance/iceberg_maintenance.py --tables speed --compact      # an toàn, chạy song song stream được
# XOÁ snapshot/file cũ, mất time travel trước mốc. Chỉ chạy khi đã được chấp thuận:
$SUBMIT /opt/spark/jobs/batch/maintenance/iceberg_maintenance.py --tables speed,gold --expire-snapshots \
   --older-than-hours 72 --retain-last 100 --yes-delete-old-snapshots
```

Đề xuất lịch: compaction bảng `speed.*` mỗi giờ hoặc mỗi ngày; expire snapshot mỗi ngày, giữ 72 giờ. Tag `published` luôn được giữ nguyên.

## 8. Unit test

```bash
python -m pytest -q jobs/tests/test_streaming_rules.py jobs/tests/test_lambda_control.py      # 45 test
```

## 9. Troubleshooting

| Hiện tượng | Nguyên nhân / xử lý |
|---|---|
| `CONCURRENT_STREAM_LOG_UPDATE ... Multiple streaming jobs detected` | Hai ứng dụng dùng chung checkpoint. Supervisor hiện dùng `flock`. Nếu vẫn gặp: chạy `stop_speed_layer.sh` rồi khởi động lại **một** lần. Dữ liệu không trùng (đã kiểm chứng). |
| `pkill -f "...speed_layer.py"` làm chết chính lệnh `docker exec bash -c` | Mẫu khớp luôn dòng lệnh của shell. Dùng `stop_speed_layer.sh` (mẫu neo `^/opt/java/...`). |
| Trino: `Metadata not found in metadata location` (dev) | Catalog dev cần `fs.local.enabled=true` và `local.location=/`. Tên cũ `fs.native-local.enabled` đã bị đổi. |
| Trino: `Failed to create file ... metadata.json` khi `CREATE VIEW` (dev) | Volume do Spark (root) ghi. Trino trong dev chạy `user: root`. |
| Trino: `Access Denied: Cannot create table serving.x` | Đúng thiết kế: chỉ cho tạo view `serving.v_*`. |
| `Column 'avg_speed' cannot be resolved` khi đọc tag `published` | Đọc theo tag dùng schema của snapshot đó. `ddl/gold.py` đã tự commit rỗng và dời tag khi schema đổi và tag đang trỏ vào `main`. |
| Image Trino lỗi `Exec format error`, launcher 0 byte | Docker Desktop giải nén hard link thành file rỗng khi máy thiếu bộ nhớ ảo. Tăng page file, xoá image, pull lại; hoặc vá bằng file lấy từ layer gốc. |
| WSL `0xc00000fd`, `paging file is too small` | Windows hết bộ nhớ ảo. Tăng page file (đã đặt 16–24 GB) hoặc tắt bớt dịch vụ nặng. |
| `airflow-scheduler` unhealthy, `No module named 'airflow'` | Dev chạy scheduler bằng root; override đã đặt `PYTHONUSERBASE=/home/airflow/.local`. Lệnh CLI dùng `docker exec -u airflow`. |
| Handoff thoát mã 2 | Một handoff khác đang giữ advisory lock. Chờ nó xong. |
| Handoff `FAILED: checks failed`, `covers_realtime=false` | Silver chưa đủ ngày. Chạy bổ sung `bronze_to_silver` rồi handoff lại. |
| Cửa sổ 5 phút cuối chưa xuất hiện | Append mode chỉ phát cửa sổ khi watermark (event_time max − 10') vượt `window_end`. Phải có dữ liệu mới thì cửa sổ cuối mới được phát. |
