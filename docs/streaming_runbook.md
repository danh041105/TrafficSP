# Runbook Streaming / Lambda — TrafficSP

Mọi lệnh chạy từ thư mục gốc repo. Trên Windows Git Bash cần đặt `export MSYS_NO_PATHCONV=1` để đường dẫn `/opt/...` không bị đổi.

```bash
C="docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile tools"   # máy dev không có MinIO; producer thuộc profile tools
# Production có MinIO: C="docker compose --profile tools"
CH() { docker exec traffic-clickhouse bash -c "clickhouse-client -u \"\$CLICKHOUSE_USER\" --password \"\$CLICKHOUSE_PASSWORD\" -d traffic -q \"$1\""; }
SUBMIT="docker exec traffic-spark-master /opt/spark/bin/spark-submit"
```

## 1. Khởi động hạ tầng

```bash
# .env: xem README mục 1 (không commit .env)
$C build spark-master producer
$C up -d postgresql kafka kafka-init clickhouse spark-master spark-worker-1
docker exec -e PYTHONPATH=/opt/spark/jobs traffic-spark-master python3 /opt/spark/jobs/ddl/clickhouse_serving.py
$SUBMIT /opt/spark/jobs/ddl/bronze.py && $SUBMIT /opt/spark/jobs/ddl/silver.py && $SUBMIT /opt/spark/jobs/ddl/gold.py
```

Kiểm tra: `CH "SHOW TABLES"` có 20 đối tượng (bảng + view); Kafka có topic `traffic_raw` (4 partition) và `weather_raw`.

## 2. Lịch sử (GD1) và dimension phục vụ

```bash
$C run --rm producer python scripts/replay_producer.py --phase gd1            # tháng 1–9 → Kafka
docker exec -d traffic-spark-master bash -c '/opt/spark/bin/spark-submit --conf spark.cores.max=2 \
   /opt/spark/jobs/stream/stream_to_bronze.py --mode continuous >> /opt/lakehouse/logs/stream_to_bronze.log 2>&1'
$SUBMIT /opt/spark/jobs/batch/transformation/bronze_to_silver.py --start-date 2025-01-01 --end-date 2025-09-30
$SUBMIT /opt/spark/jobs/batch/gold/serving_dims.py --steps baseline,link_dim,weather
```

Kiểm tra: `CH "SELECT count(), countIf(free_flow_speed > 0) FROM link_dim FINAL"`.

## 3. Speed Layer

```bash
docker exec -d traffic-spark-master bash /opt/spark/jobs/stream/run_speed_layer.sh \
   --starting-offsets latest --trigger "5 seconds" --progress-log /opt/lakehouse/logs/speed_progress.jsonl
```

- Chỉ lần chạy **đầu tiên** đọc `--starting-offsets`; các lần sau tiếp tục từ checkpoint (`$CHECKPOINT_ROOT/speed_{latest,5min,weather}`).
- Log: `/opt/lakehouse/logs/speed_layer.log`; lịch sử khởi động lại: `speed_supervisor.log`.
- Tham số qua biến môi trường (`docker exec -e ...`):

| Biến | Mặc định |
|---|---|
| `SPEED_TRIGGER` | `10 seconds` |
| `SPEED_WATERMARK` | `10 minutes` |
| `SPEED_MAX_OFFSETS_PER_TRIGGER` | `100000` |
| `SPEED_SHUFFLE_PARTITIONS` | `8` |
| `SPEED_STATE_STORE` | `rocksdb` |
| `CLICKHOUSE_RETRIES` | `8` |
| `STALE_AFTER_MINUTES` | `15` |
| `SPEED_CORES_MAX` | `2` |
| `SPEED_EXECUTOR_MEMORY` | `1500m` |

**Dừng có kiểm soát** (không mất offset/state):
```bash
docker exec traffic-spark-master bash -c 'touch /opt/lakehouse/logs/speed_layer.stop; kill -TERM $(pgrep -f "org.apache.spark.deploy.SparkSubmit.*speed_layer.py")'
```
Không dùng `pkill -f "...speed_layer.py"` trong `bash -c`: mẫu đó khớp luôn chính tiến trình bash đang chạy lệnh.

## 4. Phát dữ liệu GD2 (realtime mô phỏng)

```bash
mkdir -p logs
$C run --rm producer python scripts/replay_producer.py --phase gd2 --start 2025-10-01T04:00 --end 2025-10-03T04:00 \
   --speedup 1440 --summary-file /app/logs/gd2_w1.json
```

- `--speedup 1440`: 1 phút thực = 1 ngày dữ liệu. `--speedup 0`: không giới hạn tốc độ.
- `--count-only`: chỉ đếm, không gửi. Toàn bộ GD2 có 3.234.480 traffic, 35.632 weather và 2.468 bản ghi giờ DST.
- Kiểm thử bất thường: `--dup-fraction 0.02 --ooo-fraction 0.03 --ooo-max-delay-sec 900 --skew-links 4616340 --skew-copies 2 --seed 11`. Bản chèn thêm có header Kafka `replay-kind`.
- Phát lại cùng dữ liệu để kiểm tra idempotent: `--run-id gd2-r2`.

## 5. Kiểm tra Serving

```bash
CH "SELECT traffic_state, count() FROM v_traffic_latest GROUP BY 1"
CH "SELECT * FROM v_traffic_5min_enriched WHERE window_start >= '2025-10-02 12:00:00' LIMIT 5 FORMAT Vertical"
CH "SELECT layer, min(window_start), max(window_start), count() FROM v_traffic_5min_unified GROUP BY layer"
CH "SELECT count() - uniqExact(link_id, window_start) FROM v_traffic_5min_unified"           # phải = 0
CH "SELECT query, max(batch_id), argMax(watermark, ts), sum(rows_dropped_by_watermark) FROM stream_progress GROUP BY query"
```

Đối chiếu độc lập với Parquet gốc (trên host, cần `duckdb`, `pandas`):
```bash
CH "SELECT link_id, toUnixTimestamp(window_start) ws, n_records, n_valid, n_zero, n_invalid_other, sum_speed, sum_travel_time, min_speed, max_speed FROM v_traffic_5min_unified WHERE window_start >= '2025-10-01 04:00:00' FORMAT CSVWithNames" > logs/u.csv
python -I jobs/tests/integration/compare_5min.py --csv logs/u.csv --start 2025-10-01T04:00 --end 2025-10-03T03:45 --months 10
```
`--end` nên nhỏ hơn watermark hiện tại, vì các cửa sổ sau watermark chưa được phát. Script thoát 0 khi `identical: true`.

## 6. Batch View và Lambda handoff

```bash
$SUBMIT /opt/spark/jobs/batch/transformation/bronze_to_silver.py --start-date 2025-10-01 --end-date 2025-10-02 --steps traffic
$SUBMIT /opt/spark/jobs/batch/gold/lambda_handoff.py --until 2025-10-02T12:00:00      # hoặc --until auto
CH "SELECT t_batch, status, published_at FROM lambda_handoff ORDER BY published_at"
```

- Thử crash: `--fail-at after_commit` hoặc `--fail-at after_verify`. Sau đó T_batch phải giữ nguyên; chạy lại lệnh không có `--fail-at` là đủ.
- Khi cluster đang bận vì các job streaming giữ hết core, chạy batch bằng `--master "local[2]" --driver-memory 2g`.
- Dữ liệu muộn sau khi đã chốt: `$SUBMIT .../batch_views.py --start-day <ngày UTC>` (REPLACE PARTITION ngày đó).
- Dọn realtime cũ (**xoá dữ liệu**, chỉ chạy khi chắc chắn): `--cleanup-rt-keep-days 7`.

## 7. Smoke test end-to-end (~4 phút; đã chạy thử 2026-11-12: 8.556 cửa sổ, latest = 2025-12-01 10:58:13)

```bash
python -m pytest -q jobs/tests/test_streaming_rules.py                                   # unit test quy tắc
$C run --rm producer python scripts/replay_producer.py --phase gd2 --start 2025-12-01T05:00 --end 2025-12-01T11:00 --speedup 720
sleep 90    # micro-batch đầu tiên sau khi Speed Layer vừa khởi động mất ~45–55 s (JVM + RocksDB khởi tạo)
CH "SELECT count() FROM traffic_5min_rt FINAL WHERE window_start >= '2025-12-01 05:00:00'"   # > 0
CH "SELECT max(event_time) FROM traffic_latest FINAL"                                        # ~ 2025-12-01 10:5x
```

## 8. Troubleshooting

| Hiện tượng | Nguyên nhân / cách xử lý |
|---|---|
| `PSQLException: password authentication failed for user "iceberg"` | `conf/postgres-init.sh` bị CRLF trên Windows nên script init không chạy. `.gitattributes` đã ép `eol=lf`. Với volume Postgres đã tạo trước đó: tạo role/database `iceberg` bằng tay hoặc tạo lại volume (**xoá dữ liệu**, hỏi trước). |
| Không pull được image MinIO (401) | Dùng `docker-compose.dev.yml` (không có MinIO, warehouse trên volume `traffic_lakehouse_dev`). |
| `None.get` khi start `speed_5min` | Lỗi `dropDuplicatesWithinWatermark` của Spark 3.5.0. Code đã dùng `dropDuplicates` có cột event-time. |
| Speed Layer thoát với `Connection refused` tới ClickHouse | ClickHouse ngừng lâu hơn ngân sách `CLICKHOUSE_RETRIES` (~2 phút). Supervisor tự chạy lại; checkpoint bảo đảm không mất và không nhân đôi dữ liệu. |
| Job batch đứng ở `Initial job has not accepted any resources` | Worker (4 core) đã bị 2 job streaming giữ hết. Chạy batch với `--master local[2]` hoặc tăng worker. |
| Cửa sổ 5 phút cuối chưa xuất hiện | Append mode chỉ phát cửa sổ khi watermark (event_time max − 10') vượt `window_end`. Phải có dữ liệu mới thì cửa sổ cuối mới được phát. |
| `REPLACE PARTITION` báo `has no parts in partition` | ClickHouse mới từ chối REPLACE từ nguồn rỗng. `batch_views.py` đã xử lý ngày rỗng bằng DROP PARTITION. |
| `ILLEGAL_AGGREGATION` khi tạo view | Alias trùng tên cột bên trong hàm gộp. Xem `v_traffic_hourly_unified` (hậu tố `_h`). |
| CSV export có `\N` | NULL của ClickHouse. `compare_5min.py` đọc với `na_values=["\\N"]`. |
| Handoff `REJECTED` vì `covers_realtime = false` | Silver chưa đủ ngày (vd chưa chạy `bronze_to_silver`). Chạy bổ sung rồi handoff lại. |
| Producer chạy được nhưng Speed Layer không thấy dữ liệu mới | Kiểm tra `max_offsets_behind` trong `stream_progress`, và kiểm tra checkpoint có đang trỏ tới topic cũ không (`--checkpoint-root`). |
