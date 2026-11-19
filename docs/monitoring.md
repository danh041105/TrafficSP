# Hướng dẫn dựng Monitoring (Prometheus + Grafana)

> Bản dễ hiểu, tổng kết những gì đã làm: [monitoring-tong-ket.md](monitoring-tong-ket.md). Demo 1 lệnh: `bash scripts/monitoring_demo/run_demo.sh up`.

## 1. Kiến trúc

```
Kafka ──▶ kafka-exporter ─┐
                          ├──▶ Prometheus ──▶ Grafana
Spark (metric có sẵn) ────┘
```

| Thành phần | Vai trò | Cổng trên máy |
|---|---|---|
| `kafka-exporter` | Đọc offset, số partition, lag của consumer group từ Kafka rồi mở `/metrics` | 9308 |
| Spark (PrometheusServlet) | Spark tự mở metric dạng Prometheus, không cần cài thêm gì | 8090 (master), 8091/8092 (worker), 4040 (driver job batch), 4041 (driver speed layer) |
| `prometheus` | Cứ 15 giây vào lấy (scrape) metric từ các nguồn trên, lưu 15 ngày | 9090 |
| `grafana` | Vẽ dashboard từ dữ liệu Prometheus | 3001 |

Prometheus **chủ động kéo** số liệu (pull), nên Kafka và Spark không cần biết Prometheus tồn tại, chỉ cần mở endpoint metric.

## 2. Các file liên quan

```
docker-compose.yml                         # service kafka-exporter, prometheus, grafana
conf/spark-defaults.conf                   # bật metric của Spark (mục "Monitoring")
conf/prometheus/prometheus.yml             # danh sách nguồn Prometheus đi lấy metric
conf/grafana/provisioning/datasources/     # tự tạo datasource Prometheus trong Grafana
conf/grafana/provisioning/dashboards/      # khai báo thư mục chứa dashboard
conf/grafana/dashboards/kafka.json         # dashboard Kafka
conf/grafana/dashboards/spark.json         # dashboard Spark
```

## 3. Từng bước cấu hình

### Bước 1: kafka-exporter (docker-compose.yml)

```yaml
kafka-exporter:
  image: danielqsj/kafka-exporter:v1.9.0
  command:
    - --kafka.server=kafka:29092
    - --kafka.version=3.6.0
  ports: ["127.0.0.1:9308:9308"]
  depends_on:
    kafka: { condition: service_healthy }
  networks: [traffic-net]
```

- Dùng listener **INTERNAL** `kafka:29092` vì exporter chạy trong mạng `traffic-net`.
- `--kafka.version=3.6.0` là bắt buộc. Mặc định exporter nói chuyện bằng giao thức của Kafka 2.0, mà Kafka 4.x đã bỏ các phiên bản cũ đó.

Kiểm tra: `curl -s localhost:9308/metrics | grep kafka_brokers` → phải ra `kafka_brokers 1`.

### Bước 2: bật metric của Spark (conf/spark-defaults.conf)

```properties
spark.metrics.conf.*.sink.prometheusServlet.class             org.apache.spark.metrics.sink.PrometheusServlet
spark.metrics.conf.*.sink.prometheusServlet.path              /metrics/prometheus
spark.metrics.conf.master.sink.prometheusServlet.path         /metrics/master/prometheus
spark.metrics.conf.applications.sink.prometheusServlet.path   /metrics/applications/prometheus
spark.metrics.conf.*.source.jvm.class                         org.apache.spark.metrics.source.JvmSource
spark.ui.prometheus.enabled                           true
spark.sql.streaming.metricsEnabled                    true
spark.metrics.namespace                               ${spark.app.name}
```

| Dòng | Tác dụng |
|---|---|
| `sink.prometheusServlet.*` | Mở endpoint metric dạng Prometheus trên Web UI có sẵn của master/worker/driver |
| `source.jvm.class` | Thêm metric JVM (heap, GC) |
| `spark.ui.prometheus.enabled` | Mở `/metrics/executors/prometheus` trên UI driver: task, memory, GC theo từng executor |
| `spark.sql.streaming.metricsEnabled` | Xuất metric của Structured Streaming: inputRate, processingRate, latency |
| `spark.metrics.namespace` | Tên metric của driver dùng **tên app** thay cho app-id ngẫu nhiên, để dashboard truy vấn ổn định |

File này được mount vào cả Spark master, worker và `airflow-scheduler` (nơi driver chạy), nên chỉ cần sửa một chỗ.
Sau khi sửa phải **khởi động lại** Spark master/worker để đọc lại cấu hình.

Endpoint sau khi bật:

| Nguồn | URL bên trong mạng docker |
|---|---|
| Master | `http://spark-master:8080/metrics/master/prometheus` |
| Các application (trên master) | `http://spark-master:8080/metrics/applications/prometheus` |
| Worker | `http://spark-worker-1:8081/metrics/prometheus` |
| Driver | `http://airflow-scheduler:4040/metrics/prometheus/` (**có dấu `/` cuối**, thiếu sẽ bị redirect 302) |
| Executor | `http://airflow-scheduler:4040/metrics/executors/prometheus` |

Driver chỉ mở cổng **khi đang có job chạy**. Job batch dùng cổng 4040, job streaming (speed layer) chạy song song lấy cổng 4041.

### Bước 3: Prometheus (docker-compose.yml + conf/prometheus/prometheus.yml)

```yaml
prometheus:
  image: prom/prometheus:v3.5.0
  command:
    - --config.file=/etc/prometheus/prometheus.yml
    - --storage.tsdb.path=/prometheus
    - --storage.tsdb.retention.time=15d
    - --web.enable-lifecycle
  ports: ["9090:9090"]
  volumes:
    - ./conf/prometheus:/etc/prometheus:ro
    - prometheus_data:/prometheus
```

- **Mount cả thư mục**, không mount riêng file `prometheus.yml`. Docker Desktop trên Mac/Windows có lỗi: mount từng file thì sửa trên máy xong container vẫn thấy bản cũ hoặc bị cắt cụt.
- `--web.enable-lifecycle` cho phép nạp lại cấu hình mà không cần restart:
  ```bash
  curl -X POST localhost:9090/-/reload
  ```

Trong `prometheus.yml`, mỗi nguồn là một `job`:

```yaml
scrape_configs:
  - job_name: kafka
    static_configs: [{ targets: ["kafka-exporter:9308"] }]
  - job_name: spark-master
    metrics_path: /metrics/master/prometheus
    static_configs: [{ targets: ["spark-master:8080"] }]
  - job_name: spark-worker
    metrics_path: /metrics/prometheus
    static_configs: [{ targets: ["spark-worker-1:8081", "spark-worker-2:8081"] }]
  # job batch: cổng 4040
  - job_name: spark-driver
    metrics_path: /metrics/prometheus/
    static_configs: [{ targets: ["airflow-scheduler:4040"] }]
  - job_name: spark-executors
    metrics_path: /metrics/executors/prometheus
    static_configs: [{ targets: ["airflow-scheduler:4040"] }]
  # job streaming (speed layer): cổng 4041
  - job_name: spark-speed-layer
    metrics_path: /metrics/prometheus/
    static_configs: [{ targets: ["airflow-scheduler:4041"] }]
  - job_name: spark-speed-layer-executors
    metrics_path: /metrics/executors/prometheus
    static_configs: [{ targets: ["airflow-scheduler:4041"] }]
  # ... spark-applications, prometheus (xem file đầy đủ)
```

Lưu ý: target dùng **tên service và cổng bên trong container** (`spark-master:8080`), không dùng cổng đã map ra máy (`8090`).

### Bước 4: Grafana tự nạp datasource và dashboard (provisioning)

Thêm vào service `grafana`:

```yaml
volumes:
  - ./docker-data/grafana:/var/lib/grafana
  - ./conf/grafana/provisioning:/etc/grafana/provisioning:ro
  - ./conf/grafana/dashboards:/etc/grafana/dashboards:ro
depends_on: [prometheus]
```

`conf/grafana/provisioning/datasources/prometheus.yml`:

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus          # dashboard JSON tham chiếu datasource bằng uid này
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    editable: true
```

`conf/grafana/provisioning/dashboards/dashboards.yml`:

```yaml
apiVersion: 1
providers:
  - name: traffic-monitoring
    folder: Monitoring
    type: file
    allowUiUpdates: true
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/dashboards
```

Grafana tự đọc lại thư mục dashboard mỗi 30 giây, nên thêm/sửa file JSON không cần restart.

## 4. Chạy

> Muốn xem nhanh có số liệu ngay: `bash scripts/monitoring_demo/run_demo.sh up` (xem [monitoring-tong-ket.md](monitoring-tong-ket.md) mục 5).

1. Tạo `.env` theo README (cần ít nhất `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD`).
2. Cấp cho Docker Desktop **từ 8 GB RAM trở lên** (Settings → Resources). Với 4 GB, job Spark bị OOM kill (mã thoát 137).
3. Chạy cả hệ thống:
   ```bash
   docker compose up -d --build
   ```
   Hoặc chỉ phần monitoring + Kafka + Spark khi máy yếu:
   ```bash
   docker compose up -d kafka kafka-init kafka-exporter prometheus grafana spark-master spark-worker-1 spark-worker-2
   ```

## 5. Kiểm tra

1. **Prometheus**: mở http://localhost:9090/targets
   - `kafka`, `spark-master`, `spark-applications`, `spark-worker` phải **UP**.
   - `spark-driver`, `spark-executors` (job batch) và `spark-speed-layer`, `spark-speed-layer-executors` (job streaming) chỉ UP khi Airflow đang chạy job tương ứng. Không có job thì **DOWN là bình thường**.
2. Thử truy vấn ở tab Query của Prometheus:
   | Truy vấn | Kết quả mong đợi |
   |---|---|
   | `kafka_brokers` | 1 |
   | `metrics_master_aliveWorkers_Value` | 2 |
   | `sum by (consumergroup) (kafka_consumergroup_lag)` | lag của từng consumer group |
3. **Grafana**: mở http://localhost:3001, đăng nhập bằng tài khoản trong `.env`, vào **Dashboards → Monitoring**:
   - **Kafka - Traffic**: brokers, topics, messages/s theo topic, số message đang lưu, consumer lag, offset theo partition.
   - **Spark - Traffic**: worker/app/core/RAM của cụm; task, memory, GC, I/O theo executor; input rate vs processing rate và latency của streaming.

Muốn có dữ liệu Kafka để thử: gửi vài message vào topic:

```bash
for i in $(seq 1 500); do echo "{\"id\":$i}"; done | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic traffic_raw
```

## 6. Những điểm cần biết khi đọc dashboard

- **Consumer group lag không thấy job Spark Structured Streaming.** Spark lưu offset trong checkpoint, không commit vào Kafka. Muốn biết stream có bị tụt lại không thì xem panel **Input rate vs processing rate**: processing thấp hơn input kéo dài là stream đang chậm.
- **Tỉ lệ GC > 10%** là dấu hiệu executor thiếu memory.
- **App đang chờ > 0** kéo dài nghĩa là cụm hết core hoặc RAM.
- Metric streaming có dạng `metrics_<tên app>_driver_spark_streaming_<tên query>_inputRate_total_Value`. Đặt `.queryName("...")` cho mỗi query để biểu đồ dễ đọc.

## 7. Sửa / thêm dashboard

1. Sửa trực tiếp trên Grafana (đã bật `allowUiUpdates`).
2. Muốn lưu lâu dài: **Share → Export → Save to file**, ghi đè file trong `conf/grafana/dashboards/`, giữ nguyên `uid` của dashboard rồi commit.
3. Thêm nguồn metric mới (vd. Trino, MinIO): thêm một `job_name` vào `prometheus.yml` rồi `curl -X POST localhost:9090/-/reload`.

## 8. Lỗi thường gặp

| Hiện tượng | Nguyên nhân / cách xử lý |
|---|---|
| Target `spark-driver` lỗi `302` | Thiếu dấu `/` cuối ở `metrics_path: /metrics/prometheus/` |
| Sửa `prometheus.yml` + reload mà Prometheus vẫn dùng cấu hình cũ | Đang mount từng file. Đổi sang mount cả thư mục `./conf/prometheus:/etc/prometheus:ro` rồi `docker compose up -d prometheus` |
| `kafka-exporter` không kết nối được Kafka 4.x | Thiếu `--kafka.version=3.6.0` |
| Dashboard Spark trống phần master/worker | Chưa restart Spark sau khi sửa `spark-defaults.conf`: `docker compose restart spark-master spark-worker-1 spark-worker-2` |
| Job Spark chết với mã 137 | Hết RAM, kiểm tra `docker inspect -f '{{.State.OOMKilled}}' traffic-spark-master`. Tăng RAM cho Docker hoặc giảm `spark.executor.memory` |
| Biểu đồ bị đứt đoạn | Prometheus không lấy được số liệu trong khoảng đó (container restart, máy thiếu RAM) |
