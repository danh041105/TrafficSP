

## Mục lục

1. [Chạy demo](#1-chạy-demo)
2. [Tổng quan](#2-tổng-quan)
3. [Cấu trúc thư mục](#3-cấu-trúc-thư-mục)
4. [Chi tiết công việc đã làm](#4-chi-tiết-công-việc-đã-làm)
5. [Mô hình dữ liệu mức vật lý](#5-mô-hình-dữ-liệu-mức-vật-lý)
6. [Serving Layer (API)](#6-serving-layer-api)
7. [Giao diện](#7-giao-diện)
8. [Monitoring](#8-monitoring)
9. [Tích hợp các phần](#9-tích-hợp-các-phần)
10. [Xử lý lỗi](#10-xử-lý-lỗi)
11. [Lịch sử commit và việc còn lại](#11-lịch-sử-commit-và-việc-còn-lại)
12. [Đưa lên GitHub](#12-đưa-lên-github)

---

## 1. Chạy demo

### Yêu cầu

- **Docker Desktop** đang chạy, cấp **≥ 3,5 GB RAM** cho Docker (máy 8 GB: đóng bớt ứng dụng khác).
- Lần đầu cần Internet để build image (pip, npm) và tải tile bản đồ.
- Không chạy cùng lúc với repo TrafficSP đầy đủ (hai bên trùng tên container).

### Một lệnh

Mở terminal **trong thư mục này** (terminal của VS Code đã mở sẵn ở đây):

```bash
bash run_demo.sh up          # mặc định 20 phút dữ liệu giả; "up 60" cho 60 phút
```

Script làm 4 bước:

1. **Monitoring**: bật Kafka, Spark (master + 2 worker), kafka-exporter, Prometheus, Grafana. Bơm dữ liệu giả vào `traffic_raw` (20–50 message/giây) và `weather_raw`. Chạy một consumer cố tình đọc chậm (để thấy lag) và một job Spark streaming (để có số liệu executor/streaming).
2. **Serving demo**: build image API + web, bật ClickHouse. Lần đầu nạp dữ liệu nền 01–09/2025 từ `data/raw`, mất khoảng 2 phút.
3. Bật **replay** tháng 10/2025 (từ 05:00 ngày 07/10, tốc độ ×20), **Serving API** và **dashboard**.
4. Bảo Prometheus nạp lại cấu hình để đọc thêm job `serving-api`.

### Mở gì

| Địa chỉ | Thấy gì |
|---|---|
| http://localhost:3000 | Dashboard giao thông + thời tiết NYC |
| http://localhost:3001 → Dashboards → Monitoring | Grafana: **Kafka - Traffic**, **Spark - Traffic**, **Serving - Traffic** (tài khoản `admin` / `admin`) |
| http://localhost:8000/docs | Tài liệu API (Swagger) · số đo: http://localhost:8000/metrics |
| http://localhost:9090/targets | Prometheus đang đọc những nguồn nào |
| http://localhost:8085 | Kafka UI |
| http://localhost:8090 | Spark master |

```bash
bash run_demo.sh status      # container + target Prometheus
bash run_demo.sh down        # tắt (giữ dữ liệu)
```

### Kết quả mong đợi (đã chạy thật trên máy 8 GB)

| Kiểm tra | Kết quả |
|---|---|
| `bash run_demo.sh status` | Target `kafka`, `spark-*`, `serving-api` đều **up**. `spark-speed-layer*` **down** là bình thường vì demo không chạy speed layer thật |
| Dashboard :3000 | Nhãn **DEMO**, giờ dữ liệu chạy từ 05:00 07/10/2025, bản đồ 124 đoạn, ~90 đoạn có số đo; khoảng 07:00–09:00 số đoạn tắc tăng rõ |
| Grafana "Serving - Traffic" | Nguồn OK, "dữ liệu đứng yên" chỉ vài giây, biểu đồ trạng thái, request/giây, p50/p95 |
| Grafana "Kafka - Traffic" | 20–50 message/giây, lag của `demo-slow-consumer` tăng dần |
| Grafana "Spark - Traffic" | 2 worker sống, app `monitoring_demo` đang chạy, các lô streaming |

Lần đầu mất vài phút (Grafana khởi tạo, nạp dữ liệu). Nếu trang trống trong 10–20 giây đầu, tải lại là có dữ liệu.

---

## 2. Tổng quan

### Phân công của nhóm mô hình vật lý (tạo bảng cụ thể) + giao diện**, làm thêm **monitoring**.

### Quyết định quan trọng

| Quyết định | Lý do |
|---|---|
| Serving đọc **view** `lakehouse.serving.*` qua Trino, không đọc bảng | View là hợp đồng giữa các tầng: batch/speed đổi bảng bên dưới thì API không phải sửa |
| Giữ **chế độ demo** bằng ClickHouse | Máy 8 GB không chạy nổi cả stack (Spark + Trino + MinIO + Airflow). Demo vẫn dùng dữ liệu thật `data/raw` và cùng quy tắc trạng thái |
| Không thêm lại ClickHouse vào kiến trúc chính | Nhóm đã quyết định thay bằng Trino (commit "replace clickhouse by trino") |
| Đồng hồ = **event time** mới nhất, không dùng giờ máy | Dữ liệu là bản phát lại năm 2025 |
| Trạng thái giao thông theo đúng quy tắc speed layer (0,5 / 0,8 × free-flow) | Một quy tắc duy nhất cho Spark, Trino, API và giao diện |
| Thêm `/metrics` cho API | Monitoring trước đó chỉ thấy dữ liệu vào và xử lý, chưa thấy dữ liệu ra tới người dùng |

---

## 3. Cấu trúc thư mục

```
TrafficSP-Mai/
├── README.md                  tài liệu duy nhất (file này)
├── run_demo.sh                demo tổng: up | status | down
├── docker-compose.yml         14 service: Kafka, Spark, Prometheus, Grafana, kafka-exporter, serving-*
├── .env.example               mẫu biến môi trường (không bắt buộc)
├── serving/
│   ├── api/                   FastAPI: main.py, rules.py, metrics.py, sources/{base,trino_source,demo_source}.py
│   ├── demo/                  schema.sql, load_demo.py, replay_demo.py, common.py
│   ├── web/                   dashboard React (Vite), nginx.conf
│   └── trino_views.reference.sql   bản sao view serving của Khải (đối chiếu hợp đồng)
├── conf/
│   ├── prometheus/prometheus.yml
│   ├── grafana/provisioning/  datasource + provider dashboard
│   ├── grafana/dashboards/    kafka.json, spark.json, serving.json
│   └── spark-defaults.conf    (phần metrics Spark)
├── scripts/monitoring_demo/   run_demo.sh + speed_demo.py (dữ liệu giả + job Spark streaming)
├── dockerfiles/               spark, serving-api, serving-web
└── data/raw/                  dữ liệu thô của nhóm: NYC DOT 2025 (parquet) + ECMWF IFS (16 ô, JSON)
```

---

## 4. Chi tiết công việc đã làm

Mỗi hạng mục ghi theo khung: **mục tiêu → các bước → vấn đề và cách xử lý → kiểm chứng**.

### 4.1 Khảo sát dữ liệu và bản nháp thiết kế vật lý (05/11)

**Mục tiêu:** hiểu dữ liệu thô trước khi thiết kế bảng; đưa ra bản thiết kế vật lý đầu tiên cho nhóm.

**Các bước:**

1. Đọc tài liệu đề tài: NYC DOT Traffic Speed 2025 đo 5 phút/lần; thời tiết Open-Meteo mô hình ECMWF IFS chia NYC thành 16 ô; giai đoạn 1 batch 01–09/2025, giai đoạn 2 replay 10–12/2025.
2. Đọc repo ban đầu để biết công cụ: Spark 3.5 + Iceberg 1.10 (JDBC catalog trên Postgres), MinIO, Kafka KRaft, Airflow 3, ClickHouse, Grafana.
3. Khảo sát `2025-01.parquet` bằng Python (pyarrow + pandas):
   - 13 cột: `id, speed, travel_time, status, data_as_of, link_id, link_points, encoded_poly_line, encoded_poly_line_lvls, owner, transcom_id, borough, link_name`.
   - 1.079.027 dòng/tháng, 122 `link_id` (cả năm 124), 5 quận; `data_as_of` là giờ New York, không mang múi giờ.
   - `status = -101` chiếm ~25% (265.136 dòng), `speed` thường bằng 0 → **không tính vào trung bình**.
   - `link_points` **bị cắt ở đúng 255 ký tự** → cặp toạ độ cuối có thể cụt.
   - `encoded_poly_line` bị escape dấu `\` nhiều lớp, một link có tới 15 biến thể. Viết bộ giải mã: 104/122 link giải mã được, 18 link phải dùng `link_points`.
4. Viết bản nháp:
   - **Iceberg**: Silver `traffic_speed` (lưu `ts_local` + `ts_utc`, cờ `is_valid`, partition `days(ts_utc)`), `weather_hourly`; Gold star schema: `dim_link` (SCD2, GeoJSON, free-flow P85), `dim_date` (có ngày lễ), `dim_time` (288 slot 5'), `dim_weather_grid`, `dim_congestion_level`, `fact_traffic_5min` (join sẵn thời tiết, partition `days(slot_utc), bucket(8, link_id)`), `fact_traffic_hourly`, `fact_traffic_forecast`.
   - **ClickHouse hot tier**: `traffic_realtime` (MergeTree, TTL 14 ngày), `link_current_state` (ReplacingMergeTree + materialized view), `traffic_15min_agg` (AggregatingMergeTree), `traffic_hourly`, `traffic_forecast`, `weather_current`, view `v_map_current`, `v_kpi_now`.

**Kết quả:** bản nháp là đầu vào cho nhóm. Gold chính thức do **Trần Bách** viết (`jobs/ddl/gold.sql` trong repo). Các ý tưởng được giữ lại: lưu UTC song song giờ địa phương, trạng thái theo tỉ lệ với free-flow P85, giải mã geometry một lần ở dimension, view là hợp đồng giữa các tầng.

### 4.2 Bản thử nghiệm dashboard đầu tiên (05/11)

**Mục tiêu:** có giao diện chạy được sớm bằng dữ liệu thật, không phải chờ ingestion/streaming.

**Các bước:** dựng ClickHouse riêng; loader đọc 12 file parquet tạo `dim_link` (giải mã polyline, fallback `link_points`, độ dài haversine, P85, ô thời tiết gần nhất) và tổng hợp theo giờ cả năm (812.866 dòng); replay lấy ngày 2025 cùng thứ với hôm nay; FastAPI 13 endpoint; dashboard React + Leaflet + ECharts. Sau đó thêm **ô tìm đoạn đường**: gõ để lọc, bản đồ bay tới đoạn được chọn, viền sáng đoạn đó, các đoạn khác mờ đi.

| Vấn đề | Cách xử lý |
|---|---|
| Tile CARTO báo "API KEY REQUIRED" | Đổi sang OpenStreetMap, đảo màu thành nền tối bằng CSS filter |
| KPI tính cả cảm biến lỗi vào tốc độ TB | `avgIf(..., level > 0)` |
| View bản đồ trả khoá `d.link_id` | Đặt alias rõ ràng `AS link_id` |

### 4.3 Monitoring Kafka + Spark (19/11, commit `5ea5bc5`)

**Mục tiêu:** nhìn được sức khoẻ hệ thống (dữ liệu vào Kafka, xử lý ở Spark) mà không phải đọc log.

**Các bước:**

1. **kafka-exporter** `danielqsj/kafka-exporter:v1.9.0`: nối `kafka:29092`, `--kafka.version=3.6.0`, cổng 9308.
2. **Bật metrics Spark** (`conf/spark-defaults.conf`): `PrometheusServlet` cho master/applications/worker/driver, `spark.ui.prometheus.enabled` (metrics executor), `spark.sql.streaming.metricsEnabled`, `JvmSource`, `spark.metrics.namespace=${spark.app.name}`.
3. **Prometheus** `prom/prometheus:v3.5.0`: scrape 15 giây, lưu 15 ngày, `--web.enable-lifecycle`. 9 job: `prometheus`, `kafka`, `spark-master`, `spark-applications`, `spark-worker`, `spark-driver`, `spark-executors` (cổng 4040), `spark-speed-layer`, `spark-speed-layer-executors` (cổng 4041).
4. **Grafana provisioning**: datasource `Prometheus` (uid `prometheus`) + provider đọc `/etc/grafana/dashboards` vào folder "Monitoring", quét lại 30 giây/lần.
5. **Dashboard "Kafka - Traffic"** (8 panel) và **"Spark - Traffic"** (13 panel, 3 nhóm). Chi tiết ở mục 8.
6. **Demo một lệnh** `scripts/monitoring_demo/run_demo.sh up|down`.

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| Không lấy được metrics driver Spark | Driver redirect 302 sang `/metrics/prometheus/` | `metrics_path` có dấu `/` cuối |
| Sửa `prometheus.yml` mà vẫn chạy cấu hình cũ, mất 1 job | Docker Desktop lỗi khi bind mount **từng file** | Mount cả thư mục `./conf/prometheus` |
| kafka-exporter không nối được Kafka 4.x | Mặc định giao thức Kafka 2.0 | `--kafka.version=3.6.0` |
| Job Spark bị tắt mã 137 | Docker hết RAM | Driver/executor 512 MB, giới hạn số core |

**Kiểm chứng:** mọi target `kafka`, `spark-*` UP khi có job, hai dashboard có số liệu.

### 4.4 Đưa Serving Layer vào repo theo kiến trúc mới (22/11, commit `0e4b8e5`, `b2a52df`)

**Cập nhật nhánh và đọc kiến trúc:**

1. Nhánh `mai-monitoring` thiếu 5 commit của `dev` → merge `origin/dev` vào `mai-monitoring`; xung đột README giữ cả hai mục. **Không đụng vào nhánh `dev`.**
2. Phát hiện:
   - ClickHouse đã bị thay bằng **Trino**.
   - Speed layer của Khải ghi `lakehouse.speed.*` và định nghĩa view `lakehouse.serving.*` (`jobs/serving/trino_views.sql`, nhánh `khai-streaming-speed-layer`, chưa merge).
   - Quy tắc trạng thái: `CONGESTED < 0,5 ≤ SLOW < 0,8 ≤ FREE_FLOW`, `STALE` sau 15 phút; free-flow = P85 khung 22h–5h New York, ≥ 100 mẫu.
3. Máy 8 GB không chạy được lakehouse → chọn **API đọc view Trino + chế độ demo ClickHouse** (bật bằng biến môi trường).

**Viết code:** Serving API 2 nguồn (mục 6), chế độ demo (mục 6.4), chuyển dashboard sang dữ liệu mới và làm lại màu (mục 7), Dockerfile + nginx + compose profile (mục 9).

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| Loader bị kill (137) | Gộp `link_points` 9 tháng (~7 triệu chuỗi) trong RAM khi Kafka/Spark/Grafana đang chạy | Khử trùng mỗi tháng xuống mức link trước khi gộp |
| `compose run serving-demo-load` báo "no such service" | Service phụ thuộc ở profile chưa bật | Chạy kèm `--profile demo` |
| Replay lỗi `'function' object has no attribute 'to_numpy'` | Cột `hist` trùng phương thức `DataFrame.hist` | Dùng `p["hist"]` |
| ClickHouse `ILLEGAL_AGGREGATION` | Alias `speed` trùng tên cột → `minIf(speed)` bị hiểu lồng trong `avgIf` | Đổi alias `avg_spd`, `min_spd` |
| TypeScript: `p.upper` có thể null | Điểm nối thực tế ↔ dự báo | Kiểm tra null |

**Kiểm chứng:**

- **Demo:** chạy hoàn toàn bằng Docker Compose; cả 15 endpoint trả dữ liệu, `/api/links/999` → 404, 124/124 đoạn có LineString, dashboard hiển thị đủ, console sạch.
- **Trino:** dựng Trino 484 connector `memory` (heap 900 MB), tạo bảng mang **đúng tên + cột** 6 đối tượng API đọc (`v_link_dim`, `silver.link_info`, `v_traffic_latest`, `v_traffic_5min_unified`, `v_traffic_hourly_unified`, `v_weather_interval`), nạp mẫu (124 / 124 / 102 / 25.335 / 2.764 / 784 dòng). API với `SERVING_SOURCE=trino`: mọi endpoint 200 và có dữ liệu; `forecast` trả `[]` vì chưa có view. **Chưa chạy trên lakehouse thật.**

### 4.5 Nối Serving vào monitoring (22/11, commit `413f17a`)

**Mục tiêu:** thấy được **dữ liệu ra tới người dùng**: API có khoẻ không, dữ liệu trên dashboard có còn mới không.

**Các bước:** viết `serving/api/metrics.py` (mục 8.4), thêm job `serving-api` vào Prometheus, dashboard Grafana **"Serving - Traffic"** (13 panel), script demo tổng `run_demo.sh`, cho phép chỉnh số core Spark demo (`DEMO_SPARK_CORES`).

| Vấn đề | Cách xử lý |
|---|---|
| Panel tỉ lệ lỗi trống khi chưa có lỗi 5xx | `(sum(rate(...{status=~"5.."}[5m])) or vector(0))` |
| Ô "Đồng hồ dữ liệu" bị tô đỏ bởi ngưỡng mặc định | `colorMode: none` |
| Lệnh `status` lỗi cú pháp f-string trên Python 3.9/3.10 | Dùng định dạng `%` |
| p95 lúc khởi động lên 30 giây | Request đầu gặp cache nguội khi ClickHouse đang nạp replay; sau đó còn ~16–80 ms. Không cần sửa |

**Kiểm chứng:** chạy từng câu PromQL của dashboard trên Prometheus đều `success`; target `serving-api` UP. Thí nghiệm `docker stop traffic-serving-clickhouse`: sau ≤ 5 giây `serving_source_up=0`, `serving_source_errors_total=1`, API trả 500; bật lại → `serving_source_up=1`, API 200.

### 4.6 Đóng gói thư mục độc lập (22/11, commit `b210e76`)

**Các bước:** copy `serving/`, `conf/`, `scripts/monitoring_demo`, 3 Dockerfile, `data/raw` (60 MB), bản sao `trino_views.sql` của Khải; tách `docker-compose.yml` riêng 14 service, bỏ phụ thuộc Trino/MinIO/Postgres, mặc định `SERVING_SOURCE=demo`, Grafana `admin`/`admin`; thêm `.gitignore`, `.env.example`; chạy thử từ thư mục mới.

| Vấn đề | Cách xử lý |
|---|---|
| Replay mất ~6 phút mới khởi động (đọc 3 tháng, 2,1 triệu bản ghi khi Spark/Grafana chiếm CPU) | Chỉ đọc tháng chứa mốc bắt đầu → **21 giây** (650.880 bản ghi) |
| Docker tự tạo thư mục `jobs/` rỗng | Bỏ mount `./jobs` của Spark |
| Không chạy song song với repo | Trùng tên container; tắt bên này trước khi chạy bên kia |

**Kiểm chứng:** `bash run_demo.sh up` xong trong ~4,5 phút; mọi target UP (trừ speed layer); Grafana tự nạp đủ 3 dashboard; dashboard giao thông có số liệu.

---

## 5. Mô hình dữ liệu mức vật lý

### 5.1 Lý thuyết

**Ba mức thiết kế dữ liệu:**

| Mức | Trả lời câu hỏi | Ví dụ |
|---|---|---|
| Khái niệm | Có những thực thể nào, quan hệ ra sao? | Đoạn đường, ô thời tiết, lần đo tốc độ, giờ |
| Logic | Bảng nào, cột nào, khoá nào, grain là gì? | `fact_congestion_hourly` grain = 1 đoạn × 1 giờ UTC |
| **Vật lý** | Lưu thế nào trên hệ thống cụ thể để truy vấn nhanh, ghi an toàn? | Kiểu dữ liệu, định dạng file, partition, thứ tự sắp xếp, engine, view |

"Chuyển xuống mức vật lý" nghĩa là biến bản vẽ logic thành **câu lệnh tạo bảng chạy được** trên công cụ thật, kèm các quyết định lưu trữ.

**Star schema (Kimball):** *fact* chứa số đo, rất nhiều dòng, phải ghi rõ grain; *dimension* chứa ngữ cảnh để lọc và nhóm. *Surrogate key* là khoá do kho tự cấp, tách khỏi khoá nguồn. *SCD1* ghi đè giá trị cũ; *SCD2* thêm phiên bản mới kèm `valid_from`/`valid_to`.

**Các quyết định vật lý:**

| Quyết định | Ý nghĩa | Sai thì sao |
|---|---|---|
| Kiểu dữ liệu | `timestamp with time zone` cho thời điểm, `bigint` cho id, `double` cho số đo | Lệch múi giờ, join sai kiểu |
| Định dạng file | Parquet (dạng cột): chỉ đọc cột cần, nén tốt | Đọc cả dòng, tốn I/O |
| Table format | Iceberg: snapshot, time travel, schema evolution, hidden partitioning, tag | Ghi dở không rollback được |
| Partition | Chia theo cột hay lọc (`months(hour_start_utc)`) để bỏ qua file không cần đọc | Quá mịn → nghìn file nhỏ; quá thô → quét thừa |
| Khoá sắp xếp | Dữ liệu cùng link, cùng thời gian nằm cạnh nhau | Truy vấn 1 link phải quét hết |
| View | Tầng trừu tượng: người dùng đọc view, không đọc bảng | Đổi bảng là vỡ dashboard |

**Hai loại hệ thống:** Lakehouse (Iceberg + Trino) là nguồn sự thật, ghi bằng Spark, đọc bằng Trino, mạnh ở quét lớn và time travel. OLAP ClickHouse có `MergeTree` sắp theo `ORDER BY` (chỉ mục thưa), `ReplacingMergeTree` giữ bản mới nhất theo khoá, `TTL` tự xoá dữ liệu cũ; nhóm đã bỏ khỏi kiến trúc chính, Mai chỉ dùng cho chế độ demo.

### 5.2 Gold của nhóm (bản chính thức, Trần Bách viết)

| Bảng | Loại | Grain / khoá | Quyết định vật lý |
|---|---|---|---|
| `dim_date` | dim, SCD0 | `date_key` YYYYMMDD (giờ New York) | |
| `dim_hour` | dim | `hour_key` 0–23 + `time_band` | |
| `dim_weather_grid` | dim, SCD1 | `grid_key` = `cell_id` Silver | giữ khoá nguồn để join thẳng |
| `dim_weather_condition` | dim | `condition_key` (nhóm mưa/tuyết/gió/nhiệt) | |
| `dim_road_link` | dim, **SCD2** | `road_link_key` surrogate, `link_id` nguồn, `valid_from`/`valid_to` | geometry WKT, `free_flow_speed_mph` |
| `fact_traffic_reading` | fact | 1 phiên bản đoạn × 1 lần đo UTC | `months(reading_ts_utc)` |
| `fact_weather_hourly` | fact | 1 ô × 1 giờ UTC | `months(hour_start_utc)` |
| `fact_congestion_hourly` | fact | 1 phiên bản đoạn × 1 giờ UTC | lưu **tổng** (`sum_travel_time_sec`, `sum_free_flow_time_sec`) |

- **Lưu tổng thay vì trung bình**: TTI = `sum_travel_time / sum_free_flow_time` cộng dồn được qua nhiều giờ, còn trung bình của trung bình thì sai.
- **Giữ UTC trong timestamp**, ngày và giờ New York để trong dimension → tránh lỗi DST.

### 5.3 View serving (Khải) mà giao diện đọc

Speed layer ghi `lakehouse.speed.*` (`traffic_latest`, `traffic_5min_rt`, `weather_obs`). File `serving/trino_views.reference.sql` (bản sao) định nghĩa:

| View | Vai trò |
|---|---|
| `v_traffic_5min_unified` | **Gộp batch + speed** không chồng lấn: trước `t_batch` lấy batch (tag Iceberg `published`), từ `t_batch` lấy speed |
| `v_traffic_hourly_unified` | Như trên theo giờ, cộng tổng thành phần, không AVG(AVG) |
| `v_traffic_latest` | 1 dòng / đoạn, tính `traffic_state` và `STALE` theo đồng hồ sự kiện |
| `v_link_dim`, `v_weather_interval` | Thông tin đoạn; thời tiết theo khoảng [H, H+1), cờ `FINAL`/`PROVISIONAL` |

`SECURITY INVOKER` + `iceberg.security=read_only`: Trino chỉ đọc, chỉ tạo được view `serving.v_*`; Spark là nơi ghi duy nhất.

### 5.4 Phần vật lý của Mai

1. **Hợp đồng đọc của Serving API** (mục 6.3) và đề xuất view mới `serving.v_traffic_forecast` cho phần MLOps.
2. **Schema chế độ demo** `serving/demo/schema.sql` (ClickHouse), mỗi bảng mô phỏng một view:

| Bảng | Mô phỏng | Engine / khoá | Vì sao |
|---|---|---|---|
| `dim_link` | `v_link_dim` + `link_points` | `ReplacingMergeTree ORDER BY link_id` | nạp lại không nhân đôi |
| `reading` | bản ghi speed layer nhận | `MergeTree ORDER BY (link_id, event_time)` | lịch sử 1 link nằm liền nhau |
| `hourly` | `v_traffic_hourly_unified` | `MergeTree ORDER BY (link_id, hour_utc)` | lưu `sum_speed`, `n_valid` (cộng dồn được) |
| `weather` | `v_weather_hourly` | `ORDER BY (cell_id, time_utc)` | |
| `forecast` | `v_traffic_forecast` | `ReplacingMergeTree(issued_at)` | chỉ giữ dự báo mới nhất |

3. **Bản nháp đầu tiên** (mục 4.1).

Kiểm tra bảng demo khi đang chạy:

```bash
docker exec traffic-serving-clickhouse clickhouse-client --user demo --password demo \
  -q "SELECT table, engine, sorting_key, total_rows FROM system.tables WHERE database='traffic'"
```

---

## 6. Serving Layer (API)

### 6.1 Lý thuyết

**Lambda architecture:**

| Tầng | Nhiệm vụ | Trong đề tài |
|---|---|---|
| Batch | Xử lý lại toàn bộ lịch sử, chính xác, chậm | Spark: bronze → silver → gold theo lịch Airflow |
| Speed | Bù phần batch chưa kịp xử lý, nhanh, gần đúng | Spark Structured Streaming, cửa sổ 5 phút |
| **Serving** | **Gộp** batch view + realtime view và trả lời truy vấn | View Trino `serving.*` + Serving API |

Khó nhất của serving là **gộp mà không trùng và không hở**. TrafficSP dùng mốc `t_batch`: trước mốc lấy batch (đã kiểm chứng), từ mốc trở đi lấy speed. Batch chạy xong thì mốc dời lên.

**Event time và processing time:** event time là lúc sự việc xảy ra (`data_as_of`), processing time là lúc hệ thống xử lý. Dữ liệu là bản phát lại năm 2025, nên "bây giờ" là **đồng hồ sự kiện** (event time lớn nhất đã nhận); khung "24h qua" và trạng thái `STALE` đều tính theo đồng hồ này.

**Thiết kế API cho dashboard:**

- *Đọc qua lớp trừu tượng*: API phụ thuộc giao diện `Source`, không phụ thuộc Trino hay ClickHouse; đổi nguồn chỉ cần đổi biến môi trường.
- *Cache TTL*: dashboard làm mới 10 giây/lần, nhiều panel hỏi cùng một thứ. Cache 5 giây cho trạng thái mới nhất, 5–10 phút cho dimension và heatmap.
- *Tính đúng chỗ*: phần nặng (gộp 15 phút, heatmap) để SQL làm; phần nhẹ trên ~124 dòng (KPI, top tắc, theo quận) làm bằng Python, nên một câu `latest()` phục vụ được 5 endpoint.
- Trung bình tốc độ luôn là `tổng speed / tổng số mẫu hợp lệ`, không lấy trung bình của trung bình.

### 6.2 Cấu trúc code

```
serving/api/
├── main.py              15 endpoint + cache TTL + chọn nguồn theo SERVING_SOURCE
├── rules.py             trạng thái (0,5 / 0,8 × free-flow, STALE 15'), tách link_points, độ dài haversine
├── metrics.py           /metrics cho Prometheus (mục 8.4)
└── sources/
    ├── base.py          giao diện Source: clock, links, latest, history_15min, dow_hour_profile, weather_at, forecast
    ├── trino_source.py  đọc lakehouse.serving.* (nguồn thật)
    └── demo_source.py   đọc ClickHouse demo (cùng hình dạng dữ liệu)
```

| Hàm `Source` | Trino (thật) | Demo |
|---|---|---|
| `clock()` | `max(event_clock)` từ `v_traffic_latest` | `max(event_time)` từ `reading` |
| `links()` | `v_link_dim` ⨝ `silver.link_info.link_points` | `dim_link` |
| `latest()` | `v_traffic_latest` (đã có `traffic_state`) | `argMax` theo link, rồi `rules.classify` |
| `history_15min()` | `v_traffic_5min_unified`, `sum(sum_speed)/sum(n_valid)` theo 15' | `reading`, `toStartOfFifteenMinutes` |
| `dow_hour_profile()` | `v_traffic_hourly_unified`, đổi sang giờ New York | `hourly` |
| `weather_at()` | `v_weather_interval`, khoảng gần nhất ≤ đồng hồ | `weather` giờ H, mưa lấy mốc H+1 nếu đã tới |
| `forecast()` | `v_traffic_forecast`; chưa có view → `[]` | `forecast` (baseline) |

### 6.3 Hợp đồng dữ liệu (chế độ Trino)

| View | Cột API dùng | Dùng cho |
|---|---|---|
| `serving.v_traffic_latest` | link_id, speed, travel_time, last_valid_event_time, traffic_state, event_clock | bản đồ, KPI, top tắc, đồng hồ dữ liệu |
| `serving.v_link_dim` | link_id, link_name, borough, free_flow_speed, cell_id, latitude, longitude | thông tin đoạn đường |
| `silver.link_info` | link_points | vẽ đường |
| `serving.v_traffic_5min_unified` | link_id, window_start_utc, sum_speed, n_valid, min_speed | lịch sử 24h |
| `serving.v_traffic_hourly_unified` | link_id, hour_utc, sum_speed, n_valid | heatmap, TB lịch sử |
| `serving.v_weather_interval` | cell_id, interval_start, temperature_c, precipitation_mm, wind_speed_kmh, weather_code, weather_status | thời tiết |
| `serving.v_traffic_forecast` *(chưa có)* | link_id, issued_at, target_time, horizon_min, pred_speed, pred_lower, pred_upper, model_name, model_version | đường dự báo |

Khi phần MLOps tạo view `v_traffic_forecast` với các cột trên, dashboard tự hiện đường dự báo. Trước đó API trả `[]` và dashboard ghi "Chưa có dự báo".

### 6.4 Chế độ demo

- `load_demo.py`: đọc `data/raw` giai đoạn 1 giống Silver (bỏ `status=-101`, bỏ giờ DST lặp hoặc không tồn tại, đổi giờ New York sang UTC); chọn `link_points` dài nhất; ô thời tiết gần nhất; free-flow theo đúng quy tắc speed layer. Kết quả: 124 đoạn (112 có free-flow), 609.091 dòng giờ, 140.928 dòng thời tiết.
- `replay_demo.py`: phát lại giai đoạn 2, **giữ nguyên timestamp 2025** (giống producer `gd2`), nạp sẵn 24h trước mốc bắt đầu, chỉ đọc tháng cần phát.
- **Dự báo baseline** (không phải model của nhóm, `model_name = baseline_seasonal_blend`):

```
dự_báo(h) = α · tốc_độ_hiện_tại + (1 − α) · TB_lịch_sử(cùng thứ, cùng giờ)
α = 0,7 (15'), 0,5 (30'), 0,3 (60')   ·   khoảng tin cậy ≈ ±1,28 · độ lệch chuẩn lịch sử
```

Dự báo gần thì tin vào hiện tại; dự báo xa thì tin vào quy luật lịch sử.

Đổi điểm bắt đầu hoặc tốc độ replay:

```bash
DEMO_START="2025-11-04 06:00" DEMO_SPEED=30 SERVING_SOURCE=demo docker compose --profile demo up -d --force-recreate serving-demo-replay
```

### 6.5 Endpoint

| Endpoint | Dùng ở đâu |
|---|---|
| `/api/health`, `/api/meta` | nhãn TRINO/DEMO, giờ dữ liệu, healthcheck Docker |
| `/api/map/current` | bản đồ (GeoJSON LineString từ `link_points`, Point nếu thiếu) |
| `/api/kpi/now`, `/kpi/states`, `/kpi/boroughs` | ô KPI, thanh trạng thái, theo quận (lọc `?borough=`) |
| `/api/links`, `/api/links/top-congested` | danh sách đoạn, top đoạn chậm nhất so với free-flow |
| `/api/links/{id}`, `/history`, `/forecast`, `/heatmap` | panel chi tiết khi chọn một đoạn |
| `/api/city/today`, `/city/heatmap` | toàn mạng 24h so với lịch sử; heatmap thứ × giờ |
| `/api/weather/current` | panel và lớp thời tiết (16 ô ECMWF IFS) |

```bash
curl -s localhost:8000/api/kpi/now
curl -s "localhost:8000/api/links/top-congested?limit=3"
```

Chạy API trực tiếp trên máy, không cần build image:

```bash
pip install -r serving/api/requirements.txt
SERVING_SOURCE=demo uvicorn serving.api.main:app --port 8000 --reload
```

---

## 7. Giao diện

### 7.1 Lý thuyết

**Dashboard vận hành** trả lời "bây giờ thế nào, có cần làm gì không":

1. Thứ quan trọng nhất nằm ở chỗ dễ thấy nhất: bản đồ ở giữa, KPI ở trên cùng bên trái.
2. Từ tổng quan đến chi tiết: xem toàn mạng trước, bấm vào một đoạn mới xem 24h, dự báo, heatmap.
3. Luôn cho biết dữ liệu mới tới đâu: hiện "giờ dữ liệu" và nguồn (TRINO/DEMO).
4. Không để người xem đoán: trạng thái nào cũng có nhãn chữ, "không có số đo" phải khác hẳn "thông thoáng".

**Chọn dạng biểu đồ theo việc dữ liệu cần làm:**

| Thể hiện | Dạng | Ở dashboard |
|---|---|---|
| Một con số chính | ô KPI | tốc độ TB, số đoạn tắc/chậm |
| Trạng thái theo vị trí | bản đồ đường tô màu | Leaflet polyline |
| Thay đổi theo thời gian | đường | 24h + dự báo; toàn mạng so với lịch sử |
| So sánh nhóm | cột ngang | tốc độ theo quận |
| Mẫu lặp 2 chiều | heatmap | thứ × giờ |
| Tỉ lệ thành phần | thanh xếp chồng | phân bố trạng thái |

**Màu theo việc nó làm** (skill *dataviz*):

| Việc | Quy tắc | Áp dụng |
|---|---|---|
| Trạng thái | bảng màu status cố định + nhãn + ký hiệu | Thông thoáng `#0ca30c` ●, Chậm `#fab219` ▲, Tắc nghẽn `#d03b3b` ■, không có số đo: xám nét đứt ○ |
| Độ lớn | **một** màu nhạt → đậm, không dùng cầu vồng | heatmap xanh dương, đậm = chậm |
| Phân biệt chuỗi | categorical theo thứ tự cố định | thực tế xanh `#3987e5`, dự báo cam `#d95926` nét đứt |

Màu được **đo bằng script** `validate_palette.js`, không chọn bằng mắt. Bộ màu cũ **FAIL**: vàng ↔ xanh lá có ΔE 4,2 với người mù màu đỏ-lục, đỏ ↔ cam có ΔE 10,4 với mắt thường. Bộ mới **PASS**: ΔE 11,3 với người mù màu và 27,6 với mắt thường. Các nguyên tắc khác: một trục y mỗi biểu đồ, nét 2px, chữ dùng màu chữ, có tooltip khi rê chuột, có legend khi ≥ 2 chuỗi.

### 7.2 Thiết kế

| Thư viện | Vai trò |
|---|---|
| React 19 + TypeScript + Vite | khung ứng dụng |
| TanStack Query | gọi API, tự làm mới 10 giây |
| react-leaflet + OpenStreetMap | bản đồ (đảo màu thành nền tối bằng CSS) |
| ECharts | đường, cột, heatmap |
| nginx | phục vụ bản build, chuyển `/api` sang `serving-api` |

```
┌──────────────────────────────────────────────────────────────────────┐
│ NYC Traffic & Weather   [DEMO]  Giờ dữ liệu 07:58   [Tìm đoạn…] [Quận]│
├───────────┬──────────────────────────────────────────┬───────────────┤
│ KPI ×4    │                                          │ Thời tiết     │
│ Trạng thái│          BẢN ĐỒ (Leaflet)                │ 16 ô ECMWF    │
│ Theo quận │  đường tô màu trạng thái, lớp thời tiết  │ Top chậm nhất │
├───────────┴───────────────┬──────────────────────────┴───────────────┤
│ Chi tiết đoạn / gợi ý      │ 24h + dự báo │ Heatmap thứ × giờ          │
└────────────────────────────┴──────────────┴───────────────────────────┘
```

| File (`serving/web/src/`) | Làm gì |
|---|---|
| `api.ts` | kiểu dữ liệu + hook gọi từng endpoint |
| `format.ts` | bảng `STATE` (màu, nhãn, ký hiệu), thang `SEQ_BLUE`, giờ New York, mã thời tiết WMO |
| `components/TrafficMap.tsx` | vẽ đường/điểm, tự zoom, bay tới đoạn được chọn, làm mờ đoạn khác, lớp thời tiết |
| `components/LinkSearch.tsx` | ô tìm đoạn đường (gõ để lọc, ↑/↓/Enter) |
| `components/Charts.tsx` | 24h + dự báo (dải tin cậy), toàn mạng vs lịch sử, heatmap, cột theo quận |
| `App.tsx` | ghép bố cục, lọc theo quận, đoạn đang chọn |

**Tương tác:**

- Chọn đoạn bằng một trong ba cách: bấm trên bản đồ, chọn trong top chậm, hoặc gõ ở ô tìm. Bản đồ zoom tới đoạn, viền sáng, các đoạn khác mờ đi.
- Lọc theo quận: mọi panel cùng lọc.
- Bật/tắt lớp thời tiết.
- Màn hình hẹp hơn 1100px chuyển sang một cột.

Chạy ở chế độ phát triển (sửa code là thấy ngay):

```bash
npm --prefix serving/web install
npm --prefix serving/web run dev      # http://localhost:5173, /api chuyển sang cổng 8000
cd serving/web && npx tsc -b && npx eslint src   # kiểm tra trước khi commit
```

---

## 8. Monitoring

### 8.1 Lý thuyết

**Metrics, logs và traces:** metrics là con số theo thời gian, rẻ, hợp vẽ biểu đồ và cảnh báo (**đề tài dùng loại này**); logs là sự kiện chi tiết để điều tra; traces là đường đi của một request qua nhiều dịch vụ.

| Kiểu chỉ số | Ý nghĩa | Ví dụ |
|---|---|---|
| Counter | chỉ tăng, xem qua `rate()` | `serving_http_requests_total`, offset Kafka |
| Gauge | lên xuống | số đoạn tắc, worker còn sống |
| Histogram | đếm theo khoảng → tính phân vị | `serving_http_request_duration_seconds` → p95 |

**Nhãn (label)** chia chỉ số theo chiều (`route`, `topic`, `traffic_state`). Không gắn nhãn có vô số giá trị như `link_id` vì sẽ bùng số chuỗi; API dùng **mẫu đường dẫn** `/api/links/{link_id}` làm nhãn.

**Mô hình kéo (pull):** Prometheus **tự đi đọc** `/metrics` của từng dịch vụ (15 giây/lần) và lưu dạng chuỗi thời gian (15 ngày). Dịch vụ không tự xuất số đo thì dùng **exporter** (`kafka-exporter`). Grafana đọc Prometheus bằng **PromQL**.

| Phương pháp | Cho | Ba câu hỏi |
|---|---|---|
| **RED** | dịch vụ phục vụ request (Serving API) | Rate: bao nhiêu request/giây? Errors: bao nhiêu % lỗi? Duration: mất bao lâu (p50/p95)? |
| **USE** | tài nguyên (Spark worker, Kafka) | Utilization: dùng bao nhiêu %? Saturation: có dồn hàng không? Errors: có hỏng không? |

Với hệ thống dữ liệu còn câu hỏi thứ ba: **dữ liệu có tươi không** (data freshness). API có thể trả lời nhanh và không lỗi, nhưng nếu speed layer đã dừng thì người dùng đang xem số liệu cũ mà không biết.

**Phân vị thay vì trung bình:** trung bình che mất request chậm. p95 = 1 giây nghĩa là 5% người dùng chờ hơn 1 giây. Công thức: `histogram_quantile(0.95, sum by (le) (rate(..._bucket[5m])))`.

### 8.2 Kiến trúc và cấu hình

```
Kafka ──▶ kafka-exporter ──┐
Spark master/worker/driver ─┼──▶ Prometheus (15s, 15 ngày) ──▶ Grafana (tự nạp 3 dashboard)
Serving API /metrics ───────┘
```

| Thành phần | Cấu hình chính | Cổng |
|---|---|---|
| kafka-exporter | `--kafka.server=kafka:29092`, `--kafka.version=3.6.0` (bắt buộc với Kafka 4.x) | 9308 |
| Spark | `conf/spark-defaults.conf`: `PrometheusServlet` (master `/metrics/master/prometheus`, applications `/metrics/applications/prometheus`, worker/driver `/metrics/prometheus`), `spark.ui.prometheus.enabled` (executor `/metrics/executors/prometheus`), `spark.sql.streaming.metricsEnabled`, `JvmSource`, `spark.metrics.namespace=${spark.app.name}` | 8090, 8091/8092, 4040 (job batch), 4041 (speed layer) |
| Prometheus | `--storage.tsdb.retention.time=15d`, `--web.enable-lifecycle`; **mount cả thư mục** `./conf/prometheus` | 9090 |
| Grafana | provisioning datasource (uid `prometheus`) + provider dashboard folder "Monitoring", quét 30 giây | 3001 |

Target trong `prometheus.yml` dùng **tên service và cổng bên trong container** (`spark-master:8080`), không dùng cổng đã map ra máy. Driver chỉ mở cổng khi có job; endpoint driver phải có dấu `/` cuối (`/metrics/prometheus/`), thiếu là bị redirect 302. Nạp lại cấu hình không cần restart: `curl -X POST localhost:9090/-/reload`.

### 8.3 Dashboard Kafka và Spark

**Kafka - Traffic** (8 panel, có biến chọn `topic`):

| Chỉ số | Hỏi câu gì | Khi nào cần lo |
|---|---|---|
| Brokers | Kafka có đang sống? | khác 1 |
| Topics | Đủ topic? | thiếu `traffic_raw` / `weather_raw` |
| Under-replicated partitions | Partition thiếu bản sao? | > 0 |
| Messages/s (tổng và theo topic) | Producer có đang gửi? | về 0 khi đáng lẽ đang chạy |
| Số message đang lưu | Kafka giữ bao nhiêu? | tăng mãi, có thể đầy ổ |
| Consumer group lag | Còn bao nhiêu chưa ai đọc? | tăng liên tục |
| Offset theo partition | Dữ liệu chia đều? | lệch nhiều |

**Spark - Traffic** (13 panel, 3 nhóm):

| Nhóm | Chỉ số | Khi nào cần lo |
|---|---|---|
| Cụm | worker còn sống; app đang chạy/chờ; core và RAM mỗi worker | worker < 2; app chờ lâu = hết core/RAM |
| Executor | task chạy/xong/lỗi; bộ nhớ; tỉ lệ GC; đọc vào/shuffle | có task lỗi; GC > 10% = thiếu bộ nhớ |
| Streaming | tốc độ vào so với tốc độ xử lý; thời gian mỗi lô | xử lý chậm hơn dữ liệu vào kéo dài; lô lâu hơn trigger |

Lưu ý khi đọc:

- **Consumer group lag không thấy job Spark streaming**: Spark lưu offset trong checkpoint, không commit vào Kafka. Muốn biết stream có tụt lại không thì xem "input rate vs processing rate".
- **Executor và Streaming chỉ có số liệu khi có job đang chạy**: không có job thì trống là bình thường.
- **"Input" trong biểu đồ dữ liệu đọc vào luôn bằng 0 với job đọc Kafka**: Spark chỉ đếm byte đọc từ file.

### 8.4 Dashboard Serving (nối giao diện với monitoring)

Code `serving/api/metrics.py` (thư viện `prometheus-client`):

- **Middleware** đếm `serving_http_requests_total{method, route, status}` và histogram `serving_http_request_duration_seconds{route}` (11 bucket, 10 ms – 30 giây).
- **Custom collector** tính số đo ngay lúc Prometheus kéo, dùng lại cache 5 giây của API nên không tạo thêm truy vấn: `serving_source_up`, `serving_source_info{source}`, `serving_links_by_state{traffic_state}`, `serving_network_avg_speed_mph`, `serving_data_clock_seconds`, `serving_data_stall_seconds`, `serving_source_errors_total`.

Dashboard **"Serving - Traffic"** (`conf/grafana/dashboards/serving.json`, 13 panel):

| Nhóm | Panel | Chỉ số / PromQL | Khi nào cần lo |
|---|---|---|---|
| Dữ liệu người dùng thấy | Nguồn dữ liệu đọc được | `serving_source_up` | = 0 |
| | Đồng hồ dữ liệu | `serving_data_clock_seconds` | |
| | Dữ liệu đứng yên | `serving_data_stall_seconds` | > 2 phút vàng, > 10 phút đỏ |
| | Tốc độ TB, Đoạn tắc nghẽn | `serving_network_avg_speed_mph`, `serving_links_by_state{traffic_state="CONGESTED"}` | tắc tăng vọt |
| | Số đoạn theo trạng thái | `serving_links_by_state` (cùng màu với dashboard người dùng) | `NO_DATA`/`STALE` tăng |
| RED | Request/giây theo endpoint | `sum by (route) (rate(serving_http_requests_total[1m]))` | |
| | Tỉ lệ lỗi 5xx | `100 * (sum(rate(...{status=~"5.."}[5m])) or vector(0)) / sum(rate(...[5m]))` | > 0 |
| | Độ trễ p50/p95 | `histogram_quantile(0.5/0.95, ...)` | p95 > 2 giây |
| | Endpoint chậm nhất | `topk(8, histogram_quantile(0.95, ... by (le, route)))` | |
| | Lỗi đọc nguồn; Prometheus đọc được API | `serving_source_errors_total`; `up{job="serving-api"}` | lỗi > 0; DOWN |

### 8.5 Thực hành

```bash
curl -s localhost:8000/metrics | grep ^serving_
```

Thử trên Prometheus (http://localhost:9090/graph):

```promql
kafka_brokers
metrics_master_aliveWorkers_Value
serving_links_by_state
histogram_quantile(0.95, sum by (le) (rate(serving_http_request_duration_seconds_bucket[5m])))
```

Thí nghiệm:

1. **Dữ liệu ngừng chảy**: `docker stop traffic-serving-demo-replay`. Sau 2 phút ô "Dữ liệu đứng yên" chuyển vàng, sau 10 phút đỏ; trên dashboard người dùng, các đoạn dần chuyển `STALE`.
2. **Nguồn dữ liệu chết** (đã chạy thử): `docker stop traffic-serving-clickhouse`. Sau ≤ 5 giây `serving_source_up=0`, API trả 500, tỉ lệ lỗi tăng.
3. Khôi phục: `docker start traffic-serving-clickhouse`, chờ vài giây rồi `docker start traffic-serving-demo-replay` (replay tự thoát khi mất ClickHouse; khởi động lại thì phát lại từ đầu).

Sửa dashboard: sửa trên Grafana (đã bật `allowUiUpdates`). Muốn giữ lâu dài thì vào **Share → Export → Save to file**, ghi đè file trong `conf/grafana/dashboards/` và giữ nguyên `uid`.

---

## 9. Tích hợp các phần

```
                     ┌─────────────────────── docker network: traffic-net ───────────────────────┐
 data/raw ──(demo)──▶│ serving-demo-load / replay ─▶ serving-clickhouse ─┐                         │
                     │                                                   ├─▶ serving-api :8000 ─┐  │
 Kafka→Spark→Iceberg │ trino :8080 (view lakehouse.serving.*) ──(thật)───┘     │ /metrics      │  │
                     │                                                         ▼               ▼  │
                     │ kafka ─▶ kafka-exporter ─▶ prometheus :9090 ◀── spark-master/worker   serving-web (nginx)
                     │                              │                                   :3000 ─┘  │
                     │                              ▼                                             │
                     │                           grafana :3001                                    │
                     └────────────────────────────────────────────────────────────────────────────┘
```

| Điểm nối | Cách nối | File |
|---|---|---|
| Dữ liệu → API | `SERVING_SOURCE=trino|demo`; Trino `trino:8080`, ClickHouse `serving-clickhouse:8123` | `docker-compose.yml` |
| API → dashboard | nginx chuyển `/api/*` sang `serving-api:8000`, phân giải tên lúc request (`resolver 127.0.0.11`) | `serving/web/nginx.conf` |
| API → Prometheus | job `serving-api` đọc `serving-api:8000/metrics` | `conf/prometheus/prometheus.yml` |
| Prometheus → Grafana | datasource `prometheus` + thư mục dashboard tự nạp | `conf/grafana/provisioning/` |
| Trạng thái giao thông | một quy tắc ở Spark, view Trino, `rules.py`, màu trên web và Grafana | `serving/api/rules.py`, `serving/web/src/format.ts`, `serving.json` |

**Docker Compose profile:**

| Profile | Bật | Dùng khi |
|---|---|---|
| *(không)* | Kafka, Spark, Prometheus, Grafana, kafka-exporter, kafka-ui | monitoring |
| `demo` | `serving-clickhouse`, `serving-demo-replay`, `serving-api`, `serving-web` | chạy giao diện không cần lakehouse |
| `demo-load` | `serving-demo-load` (chạy 1 lần) | nạp dữ liệu nền |
| `serving` (trong repo đầy đủ) | `serving-api` đọc Trino + `serving-web` | hệ thống thật |

**Chạy với hệ thống thật** (trong repo TrafficSP đầy đủ, khi nhóm có đủ stack):

1. Merge speed layer và `jobs/serving/trino_views.sql` của Khải; chạy batch và speed layer.
2. Tạo view: `docker exec -i traffic-trino trino --catalog lakehouse < jobs/serving/trino_views.sql`.
3. `docker compose --profile serving up -d --build serving-api serving-web`.
4. Dashboard hiện nhãn **TRINO**; Grafana "Serving" cho biết Trino trả lời nhanh hay chậm và speed layer có còn chảy dữ liệu không.

---

## 10. Xử lý lỗi

| Hiện tượng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| `bash: run_demo.sh: No such file or directory` | Terminal không đứng trong thư mục này | `cd ~/Downloads/TrafficSP-Mai` rồi chạy lại |
| Container tắt với mã **137** | Docker hết RAM | Tăng RAM Docker, đóng bớt ứng dụng; script tổng đã dùng 1 core Spark |
| "container name already in use" | Đang chạy repo TrafficSP đầy đủ | Tắt bên kia, hoặc `docker rm -f <tên>` |
| Dashboard trống, API trả 503 "Chưa có dữ liệu realtime" | Replay chưa chạy hoặc đã dừng | `docker logs traffic-serving-demo-replay`; chạy lại `bash run_demo.sh up` |
| Trang trống 10–20 giây đầu | Đang khởi động (Grafana, nạp dữ liệu) | Đợi rồi tải lại |
| Bản đồ không có nền | Không có Internet (tile OpenStreetMap) | Dữ liệu vẫn hiển thị trên nền tối |
| Grafana chưa thấy dashboard | Grafana chưa quét lại thư mục (30 giây) | Đợi hoặc `docker restart traffic-grafana` |
| Target `serving-api` down | API chưa bật, hoặc Prometheus chưa nạp cấu hình mới | `curl -X POST localhost:9090/-/reload` |
| Target `spark-driver` down | Job Spark demo chưa lên hoặc đã hết giờ | Đợi ~15 giây, hoặc chạy lại `bash run_demo.sh up` |
| Target `spark-driver` lỗi 302 | Thiếu dấu `/` cuối ở `metrics_path` | `/metrics/prometheus/` |
| "airflow-scheduler đang chạy" | Trùng tên mạng với job Spark demo | `docker compose stop airflow-scheduler` |
| Dashboard Spark trống phần master/worker | Chưa restart Spark sau khi sửa `spark-defaults.conf` | `docker compose restart spark-master spark-worker-1 spark-worker-2` |
| Muốn nạp lại dữ liệu demo từ đầu | | `docker compose --profile demo down -v` rồi `bash run_demo.sh up` |

---

## 11. Lịch sử commit và việc còn lại

Code lấy từ repo https://github.com/danh041105/TrafficSP, nhánh `mai-monitoring` (bản trên máy).

| Commit | Ngày | Nội dung |
|---|---|---|
| `5ea5bc5` | 19/11 | Monitoring: kafka-exporter, Prometheus, dashboard Kafka/Spark, demo |
| `75244e6` | 19/11 | Merge `dev` |
| `0e4b8e5` | 22/11 | Merge `dev` (silver→gold, DAG); giải quyết xung đột README |
| `b2a52df` | 22/11 | Serving Layer: FastAPI (Trino + demo), dashboard React, compose profile |
| `413f17a` | 22/11 | `/metrics` cho API, dashboard Grafana Serving, demo tổng, tài liệu |
| `b210e76` | 22/11 | Replay chỉ đọc tháng cần phát (khởi động nhanh) |

| Việc còn lại | Phụ thuộc |
|---|---|
| Chạy chế độ Trino trên lakehouse thật | Nhánh của Khải được merge; máy đủ RAM |
| Hiển thị dự báo thật | Phần MLOps tạo view `serving.v_traffic_forecast` (mục 6.3) |
| Thống nhất bảng Gold giữa `dev` và view của Khải | Trưởng nhóm, Bách, Khải |
| Cảnh báo tự động (`serving_data_stall_seconds > 600`, `serving_source_up == 0`, worker chết) | Grafana Alerting |
| Theo dõi thêm Trino, MinIO, Airflow, CPU/RAM từng container | |

---

