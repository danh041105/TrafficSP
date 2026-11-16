# Hướng dẫn cấu hình biến môi trường để chạy hệ thống

## Chạy Silver cục bộ để làm Gold bằng Spark SQL

Mở PowerShell 7 tại thư mục TrafficSP. Bật Docker Desktop ở chế độ Linux containers.
Luồng này dùng PostgreSQL làm catalog Iceberg, MinIO lưu bảng và checkpoint, Kafka nhận dữ liệu raw. Spark chạy trong container tạm với `local[2]`.

Trong `.env`, đặt `POSTGRES_USER`, `POSTGRES_PASSWORD` và `MINIO_ROOT_USER` thành `admin` cho môi trường local mới. Mật khẩu MinIO cần ít nhất 8 ký tự. `ICEBERG_DB_PASSWORD` là mật khẩu cho role `iceberg` và được truyền cho Spark từ `.env`. Với volume PostgreSQL đã khởi tạo, dùng thông tin đăng nhập của volume đó. Đặt `minio.license` ở thư mục gốc.

### 1. Khởi động dịch vụ

```powershell
docker compose config --quiet
docker compose build spark-master
docker compose up -d --wait postgresql minio kafka
docker compose run --rm --no-deps -T minio-init
docker compose run --rm --no-deps -T kafka-init
```

`minio-init` tạo bucket `lakehouse`. `kafka-init` tạo hai topic `traffic_raw` và `weather_raw`. Khi chạy trên kho mới, Spark tự tạo checkpoint trong `lakehouse/checkpoints/batch_layer/`. Nếu tiếp tục từ một kho cũ, kiểm tra checkpoint hiện có trước khi ingest. [Tài liệu dự án về checkpoint](https://docs.google.com/document/d/1Im_OuwxiucTJhpJsN64OjMwMqHvl7IKBKPcixj8hvxg/edit?tab=t.5ixzzo6ibqj9)

### 2. Nạp mẫu và xây Silver

```powershell
function Invoke-TrafficSpark {
    $launch = 'tool="$1"; shift; exec "/opt/spark/bin/$tool" --master "local[2]" --driver-memory 3g --conf spark.sql.session.timeZone=UTC --conf spark.sql.shuffle.partitions=16 --conf "spark.sql.catalog.lakehouse.jdbc.password=$ICEBERG_DB_PASSWORD" "$@"'
    docker compose run --rm --no-deps spark-master sh -ec $launch spark @args
}

Invoke-TrafficSpark spark-submit /opt/spark/jobs/ddl/bronze.py
Invoke-TrafficSpark spark-submit /opt/spark/jobs/ddl/silver.py

$env:PYTHONPATH = 'jobs'
$env:KAFKA_BOOTSTRAP = 'localhost:9092'
& .\.venv\Scripts\python.exe -X utf8 -m scripts.replay_producer --phase test

Invoke-TrafficSpark spark-submit /opt/spark/jobs/batch/ingestion/kafka_to_bronze.py
Invoke-TrafficSpark spark-submit /opt/spark/jobs/batch/transformation/bronze_to_silver.py --start-date 2025-01-01
```

Producer chạy trên Windows và dùng `pyarrow` cùng `confluent-kafka` trong `.venv`. `test` lấy một ngày traffic. Weather của mẫu chỉ phủ ngày 01/01 theo UTC, nên traffic cuối ngày New York có thể thiếu weather khi ghép.

Để nạp lịch sử từ tháng 1 đến tháng 9 trên kho mới, thay `--phase test` bằng `--phase gd1` và thêm `--end-date 2025-09-30` vào lệnh Silver. Nếu đã nạp `test` rồi chạy `gd1` trên cùng kho, Bronze sẽ nhận lại các bản tin mẫu với offset mới. Silver lọc trùng theo khóa và bảng audit ghi thêm một lượt chạy. Khi Kafka đã nhận dữ liệu cần xử lý, tiếp tục từ lệnh Bronze hoặc Silver tương ứng để tránh phát lại.

### 3. Kiểm tra Silver và mở Spark SQL

```powershell
docker compose stop -t 60 kafka
docker compose up -d --wait postgresql minio
Invoke-TrafficSpark spark-sql
```

```sql
SHOW TABLES IN lakehouse.silver;
SELECT COUNT(*) FROM lakehouse.silver.traffic_cleaned;
SELECT COUNT(*) FROM lakehouse.silver.weather_cleaned;
SELECT COUNT(*) FROM lakehouse.silver.link_info;

SELECT t.link_id, t.traffic_utc, t.speed, l.cell_id, w.temperature_2m
FROM lakehouse.silver.traffic_cleaned AS t
LEFT JOIN lakehouse.silver.link_info AS l ON t.link_id = l.link_id
LEFT JOIN lakehouse.silver.weather_cleaned AS w
  ON l.cell_id = w.cell_id AND date_trunc('hour', t.traffic_utc) = w.weather_utc
WHERE t.date = DATE '2025-01-01'
LIMIT 10;
```

Giữ PostgreSQL và MinIO trong lúc làm Gold. Dùng `CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;` khi bắt đầu tạo bảng. Có thể lưu SQL trong `jobs/` và chạy `Invoke-TrafficSpark spark-sql -f /opt/spark/jobs/<ten-file>.sql`.

## Cấu hình và chạy toàn bộ stack

### 1. Tạo file `.env`

Tạo file `.env` ở thư mục gốc (cùng cấp với `docker-compose.yml`) rồi điền giá trị của bạn:

```dotenv
# cấu hình đăng nhập minio
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=doi_mat_khau_nay

# cấu hình đăng nhập grafana
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=doi_mat_khau_nay

# cấu hình đăng nhập clickhouse
CLICKHOUSE_USER=admin
CLICKHOUSE_PASSWORD=doi_mat_khau_nay

# cấu hình đăng nhập postgres_db cho airflow và iceberg catalog
POSTGRES_USER=postgres
POSTGRES_PASSWORD=doi_mat_khau_nay
AIRFLOW_DB_PASSWORD=doi_mat_khau_nay
ICEBERG_DB_PASSWORD=iceberg

# cấu hình airflow
AIRFLOW_FERNET_KEY=<sinh theo mục 2>
AIRFLOW_JWT_SECRET=<sinh theo mục 2>
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=doi_mat_khau_nay
AIRFLOW_UID=50000
```

### 2. Sinh `AIRFLOW_FERNET_KEY` và `AIRFLOW_JWT_SECRET`

```bash
# AIRFLOW_FERNET_KEY: 32 byte ngẫu nhiên, mã hoá base64 url-safe
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"

# AIRFLOW_JWT_SECRET
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Dán kết quả vào `.env`, không có dấu nháy.

### 4. File license MinIO

Image `quay.io/minio/aistor/minio` cần file license. Đặt file `minio.license` ở thư mục gốc, cùng cấp với `docker-compose.yml`.
Cách tạo minio.license thì lên web minio --> chọn download --> chọn request lincense key --> và chọn mục free 

### 6. Kiểm tra cấu hình và chạy

Kiểm tra xem các biến đã được đọc đủ chưa:

```bash
docker compose config
```

Nếu có biến chưa khai báo trong `.env`, Compose sẽ cảnh báo `The "XXX" variable is not set`.

Build image và chạy:

```bash
docker compose up -d --build
```
