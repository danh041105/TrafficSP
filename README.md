# Hướng dẫn cấu hình biến môi trường để chạy hệ thống
## 1. Tạo file `.env`

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

## 2. Sinh `AIRFLOW_FERNET_KEY` và `AIRFLOW_JWT_SECRET`

```bash
# AIRFLOW_FERNET_KEY: 32 byte ngẫu nhiên, mã hoá base64 url-safe
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"

# AIRFLOW_JWT_SECRET
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Dán kết quả vào `.env`, không có dấu nháy.

## 4. File license MinIO

Image `quay.io/minio/aistor/minio` cần file license. Đặt file `minio.license` ở thư mục gốc, cùng cấp với `docker-compose.yml`.
Cách tạo minio.license thì lên web minio --> chọn download --> chọn request lincense key --> và chọn mục free 

## 6. Kiểm tra cấu hình và chạy

Kiểm tra xem các biến đã được đọc đủ chưa:

```bash
docker compose config
```

Nếu có biến chưa khai báo trong `.env`, Compose sẽ cảnh báo `The "XXX" variable is not set`.

Build image và chạy:

```bash
docker compose up -d --build
```
