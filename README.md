# TrafficSP — chạy local và kết nối SQL

## Kết nối DBeaver sau khi clone hoặc git pull

Hướng dẫn này dùng Docker và SQL client trên cùng một máy; các lệnh terminal chạy trong **PowerShell 7** tại thư mục gốc repo. DBeaver Desktop và CloudBeaver đều truy vấn các bảng Iceberg thông qua Trino. PostgreSQL giữ catalog metadata, MinIO giữ file dữ liệu của bảng.

### 1. Chuẩn bị repo và môi trường local

Cài Git, Docker Desktop ở chế độ Linux containers và PowerShell 7. Nếu dùng DBeaver Desktop, cài [DBeaver Community](https://dbeaver.io/download/). Cần Internet để tải Docker images, Spark dependencies và JDBC driver lần đầu.

Nếu chưa có repo:

```powershell
git clone https://github.com/danh041105/TrafficSP.git
cd TrafficSP
```

Nếu đã clone, vào thư mục repo rồi `git pull` từ nhánh team đang sử dụng. Nhánh đó cần có `.env.example`, `jobs/ddl/gold.sql` và hai file cấu hình CloudBeaver `conf/cloudbeaver/data-sources.json`, `scripts/configure_cloudbeaver.ps1` được nhắc bên dưới.

Tạo `.env` nếu máy chưa có:

```powershell
if (-not (Test-Path -LiteralPath .env)) {
    Copy-Item -LiteralPath .env.example -Destination .env
}
```

Mở `.env` bằng editor và thay mọi giá trị `<...>` bằng mật khẩu riêng. Mật khẩu MinIO cần ít nhất 8 ký tự; mật khẩu CloudBeaver nên có chữ hoa, chữ thường và chữ số. `AIRFLOW_DB_PASSWORD` vẫn cần điền vì init PostgreSQL tạo role Airflow cùng role Iceberg, dù chỉ chạy các service SQL. `AIRFLOW_FERNET_KEY` và `AIRFLOW_JWT_SECRET` có thể để trống khi chưa chạy Airflow.

Đặt file **`minio.license` hợp lệ do MinIO cấp** ở thư mục gốc, cùng cấp `docker-compose.yml`. Kiểm tra file và cú pháp Compose:

```powershell
Test-Path -LiteralPath .\minio.license -PathType Leaf
docker compose config --quiet
```

Lệnh kiểm tra license phải trả `True`; Compose không được báo thiếu biến. `.env`, license và dữ liệu trong Docker volumes không được mang theo khi clone/pull. Máy mới dùng kho local riêng, không tự có dữ liệu Silver của máy người khác. Với volume cũ, dùng đúng thông tin đăng nhập đã khởi tạo volume; sửa `.env` không tự đổi mật khẩu tài khoản đã lưu trong PostgreSQL hoặc CloudBeaver.

### 2. Khởi tạo lakehouse trên máy mới

Để truy vấn chỉ cần PostgreSQL, MinIO và Trino. Spark dùng để tạo bảng; không cần bật Spark workers, Kafka, Airflow hoặc Grafana cho bước kết nối SQL này.

```powershell
docker compose build spark-master
docker compose up -d --wait --wait-timeout 240 postgresql minio
docker compose run --rm --no-deps -T minio-init

function Invoke-TrafficSpark {
    $launch = 'tool="$1"; shift; exec "/opt/spark/bin/$tool" --master "local[2]" --driver-memory 3g --conf spark.sql.session.timeZone=UTC --conf spark.sql.shuffle.partitions=16 --conf "spark.sql.catalog.lakehouse.jdbc.password=$ICEBERG_DB_PASSWORD" "$@"'
    docker compose run --rm --no-deps -T spark-master sh -ec $launch spark @args
}

Invoke-TrafficSpark spark-submit /opt/spark/jobs/ddl/bronze.py
Invoke-TrafficSpark spark-submit /opt/spark/jobs/ddl/silver.py
Invoke-TrafficSpark spark-submit /opt/spark/jobs/ddl/gold.py

docker compose up -d --wait --wait-timeout 240 trino
docker compose ps
```

`minio-init` tạo bucket `lakehouse`. Ba job DDL tạo catalog objects, schemas và các bảng còn thiếu, không nạp dữ liệu. PostgreSQL, MinIO và Trino phải healthy trước khi kết nối. Init dùng `CREATE IF NOT EXISTS`; nếu kho cũ đã có bảng thì schema không tự được sửa theo file mới. Ví dụ, bảng Gold có grid key INT từ bản trước cần migration riêng sang STRING trước khi load.

### 3. Kết nối DBeaver Desktop

Mở **New Database Connection**, tìm driver **Trino**, chọn **Next**. Chọn **Connect by: URL** rồi nhập:

| Trường | Giá trị |
|---|---|
| JDBC URL | `jdbc:trino://localhost:8088/lakehouse/gold` |
| Authentication | Username/password (Database Native) |
| Username | `traffic_sql` |
| Password | Để trống |
| Connection name | `TrafficSP Local (Trino)` |

URL trên tương ứng host `localhost`, port `8088`, catalog `lakehouse`, schema mặc định `gold`. Có thể đổi phần cuối URL thành `silver` để dùng Silver mặc định.

Trong **Driver properties**, có thể đặt `timezone=UTC` để phiên SQL dùng UTC. Chọn **Test Connection**; nếu DBeaver yêu cầu tải driver, chọn **Download**. Driver JDBC `484` khớp image Trino của repo; driver class là `io.trino.jdbc.TrinoDriver` nếu phải cấu hình thủ công. Sau khi test thành công, chọn **Finish**.

Không thêm `?user=traffic_sql` vào JDBC URL khi đã điền ô Username. Trino không cho khai báo cùng một connection parameter trong cả URL và properties. Xem [DBeaver Trino connection settings](https://dbeaver.com/docs/dbeaver/Database-driver-Trino/) và [Trino JDBC driver](https://trino.io/docs/current/client/jdbc.html).

### 4. Dùng CloudBeaver qua trình duyệt (tùy chọn)

Nếu chỉ dùng DBeaver Desktop, bước 3 đã đủ. Để dùng giao diện web của repo:

```powershell
docker compose up -d --wait --wait-timeout 240 cloudbeaver
```

Mở [CloudBeaver local](http://localhost:8978), chờ web sẵn sàng rồi cấu hình datasource một lần:

```powershell
.\scripts\configure_cloudbeaver.ps1
```

Script merge kết nối Trino vào workspace, sao lưu cấu hình cũ và restart riêng CloudBeaver. Chờ web lên lại, refresh trang và đăng nhập:

| Bước đăng nhập | Username | Password |
|---|---|---|
| Giao diện CloudBeaver | `CLOUDBEAVER_ADMIN_USER` trong `.env` (mặc định `cbadmin`) | `CLOUDBEAVER_ADMIN_PASSWORD` của chính máy đó |
| Database Authentication của **Traffic Lakehouse (Trino)** | `traffic_sql` | Để trống |

Server mới được cấu hình tự động từ `.env`; workspace đã khởi tạo dùng tài khoản hiện có. Không dùng tài khoản admin CloudBeaver làm database user Trino.

Hai SQL client dùng địa chỉ khác nhau vì vị trí chạy:

| Client | Địa chỉ Trino | JDBC URL |
|---|---|---|
| DBeaver Desktop trên host | `localhost:8088` | `jdbc:trino://localhost:8088/lakehouse/gold` |
| CloudBeaver trong Docker network | `trino:8080` | `jdbc:trino://trino:8080/lakehouse/gold` |

`8978` là cổng giao diện web CloudBeaver. Các cổng này đang bind loopback, phục vụ truy cập trên máy chạy Docker.

### 5. Mở SQL Editor và kiểm tra

Chọn kết nối vừa tạo, mở **SQL Editor → New SQL Script**, chạy lần lượt:

```sql
SELECT version();
SHOW CATALOGS;
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.bronze;
SHOW TABLES FROM lakehouse.silver;
SHOW TABLES FROM lakehouse.gold;

SELECT COUNT(*) AS traffic_rows FROM lakehouse.silver.traffic_cleaned;
SELECT COUNT(*) AS gold_rows FROM lakehouse.gold.fact_traffic_reading;
```

Máy mới chỉ chạy DDL sẽ có các bảng rỗng, COUNT trả `0` là bình thường. Muốn nạp dữ liệu đến Silver, làm tiếp phần [Nạp mẫu và xây Silver](#2-nạp-mẫu-và-xây-silver) bên dưới, bao gồm chuẩn bị dữ liệu nguồn và môi trường producer. Job nạp Silver → Gold nằm ở `jobs/batch/transformation/silver_to_gold.py`; xem phần hướng dẫn load Gold bên dưới.

Trino catalog đang đặt `iceberg.security=read_only`: dùng DBeaver/CloudBeaver để SELECT và xem bảng; chạy DDL, INSERT hoặc MERGE lakehouse bằng Spark SQL.

### 6. Lỗi thường gặp và chạy lại sau git pull

| Triệu chứng | Kiểm tra / xử lý |
|---|---|
| Connection refused / không mở được web | Kiểm tra Docker đang chạy và `docker compose ps`. DBeaver Desktop dùng 8088; CloudBeaver web dùng 8978. Chờ Trino healthy và CloudBeaver web sẵn sàng. |
| `Connection property user is both in the URL and an argument` | Bỏ `?user=...` khỏi URL; chỉ dùng ô Username hoặc native auth properties. Sau khi sửa datasource CloudBeaver, refresh/reconnect. |
| Không resolve được host `trino` trong DBeaver Desktop | Trên host dùng `localhost:8088`; tên `trino:8080` dành cho Docker network. |
| Không thấy `lakehouse` / namespace / table | Kiểm tra catalog mount trong `conf/trino/catalog`, PostgreSQL/MinIO và đã chạy ba job DDL ở bước 2. Máy mới không có dữ liệu trong volumes của máy người khác. |
| MinIO unhealthy hoặc init bucket thất bại | Kiểm tra `minio.license` là file hợp lệ và `docker compose logs --tail=60 minio`. |
| DB password sai dù `.env` đã đổi | Với volume cũ, mật khẩu role PostgreSQL vẫn là mật khẩu lúc khởi tạo. Dùng credentials của kho đó; không xóa volume để xử lý lỗi đăng nhập. |
| Admin CloudBeaver không đăng nhập được | Dùng đúng admin của workspace và mật khẩu của máy đó. Biến CB_ADMIN_* chỉ cấu hình first-run, không reset tài khoản đã tồn tại. |
| Lệnh ghi bị từ chối | Catalog Trino read_only theo thiết kế; dùng Spark để ghi. |

Với máy đã chạy trước đó, sau `git pull` thường chỉ cần:

```powershell
docker compose config --quiet
docker compose up -d --wait --wait-timeout 240 postgresql minio trino
# Thêm cloudbeaver vào lệnh trên nếu dùng giao diện web.
```

Chạy lại `configure_cloudbeaver.ps1` nếu template datasource đã thay đổi. Chỉ build lại Spark khi image/dependencies thay đổi và chỉ chạy DDL cho bảng còn thiếu. Giữ `.env`, license, volumes và checkpoints đang dùng; schema thay đổi cần migration riêng. `git pull` không tự tạo dữ liệu, đổi mật khẩu hay chạy pipeline.

## Chạy Silver cục bộ để làm Gold bằng Spark SQL

Mở PowerShell 7 tại thư mục TrafficSP. Bật Docker Desktop ở chế độ Linux containers.
Luồng này dùng PostgreSQL làm catalog Iceberg, MinIO lưu bảng và checkpoint, Kafka nhận dữ liệu raw. Spark chạy trong container tạm với `local[2]`.

Dùng `.env.example` làm mẫu cho `.env` và điền mật khẩu riêng. `POSTGRES_USER` và `MINIO_ROOT_USER` trong mẫu là `admin`; `ICEBERG_DB_PASSWORD` là mật khẩu cho role `iceberg`, được truyền cho Spark từ `.env`. Với volume PostgreSQL đã khởi tạo, dùng thông tin đăng nhập của volume đó. Đặt `minio.license` hợp lệ ở thư mục gốc.

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

Giữ PostgreSQL và MinIO trong lúc làm Gold. Nếu Silver đã có dữ liệu, tiếp tục ở phần dưới.

## Khởi tạo Gold và viết SQL qua CloudBeaver

Chạy từ PowerShell 7 tại thư mục repo. Dùng hàm `Invoke-TrafficSpark` ở trên và image Spark đã build.

```powershell
docker compose up -d --wait postgresql minio
Invoke-TrafficSpark spark-submit /opt/spark/jobs/ddl/gold.py
```

DDL nằm trong [`jobs/ddl/gold.sql`](jobs/ddl/gold.sql). Có thể chạy trực tiếp bằng `Invoke-TrafficSpark spark-sql -f /opt/spark/jobs/ddl/gold.sql`. Init tạo namespace `lakehouse.gold` và tám bảng Iceberg: năm dimension, ba fact. Các bảng mới vẫn rỗng. Chạy lại chỉ tạo đối tượng còn thiếu, không sửa schema bảng đã có và không nạp dữ liệu Silver.

Contract của `dim_road_link` là SCD 2 với khoảng ngày `[valid_from, valid_to)`, `valid_to IS NULL` cho phiên bản hiện hành. `dim_weather_grid` là SCD 1, `dim_date` là SCD 0. DDL chỉ mô tả các contract này; logic cập nhật SCD và kiểm tra PK/FK cần được thực hiện trong job nạp dữ liệu sau này. Iceberg không tự cưỡng chế PK/FK. Timestamp dùng UTC; múi giờ nghiệp vụ để tạo `date_key`/`hour_key` cần được chốt khi xây pipeline.

Contract key được chốt: `grid_key` giữ nguyên `cell_id` STRING của Silver; `nearest_grid_key` lấy trực tiếp mapping `silver.link_info.cell_id`. `road_link_key` là surrogate số tăng cho mỗi version mới, còn `link_id` được phép lặp qua các version. Khai báo INT trong DDL không tự tăng ID; job nạp phải cấp key và giữ key cũ khi rerun. DDL nguồn hiện dùng STRING cho cả bốn cột grid PK/FK. Các bảng đã tạo với grid key INT từ bản trước cần migration riêng; chạy lại init `CREATE IF NOT EXISTS` không đổi schema và lần cập nhật contract này chưa chạy migration.

### PK/FK Gold và đường nối trong ERD

Trong `gold.sql` hiện tại, PK/FK là **logical contract** ghi bằng column comments. Có `NOT NULL` nhưng chưa có `PRIMARY KEY`, `FOREIGN KEY ... REFERENCES`, `UNIQUE` hoặc Iceberg identifier fields. Comment không tạo constraint hoặc metadata FK để SQL client tự vẽ đường nối.

Project dùng Iceberg 1.10.1, table format v2. Với stack này, không có FK được engine tự kiểm tra như một relational database. Iceberg có thể khai báo key metadata bằng `ALTER TABLE ... SET IDENTIFIER FIELDS`, nhưng không tự đảm bảo uniqueness hoặc liên kết tới bảng cha. Xem [Iceberg 1.10 Spark DDL](https://iceberg.apache.org/docs/1.10.0/spark-ddl/#alter-table--set-identifier-fields) và [identifier fields trong spec 1.10.1](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/format/spec.md#identifier-field-ids). Hiện project chưa đặt identifier fields; phần này không được coi là FK constraint.

Các khóa chính logic:

| Bảng | Logical PK |
|---|---|
| `dim_date` | `date_key` |
| `dim_hour` | `hour_key` |
| `dim_road_link` | `road_link_key` (surrogate mỗi SCD2 version; `link_id` được lặp) |
| `dim_weather_grid` | `grid_key` (giữ nguyên `cell_id`) |
| `dim_weather_condition` | `condition_key` |
| `fact_traffic_reading` | `(road_link_key, reading_ts_utc)` |
| `fact_weather_hourly` | `(grid_key, hour_start_utc)` |
| `fact_congestion_hourly` | `(road_link_key, hour_start_utc)` |

Đủ **13 logical FK** được mô tả trong DDL:

| Bảng con | Cột FK → bảng/cột cha |
|---|---|
| `dim_road_link` | `nearest_grid_key → dim_weather_grid.grid_key` |
| `fact_traffic_reading` | `road_link_key → dim_road_link.road_link_key`; `date_key → dim_date.date_key`; `hour_key → dim_hour.hour_key` |
| `fact_weather_hourly` | `grid_key → dim_weather_grid.grid_key`; `date_key → dim_date.date_key`; `hour_key → dim_hour.hour_key`; `condition_key → dim_weather_condition.condition_key` |
| `fact_congestion_hourly` | `road_link_key → dim_road_link.road_link_key`; `date_key → dim_date.date_key`; `hour_key → dim_hour.hour_key`; `grid_key → dim_weather_grid.grid_key`; `condition_key → dim_weather_condition.condition_key` |

Khi triển khai loader, Spark phải kiểm tra logical PK không trùng, FK khác NULL có bản ghi cha và khoảng hiệu lực SCD2 không chồng nhau; lookup fact theo đúng road version. `NOT NULL` chỉ bắt buộc giá trị, không thay các kiểm tra này. `nearest_grid_key` hiện được phép NULL trong DDL, các cột key khác đều NOT NULL. Loader và các kiểm tra này chưa được code/chạy; bảng rỗng không phải bằng chứng quy tắc đã được thực thi.

SQL JOIN vẫn viết trực tiếp theo key, ví dụ:

```sql
SELECT f.reading_ts_utc, d.link_id, d.link_name, f.speed_mph
FROM lakehouse.gold.fact_traffic_reading f
JOIN lakehouse.gold.dim_road_link d
  ON f.road_link_key = d.road_link_key
LIMIT 20;
```

Để nhìn thấy đường nối trong **DBeaver Desktop**, có thể khai báo [virtual keys](https://dbeaver.com/docs/dbeaver/Virtual-Keys/) hoặc tạo [custom ER diagram](https://dbeaver.com/docs/dbeaver/Custom-Diagrams/) theo bảng quan hệ trên. Các quan hệ đó lưu phía DBeaver, không tạo constraint trong Iceberg và không thay data-quality checks của Spark. Project hiện chưa cấu hình virtual model trong GUI; không coi cột có tên `_key` hoặc comment FK là quan hệ đã được SQL client nhận diện.

### Mở CloudBeaver và quyền ghi lakehouse

Với workspace CloudBeaver mới, thêm `CLOUDBEAVER_ADMIN_USER` và `CLOUDBEAVER_ADMIN_PASSWORD` vào `.env` trước khi khởi động. Tên mặc định là `cbadmin`; dùng mật khẩu riêng đủ mạnh. Giữ thông tin đăng nhập của workspace hiện có khi tiếp tục sử dụng volume cũ.

```powershell
docker compose up -d --wait trino cloudbeaver
```

Mở [CloudBeaver](http://localhost:8978), chờ trang sẵn sàng, rồi chạy một lần:

```powershell
.\scripts\configure_cloudbeaver.ps1
```

Script thêm kết nối **Traffic Lakehouse (Trino)** vào workspace, sao lưu cấu hình cũ và khởi động lại CloudBeaver để nạp kết nối. Các kết nối khác được giữ lại. Đăng nhập bằng tài khoản CloudBeaver trong `.env`, chọn kết nối này và mở SQL Editor. Kết nối dùng địa chỉ nội bộ `trino:8080`, catalog `lakehouse`, schema mặc định `gold`; cổng Trino trên host là [localhost:8088](http://localhost:8088).

```sql
SHOW TABLES FROM lakehouse.gold;
SELECT COUNT(*) AS reading_count FROM lakehouse.gold.fact_traffic_reading;
SELECT link_id, traffic_utc, speed
FROM lakehouse.silver.traffic_cleaned
ORDER BY traffic_utc
LIMIT 20;
```

Trong hộp **Database Authentication**, dùng username `traffic_sql` và để trống password. JDBC URL là `jdbc:trino://trino:8080/lakehouse/gold`; username nằm trong native auth properties, không thêm `?user=...` vào URL vì sẽ trùng với username hộp đăng nhập truyền vào. Đây là user của kết nối Trino; tài khoản `cbadmin` dùng đăng nhập giao diện CloudBeaver. Cấu trúc `auth-properties.userName` theo [DBeaver auth model reference](https://dbeaver.com/docs/dbeaver/Auth-Model-Reference/).

CloudBeaver gửi truy vấn qua Trino để đọc cả Bronze, Silver và Gold. Catalog Trino đang đặt `iceberg.security=read_only`, vì vậy chạy DDL và các lệnh ghi bằng Spark SQL. Không cần bật Airflow hoặc cụm Spark master/worker để truy vấn dữ liệu đã lưu.

Kết quả EDA ban đầu nằm trong [`eda/gold_readiness.md`](eda/gold_readiness.md). Mapping hiện hành ở [`eda/silver_gold_column_mapping.csv`](eda/silver_gold_column_mapping.csv); implementation và hướng dẫn load nằm ở phần **Load Gold và truy vấn dashboard** bên dưới.

## Cấu hình và chạy toàn bộ stack

### 1. Tạo file `.env`

Copy [`.env.example`](.env.example) thành `.env` ở thư mục gốc nếu chưa có. Thay mọi placeholder `<...>` bằng giá trị riêng; mẫu không chứa secret thật. Giữ `.env` hiện có nếu đang dùng một kho local đã khởi tạo. Khi chạy thêm Airflow, sinh hai key ở mục 2.

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
Nhận license hợp lệ từ MinIO theo chính sách của nhà cung cấp. License riêng không được lưu trong Git; mỗi môi trường cần chuẩn bị file phù hợp trước khi khởi động.

### 6. Kiểm tra cấu hình và chạy

Kiểm tra xem các biến đã được đọc đủ chưa:

```bash
docker compose config --quiet
```

Nếu có biến chưa khai báo trong `.env`, Compose sẽ cảnh báo `The "XXX" variable is not set`.

Build image và chạy:

```bash
docker compose up -d --build
```

## Load Gold và truy vấn dashboard

Job `jobs/batch/transformation/silver_to_gold.py` thực thi Spark SQL, giữ timestamp UTC và calendar/hour theo America/New_York. Gold có tám bảng, 63 cột sau khi bỏ `dim_date.is_cbd_tolling`; grid keys dùng STRING cell_id. Bronze → Silver loại status=-101; loader Gold kiểm tra lại filter.

Chiều dài là **ước lượng** median chính xác của `speed_mph * 0.44704 * travel_time_sec` theo link, với speed/time dương và hữu hạn. Latitude/longitude của `silver.link_info` là điểm đại diện cho bản đồ và mapping grid, không phải chiều dài. Free-flow là P85 chính xác (`percentile_cont`) của speed dương trong 00:00–05:59 New York, cùng grid/giờ weather có precipitation=0 và snowfall=0. Kỳ baseline cố định cần giữ nguyên khi rerun một phần dữ liệu. Baseline toàn kỳ 01–09/2025 phục vụ mô tả lịch sử; dự báo/backtest cần kỳ hiệu chuẩn trong quá khứ.

Dùng hàm `Invoke-TrafficSpark` đã định nghĩa phía trên:

```powershell
Invoke-TrafficSpark spark-submit /opt/spark/jobs/ddl/gold.py
Invoke-TrafficSpark spark-submit /opt/spark/jobs/tests/test_data_cleaning.py
Invoke-TrafficSpark spark-submit /opt/spark/jobs/tests/test_silver_to_gold.py --integration
Invoke-TrafficSpark spark-submit /opt/spark/jobs/batch/transformation/silver_to_gold.py --start-date 2025-01-01 --end-date 2025-09-30 --baseline-start-date 2025-01-01 --baseline-end-date 2025-09-30
```

`--steps dimensions,traffic,weather,congestion,validate` là mặc định; có thể chọn subset, ví dụ `--steps validate`. Dimensions giữ road/condition keys qua rerun. Road initial snapshot được giả định hiệu lực từ 01/01/2025; thay đổi thuộc tính cần `--effective-date` để mở SCD2 version mới và nạp lại toàn phạm vi facts bị ảnh hưởng. Chạy một writer Gold tại một thời điểm.

Facts được thay thế theo scope tháng: materialize/kiểm tra source trước, DELETE scope rồi INSERT. Hai commit này không phải một giao dịch nguyên tử; nếu dừng giữa chúng, rerun đúng scope để phục hồi. Các tháng ngoài scope không bị xóa. Weather nạp cả buffer hiện có ở Silver. Calendar có cả buffer; is_holiday, owner, elevation, rain và cloud cover giữ NULL khi Silver chưa có nguồn/định nghĩa.

Precipitation tại weather timestamp H biểu diễn [H−1h,H), còn temperature/wind là tại H. Congestion giờ H join weather H một lần sau aggregation, không LAG hoặc SUM precipitation qua reading. Ba SUM congestion cùng tập reading có length/free-flow/time hợp lệ; đó là tổng trên reading quan sát, không phải tổng quãng đường mọi xe.

Bốn SELECT Trino có comment tiếng Việt trong [`jobs/batch/dashboard/dashboard_queries.sql`](jobs/batch/dashboard/dashboard_queries.sql) dùng fact theo **đoạn đường–giờ**: H01 tìm giờ New York có TTI trung bình cao nhất; H02 so sánh mưa/tuyết với khô ráo trong cùng road version–giờ–thứ; H03 mô tả thời tiết khi tắc và tỷ lệ tắc trong từng nhóm thời tiết; H04 liệt kê từng đoạn–giờ tắc cùng weather grid gần nhất. TTI của một đoạn–giờ là `sum_travel_time_sec / sum_free_flow_time_sec`; H01 lấy trung bình các TTI này, mỗi đoạn–giờ quan sát có một phiếu. Ngưỡng tắc `TTI >= 1.5` là tham số dashboard có thể chỉnh trong SQL. So sánh thời tiết là liên hệ quan sát, chưa kết luận nhân quả. Không điền giờ mất reading thành zero congestion. Cách đọc và trọng số chi tiết ở [`eda/dashboard_queries.md`](eda/dashboard_queries.md).

Rebuild có xóa dữ liệu cần được chủ dữ liệu cho phép trước. Chỉ dùng `gold.py --recreate` khi đã được phép drop/purge đúng tám bảng Gold và đã chuẩn bị nạp lại; lệnh init thông thường không cần cờ này. Các job nạp thường ngày không yêu cầu xóa raw files, Kafka topics hoặc checkpoints.

Sau khi Kafka → Bronze hoàn tất và schema Silver đã sẵn sàng, dùng job `bronze_to_silver.py`: chạy weather/link_info một lần, rồi traffic từng tháng để giới hạn bộ nhớ. Ngày traffic tính theo New York. Ví dụ nạp lịch sử 01–09/2025:

```powershell
Invoke-TrafficSpark spark-submit /opt/spark/jobs/batch/transformation/bronze_to_silver.py --start-date 2025-01-01 --end-date 2025-09-30 --steps 'weather,link_info'
if ($LASTEXITCODE -ne 0) { throw 'Silver weather/link_info failed' }

foreach ($trafficMonth in 1..9) {
    $monthStart = [datetime]::new(2025, $trafficMonth, 1)
    $startDay = $monthStart.ToString('yyyy-MM-dd')
    $endDay = $monthStart.AddMonths(1).AddDays(-1).ToString('yyyy-MM-dd')
    Invoke-TrafficSpark spark-submit /opt/spark/jobs/batch/transformation/bronze_to_silver.py --start-date $startDay --end-date $endDay --steps traffic
    if ($LASTEXITCODE -ne 0) { throw "Silver traffic failed for month $trafficMonth" }
}
```

Chỉ chạy lệnh load Gold ở trên sau khi toàn bộ bước Silver hoàn tất thành công. Không phát lại producer nếu Bronze đã có đủ dữ liệu cần xử lý.
