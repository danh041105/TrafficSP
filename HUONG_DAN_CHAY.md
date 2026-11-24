# Hướng dẫn chạy nhanh TrafficSP (Speed Layer, Batch, ML, Dashboard)

Chạy mọi lệnh trong PowerShell, tại thư mục gốc repo. Chi tiết hơn xem `README.md` (cài đặt lần đầu, `.env`, `minio.license`).

```powershell
$C = "docker compose -p trafficsp"   # luôn dùng cùng tên project để không trùng container
```

Không chạy `docker compose down -v` hay `docker volume prune`: lệnh này xoá dữ liệu Kafka, Iceberg và checkpoint.

## 1. Lần đầu sau khi clone

```powershell
Copy-Item .env.example .env          # rồi điền mật khẩu trong .env
Invoke-Expression "$C config --quiet"
Invoke-Expression "$C --profile tools build spark-master producer dashboard"   # producer chỉ cần build, không bật
docker build -f dockerfiles/airflow.dockerfile -t ad/airflow:3.3.2 dockerfiles
cd dashboard/web; npm ci; npm run build; cd ../..   # dashboard đọc bản build trong dashboard/web/dist
```

Tạo bảng và view (chạy lại nhiều lần vẫn an toàn): xem mục 1 của `docs/streaming_runbook.md`.

## 2. Bật hệ thống

```powershell
$S = "postgresql minio kafka spark-master spark-worker-1 trino airflow-apiserver airflow-scheduler airflow-dag-processor " +
     "kafka-exporter cadvisor node-exporter prometheus grafana dashboard"
Invoke-Expression "$C up -d $S"
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Không bật `spark-worker-2`, `kafka-ui`, `cloudbeaver` nếu không cần: máy 16 GB RAM không đủ chạy hết.

Bật Speed Layer (chạy liên tục, không nằm trong Airflow):

```powershell
docker exec -d traffic-spark-master bash /opt/spark/jobs/stream/run_speed_layer.sh --starting-offsets latest --trigger "30 seconds"
```

Trong Airflow (http://localhost:8080) bật `trafficsp_lambda_batch` (chạy mỗi giờ) và `trafficsp_ml_inference`.
Để `traffic_batch_hourly` ở trạng thái tạm dừng: nó trùng việc với `trafficsp_lambda_batch`.

Model dự báo đã có sẵn trong repo (`models/ml/lgbm-v2-*/model.json.gz`), không cần train lại.

## 3. Chạy phiên phát lại (demo)

```powershell
python scripts/demo_replay.py start --start 2025-12-18T12:00 --end 2025-12-18T15:00 --speedup 4
python scripts/demo_replay.py status      # mở terminal khác để xem
python scripts/demo_replay.py stop        # BẮT BUỘC chạy khi xong
```

- Dữ liệu phát lại là dữ liệu lịch sử NYC DOT năm 2025, không phải giao thông trực tiếp.
- `start` giữ terminal tới khi phát xong (3 giờ dữ liệu, tốc độ 4 lần: khoảng 45 phút).
- Mỗi lúc chỉ chạy một phiên. Muốn chạy phiên mới: `stop` trước rồi mới `start`.
  `start` chưa tự dừng phiên cũ, hai phiên chồng nhau sẽ làm máy hết RAM.
- `stop` không xoá dữ liệu của phiên, chỉ dừng Speed Layer demo và bật lại Speed Layer chính.

## 4. Địa chỉ giao diện

| Giao diện | Địa chỉ |
|---|---|
| Dashboard TrafficSP | http://localhost:8095 |
| Grafana (tài nguyên, Kafka, Spark) | http://localhost:3001 |
| Prometheus | http://localhost:9090 |
| Airflow | http://localhost:8080 |
| Spark Master / Speed Layer UI | http://localhost:8090 / http://localhost:4041 |
| Kafka UI (nếu bật `kafka-ui`) | http://localhost:8085 |
| Trino (DBeaver) | `localhost:8088` |

## 5. Dừng hệ thống

```powershell
docker exec traffic-spark-master bash /opt/spark/jobs/stream/stop_speed_layer.sh
Invoke-Expression "$C stop"
```

`stop` giữ nguyên dữ liệu; lần sau chỉ cần làm lại mục 2.

## 6. Máy chậm thì kiểm tra

- Grafana, dashboard "Tài nguyên hệ thống": CPU, RAM của máy Docker và từng container.
- Còn phiên demo nào chưa `stop` không: `python scripts/demo_replay.py status`.
- Docker Desktop cần ổ đĩa còn trống vài GB; RAM cấp cho Docker đặt trong `%USERPROFILE%\.wslconfig`.
