# Monitoring TrafficSP: đã làm những gì (bản dễ hiểu)

> Người phụ trách: Mai. Phần hướng dẫn kỹ thuật đầy đủ xem [monitoring.md](monitoring.md).

## 1. Monitoring là gì, để làm gì?

Hệ thống TrafficSP có nhiều phần chạy cùng lúc: Kafka nhận dữ liệu, Spark xử lý. Monitoring giống **bảng đồng hồ của ô tô**: nhìn vào là biết hệ thống đang khỏe hay có vấn đề, không phải mở log ra đọc.

Mỗi "đồng hồ" là một **chỉ số**: một con số đo một thứ, được ghi lại liên tục theo thời gian.
Ví dụ: "mỗi giây có bao nhiêu message vào Kafka", "Spark còn mấy máy worker sống".

## 2. Hệ thống gồm những gì?

```
Kafka ──▶ kafka-exporter ─┐
                          ├──▶ Prometheus ──▶ Grafana
Spark (chỉ số có sẵn) ────┘
```

| Thành phần | Việc nó làm | Ví von |
|---|---|---|
| **kafka-exporter** | Đọc các con số của Kafka (số message, lag…) và đưa ra dạng Prometheus hiểu được | Người đọc đồng hồ nước |
| **Spark** | Tự đưa ra con số của chính nó (worker, task, RAM…), chỉ cần bật lên | Đồng hồ có sẵn trên xe |
| **Prometheus** | Cứ 15 giây đi đọc tất cả con số một lần và ghi lại (giữ 15 ngày) | Cuốn sổ ghi chép |
| **Grafana** | Vẽ các con số đã ghi thành biểu đồ | Bảng đồng hồ |

## 3. Bảng tổng kết công việc đã làm

| # | Việc đã làm | File liên quan | Kết quả |
|---|---|---|---|
| 1 | Thêm **kafka-exporter** đọc chỉ số Kafka | `docker-compose.yml` | Thấy được broker, số message, lag |
| 2 | Bật **chỉ số có sẵn của Spark** cho master, worker, driver, executor, streaming | `conf/spark-defaults.conf` | Spark tự đưa ra hơn 150 chỉ số |
| 3 | Thêm **Prometheus** và khai báo nơi đi đọc chỉ số | `docker-compose.yml`, `conf/prometheus/prometheus.yml` | Ghi lại số liệu 15 giây/lần, giữ 15 ngày |
| 4 | Cho **Grafana** tự kết nối Prometheus và tự nạp dashboard (không phải bấm tay) | `conf/grafana/provisioning/` | Mở Grafana là có sẵn dashboard |
| 5 | Làm **dashboard Kafka** (7 biểu đồ) | `conf/grafana/dashboards/kafka.json` | Xem dữ liệu vào Kafka |
| 6 | Làm **dashboard Spark** (13 biểu đồ, 3 nhóm) | `conf/grafana/dashboards/spark.json` | Xem cụm Spark, job, streaming |
| 7 | **Chạy thử** và sửa 2 lỗi cấu hình (xem mục 6) | `prometheus.yml`, `docker-compose.yml` | Mọi nguồn chỉ số đều đọc được |
| 8 | Làm **demo chạy bằng 1 lệnh** | `scripts/monitoring_demo/` | Ai cũng chạy lại được để xem |
| 9 | Viết **tài liệu** | `docs/monitoring.md`, file này | |
| 10 | Gộp với phần monitoring nhóm đã đưa lên `dev` (commit `c4742c9`) | | Không trùng lặp, giữ cấu trúc của nhóm |

## 4. Đang theo dõi những chỉ số nào?

### Dashboard "Kafka - Traffic"

| Chỉ số | Hỏi câu gì | Khi nào cần lo |
|---|---|---|
| Brokers | Kafka có đang sống không? | Khác 1 |
| Topics | Có đủ topic không? | Thiếu `traffic_raw` / `weather_raw` |
| Under-replicated partitions | Có partition nào bị thiếu bản sao? | Lớn hơn 0 |
| Messages vào / giây | Producer có đang gửi dữ liệu không? | Về 0 khi đáng lẽ đang chạy |
| Số message đang lưu | Kafka đang giữ bao nhiêu message? | Tăng mãi, có thể đầy ổ |
| Consumer group lag | Còn bao nhiêu message chưa ai đọc? | Tăng liên tục |
| Offset theo partition | Dữ liệu có chia đều các partition? | Lệch nhiều |

### Dashboard "Spark - Traffic"

| Nhóm | Chỉ số | Khi nào cần lo |
|---|---|---|
| **Cụm** | Worker còn sống | Nhỏ hơn 2 |
| | App đang chạy / đang chờ | App chờ > 0 lâu, tức là cụm hết core hoặc RAM |
| | Core và RAM đã dùng trên mỗi worker | Hết sạch |
| **Job (executor)** | Task đang chạy, hoàn thành, thất bại | Có task thất bại |
| | Bộ nhớ executor | Gần chạm giới hạn |
| | Tỉ lệ thời gian dọn rác (GC) | Trên 10%, tức là executor thiếu bộ nhớ |
| | Dữ liệu đọc vào / shuffle | Shuffle quá lớn |
| **Streaming** | Tốc độ dữ liệu vào so với tốc độ xử lý | Xử lý chậm hơn dữ liệu vào trong thời gian dài, tức là stream bị tụt lại |
| | Thời gian xử lý mỗi lô | Lâu hơn khoảng trigger, tức là các lô bị dồn |

## 5. Chạy demo

Cần: Docker Desktop bật, cấp **từ 8 GB RAM** (tối thiểu còn trống ~3 GB).

```bash
bash scripts/monitoring_demo/run_demo.sh up        # chạy 15 phút
```

Demo tự làm 4 việc:
1. Bật Kafka, Spark, kafka-exporter, Prometheus, Grafana.
2. Bơm dữ liệu giả vào Kafka: `traffic_raw` 20–50 message/giây, `weather_raw` 1 message/2 giây.
3. Tạo một consumer cố tình đọc chậm, để thấy lag tăng.
4. Chạy một job Spark streaming đọc `traffic_raw`, tính tốc độ trung bình theo `link_id` mỗi phút.

Sau 2–3 phút, mở http://localhost:3001 → **Dashboards → Monitoring**.

Tắt demo:

```bash
bash scripts/monitoring_demo/run_demo.sh down
```

## 6. Những lỗi đã gặp và cách đã sửa

| Lỗi | Nguyên nhân | Đã sửa |
|---|---|---|
| Prometheus không lấy được chỉ số driver Spark | Đường dẫn thiếu dấu `/` cuối, Spark trả về redirect 302 | Đổi thành `/metrics/prometheus/` |
| Sửa `prometheus.yml` nhưng Prometheus vẫn chạy cấu hình cũ, mất cả 1 job | Docker Desktop trên Mac/Windows lỗi khi mount **từng file** | Mount **cả thư mục** `conf/prometheus` |
| kafka-exporter không nói chuyện được với Kafka 4.x | Mặc định dùng giao thức Kafka 2.0 đã bị bỏ | Thêm `--kafka.version=3.6.0` |
| Job Spark bị tắt đột ngột (mã 137) | Docker hết RAM | Cấp thêm RAM cho Docker; demo dùng ít RAM hơn |

## 7. Lưu ý khi đọc dashboard

- **Lag của consumer group không thấy job Spark streaming**: Spark tự lưu vị trí đọc trong checkpoint, không báo cho Kafka. Muốn biết stream có chậm không thì xem biểu đồ **tốc độ dữ liệu vào so với tốc độ xử lý**.
- **Phần Executor và Streaming chỉ có số liệu khi có job Spark đang chạy**: driver chỉ mở cổng khi chạy. Không có job thì trống là bình thường.
- **Đường "input" trong biểu đồ dữ liệu đọc vào luôn bằng 0 với job đọc Kafka**: Spark chỉ đếm byte đọc từ file.

## 8. Việc tiếp theo

- [ ] Theo dõi Trino, MinIO, Airflow, Postgres (chờ bản vẽ kiến trúc mới).
- [ ] Theo dõi CPU/RAM từng container (cAdvisor).
- [ ] Cảnh báo tự động: worker chết, lag tăng liên tục, có task thất bại.
- [ ] Thêm `spark.executor.cores` vào các lệnh `spark-submit`.
