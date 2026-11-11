# config.py: chứa tên topic, tên bảng, địa chỉ Kafka, đường dẫn được sử dụng > 2 lần
import os
from pathlib import Path

# Kafka portal
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# 2 topic raw trên kafka
TOPIC_TRAFFIC = "traffic_raw"
TOPIC_WEATHER = "weather_raw"
MAX_OFFSETS_PER_TRIGGER = 500_000   # tối đa 500k message mỗi micro-batch

# Lấy dữ liệu trong data/raw cho producer
# Lấy đường dẫn tuyệt đối đến thư mục cha cách thư mục chứa file Python hiện tại 2 cấp.
ROOT = Path(__file__).resolve().parents[2]
RAW_TRAFFIC_DIR = ROOT / "data/raw/nyc_dot"
RAW_WEATHER_DIR = ROOT / "data/raw/open_meteo/ecmwf_ifs"  

# Chia giai đoạn 
# Giai đoạn 1: từ ngày 01/01/2025 đến ngày 30/09/2025
HISTORY_MONTHS = range(1, 10)
# Giai đoạn 2: từ ngày 01/10/2025 đến ngày 31/12/2025
REPLAY_MONTHS = range(10, 13)

# Lakehouse(MinIO + Iceberg):
# namespace nó tương tự như schema trong database được dùng để tổ chức và quản lý các table và view trong iceberg

# Bronze
BRONZE_NAMESPACE = "lakehouse.bronze"
# các table trong namespace
BRONZE_TRAFFIC_TABLE = "lakehouse.bronze.traffic_raw"
BRONZE_WEATHER_TABLE = "lakehouse.bronze.weather_raw"

# Silver
SILVER_NAMESPACE = "lakehouse.silver"
SILVER_TRAFFIC_CLEANED = "lakehouse.silver.traffic_cleaned"
SILVER_WEATHER_CLEANED = "lakehouse.silver.weather_cleaned"
SILVER_LINK_INFO = "lakehouse.silver.link_info"
SILVER_INGEST_AUDIT = "lakehouse.silver.ingest_audit"


# ---------- Spark Streaming ----------
# Lưu checkpoint để có 2 thông tin: đã đọc tới offset nào ở từng partition của topic, và đợt nào đã ghi xong vào bảng Iceberg
# Mặc định trên MinIO (s3a). Biến môi trường CHECKPOINT_ROOT cho phép môi trường dev dùng thư mục dùng chung khác.
CHECKPOINT_TRAFFIC = os.getenv("CHECKPOINT_ROOT", "s3a://lakehouse/checkpoints") + "/traffic_raw"
CHECKPOINT_WEATHER = os.getenv("CHECKPOINT_ROOT", "s3a://lakehouse/checkpoints") + "/weather_raw"
# setup: mỗi 30s Spark sẽ lấy toàn bộ message mới trong Kafka ghi vào Bronze.
TRIGGER_INTERVAL = "30 seconds"

# Ranh giới phân chia dữ liệu thời tiết thành 2 giai đoạn
# 2025-10-01 00:00 New York = 2025-10-01 04:00 UTC
# Giai đoạn 1: time <= WEATHER_PHASE_BOUNDARY_UTC
# Giai đoạn 2: time >  WEATHER_PHASE_BOUNDARY_UTC

WEATHER_PHASE_BOUNDARY_UTC = "2025-10-01T04:00"

# ================= Speed Layer (Spark Structured Streaming) + Serving (ClickHouse) =================
# Không dùng Flink. Mọi giá trị đọc được từ biến môi trường để benchmark/điều chỉnh không phải sửa code.
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_SERVING_DB = os.getenv("CLICKHOUSE_SERVING_DB", "traffic")
# Số lần thử kết nối/ghi; backoff 2,4,8,16,30,30,... giây -> 8 lần ~ 2 phút ClickHouse ngừng mà query không chết
CLICKHOUSE_RETRIES = int(os.getenv("CLICKHOUSE_RETRIES", "8"))

# Mỗi streaming query một checkpoint riêng (không dùng chung thư mục)
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "s3a://lakehouse/checkpoints")
CHECKPOINT_SPEED_LATEST = f"{CHECKPOINT_ROOT}/speed_latest"
CHECKPOINT_SPEED_5MIN = f"{CHECKPOINT_ROOT}/speed_5min"
CHECKPOINT_SPEED_WEATHER = f"{CHECKPOINT_ROOT}/speed_weather"

SPEED_TRIGGER = os.getenv("SPEED_TRIGGER", "10 seconds")
SPEED_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("SPEED_MAX_OFFSETS_PER_TRIGGER", "100000"))
SPEED_STARTING_OFFSETS = os.getenv("SPEED_STARTING_OFFSETS", "earliest")
# Watermark = độ trễ tối đa (theo event-time) còn được tính vào cửa sổ 5 phút; dữ liệu muộn hơn sẽ bị loại
# khỏi Speed Layer (đếm qua numRowsDroppedByWatermark) và được Batch Layer sửa khi recompute.
SPEED_WATERMARK = os.getenv("SPEED_WATERMARK", "10 minutes")
SPEED_WINDOW = "5 minutes"
SPEED_SHUFFLE_PARTITIONS = os.getenv("SPEED_SHUFFLE_PARTITIONS", "8")
# Latest reading được coi là STALE nếu cũ hơn đồng hồ sự kiện (event_time lớn nhất toàn mạng) quá ngưỡng này
STALE_AFTER_MINUTES = int(os.getenv("STALE_AFTER_MINUTES", "15"))
# Free-flow (D-18): p85 tốc độ hợp lệ lúc khô, 22:00-05:59 giờ New York, cần >= 100 mẫu
FREE_FLOW_PERCENTILE = 0.85
FREE_FLOW_LOCAL_HOURS = (22, 23, 0, 1, 2, 3, 4, 5)
FREE_FLOW_MIN_SAMPLES = 100

# Gold (Iceberg) cho Batch View 5 phút / 1 giờ
GOLD_NAMESPACE = "lakehouse.gold"
GOLD_TRAFFIC_5MIN = "lakehouse.gold.traffic_5min"
GOLD_TRAFFIC_HOURLY = "lakehouse.gold.traffic_hourly"
GOLD_LINK_BASELINE = "lakehouse.gold.link_baseline"
SILVER_TRAFFIC_QUARANTINE = "lakehouse.silver.traffic_quarantine"
