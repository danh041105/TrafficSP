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

# ================= Speed Layer (Spark Structured Streaming) -> Iceberg; truy vấn qua Trino =================
# Không dùng Flink, không dùng ClickHouse. Batch và Speed ghi vào HAI namespace khác nhau (gold.* / speed.*),
# không bao giờ ghi chung một bảng. Mọi giá trị đọc được từ biến môi trường để benchmark không phải sửa code.

# Mỗi streaming query một checkpoint riêng (không dùng chung thư mục)
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "s3a://lakehouse/checkpoints")
# v2: checkpoint mới cho sink Iceberg (không dùng lại checkpoint của bản sink ClickHouse cũ)
CHECKPOINT_SPEED_LATEST = f"{CHECKPOINT_ROOT}/speed_v2_latest"
CHECKPOINT_SPEED_5MIN = f"{CHECKPOINT_ROOT}/speed_v2_5min"
CHECKPOINT_SPEED_WEATHER = f"{CHECKPOINT_ROOT}/speed_v2_weather"

# Mỗi trigger = 1 commit Iceberg / query -> trigger quyết định số snapshot và số file nhỏ (xem docs/streaming_benchmark.md)
SPEED_TRIGGER = os.getenv("SPEED_TRIGGER", "30 seconds")
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

# Speed (Iceberg) - chỉ Speed Layer ghi. SPEED_NAMESPACE đổi được (vd lakehouse.speed_bench) để benchmark cô lập.
SPEED_NAMESPACE = os.getenv("SPEED_NAMESPACE", "lakehouse.speed")
SPEED_TRAFFIC_5MIN = f"{SPEED_NAMESPACE}.traffic_5min_rt"     # grain (link_id, window_start_utc), append-only
SPEED_TRAFFIC_LATEST = f"{SPEED_NAMESPACE}.traffic_latest"    # grain 1 dòng / link, MERGE INTO
SPEED_WEATHER_OBS = f"{SPEED_NAMESPACE}.weather_obs"          # thời tiết nhận realtime, append-only

# Gold (Iceberg) - chỉ Batch Layer ghi
GOLD_NAMESPACE = "lakehouse.gold"
GOLD_TRAFFIC_5MIN = "lakehouse.gold.traffic_5min_batch"
GOLD_TRAFFIC_HOURLY = "lakehouse.gold.traffic_hourly"
GOLD_LINK_BASELINE = "lakehouse.gold.link_baseline"
GOLD_LAMBDA_CONTROL = "lakehouse.gold.lambda_control"        # nhật ký trạng thái handoff (append-only)
SILVER_TRAFFIC_QUARANTINE = "lakehouse.silver.traffic_quarantine"

# Lambda handoff: Trino đọc Batch View qua TAG này (trỏ tới snapshot đã kiểm chứng), không đọc nhánh main
LAMBDA_PUBLISHED_TAG = "published"
LAMBDA_LOCK_KEY = "trafficsp.lambda_handoff"                 # PostgreSQL advisory lock: chặn 2 handoff chạy đồng thời

# Serving (Trino): view nằm trong schema riêng, không chứa dữ liệu
SERVING_SCHEMA = "lakehouse.serving"
OPS_LOG_DIR = os.getenv("OPS_LOG_DIR", "/opt/lakehouse/logs")  # JSONL tiến độ/độ trễ của Speed Layer
