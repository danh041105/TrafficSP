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
CHECKPOINT_TRAFFIC = "s3a://lakehouse/checkpoints/traffic_raw"
CHECKPOINT_WEATHER = "s3a://lakehouse/checkpoints/weather_raw"
# setup: mỗi 30s Spark sẽ lấy toàn bộ message mới trong Kafka ghi vào Bronze.
TRIGGER_INTERVAL = "30 seconds"

# Ranh giới phân chia dữ liệu thời tiết thành 2 giai đoạn
# 2025-10-01 00:00 New York = 2025-10-01 04:00 UTC
# Giai đoạn 1: time <= WEATHER_PHASE_BOUNDARY_UTC
# Giai đoạn 2: time >  WEATHER_PHASE_BOUNDARY_UTC

WEATHER_PHASE_BOUNDARY_UTC = "2025-10-01T04:00"