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

# Múi giờ của New York
TRAFFIC_TIMEZONE = "America/New_York"

# Lakehouse(MinIO + Iceberg):
# namespace nó tương tự như schema trong database được dùng để tổ chức và quản lý các table và view trong iceberg
# Bronze
BRONZE_NAMESPACE = "lakehouse.bronze"
# các table trong namespace bronze
BRONZE_TRAFFIC_TABLE = "lakehouse.bronze.traffic_raw"
BRONZE_WEATHER_TABLE = "lakehouse.bronze.weather_raw"

# Silver
SILVER_NAMESPACE = "lakehouse.silver"
# các table trong namespace silver
SILVER_TRAFFIC_CLEANED = "lakehouse.silver.traffic_cleaned"
SILVER_WEATHER_CLEANED = "lakehouse.silver.weather_cleaned"
SILVER_LINK_INFO = "lakehouse.silver.link_info"
SILVER_INGEST_AUDIT = "lakehouse.silver.ingest_audit"
# bổ sung 1 bảng traffic quarantine
SILVER_TRAFFIC_QUARANTINE = "lakehouse.silver.traffic_quarantine"

# Gold (silver_to_gold.py vẫn tự ghép tên bảng từ "lakehouse.gold")
GOLD_NAMESPACE = "lakehouse.gold"
GOLD_FACT_TRAFFIC_READING = "lakehouse.gold.fact_traffic_reading"

# Lưu checkpoint để có 2 thông tin: đã đọc tới offset nào ở từng partition của topic, và đợt nào đã ghi xong vào bảng Iceberg
CHECKPOINT_TRAFFIC = "s3a://lakehouse/checkpoints/batch_layer/traffic_raw"
CHECKPOINT_WEATHER = "s3a://lakehouse/checkpoints/batch_layer/weather_raw"

# ---------- Spark Streaming ----------
# setup: mỗi 30s Spark sẽ lấy toàn bộ message mới trong Kafka ghi vào Bronze.
TRIGGER_INTERVAL = "30 seconds"

# Ranh giới phân chia dữ liệu thời tiết thành 2 giai đoạn
# 2025-10-01 00:00 New York = 2025-10-01 04:00 UTC
# Giai đoạn 1: time <= WEATHER_PHASE_BOUNDARY_UTC
# Giai đoạn 2: time >  WEATHER_PHASE_BOUNDARY_UTC

WEATHER_PHASE_BOUNDARY_UTC = "2025-10-01T04:00"

# Speed layer
SPEED_UI_PORT = 4041
SPEED_NAMESPACE = "lakehouse.speed"
# các table trong namespace speed
SPEED_TRAFFIC_5MIN = "lakehouse.speed.traffic_5min"
SPEED_TRAFFIC_LATEST = "lakehouse.speed.traffic_latest"
SPEED_WEATHER_OBS = "lakehouse.speed.weather_obs"

# Cấu hình các tham số của speed_layer:
# mỗi 30s Spark sẽ lấy toàn bộ message mới trong Kafka và đẩy vào speed layer
SPEED_TRIGGER = "30 seconds"
# Số lượng message tối đa được lấy mỗi 30s
SPEED_MAX_OFFSETS_PER_TRIGGER = 100_000
SPEED_STARTING_OFFSETS = "earliest"
SPEED_WINDOW = "5 minutes"

SPEED_SHUFFLE_PARTITIONS = "8" # Giới hạn số lượng shuffle partition của spark khi thực hiện các phép tính toán + tổng hợp

# mấy cái config dell giải thích được (để khải giải thích)
FREE_FLOW_PERCENTILE = 0.85
FREE_FLOW_BASELINE_HOURS = (22, 23, 0, 1, 2, 3, 4, 5)
FREE_FLOW_MIN_SAMPLES = 100

# T_Batch(tên tối nghĩa vcl để khải sửa lại)
# Thêm các config T_batch vào đây đi.


# Checkpoint của speed layer
CHECKPOINT_SPEED_TRAFFIC = "s3a://lakehouse/checkpoints/speed_layer/speed_traffic"
CHECKPOINT_SPEED_WEATHER = "s3a://lakehouse/checkpoints/speed_layer/speed_weather"