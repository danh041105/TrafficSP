# config.py: chứa tên topic, tên bảng, địa chỉ Kafka, đường dẫn được sử dụng > 2 lần
import os
from pathlib import Path

# Kafka portal
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# 2 topic raw trên kafka
# Đổi qua biến môi trường chỉ cho phiên phát lại kiểm chứng (scripts/demo_replay.py dùng topic *_demo riêng)
TOPIC_TRAFFIC = os.getenv("TOPIC_TRAFFIC", "traffic_raw")
TOPIC_WEATHER = os.getenv("TOPIC_WEATHER", "weather_raw")
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
# Bronze/Silver đổi được qua biến môi trường CHỈ cho làn batch của phiên phát lại kiểm chứng (scripts/demo_replay.py)
BRONZE_NAMESPACE = os.getenv("BRONZE_NAMESPACE", "lakehouse.bronze")
# các table trong namespace bronze
BRONZE_TRAFFIC_TABLE = f"{BRONZE_NAMESPACE}.traffic_raw"
BRONZE_WEATHER_TABLE = f"{BRONZE_NAMESPACE}.weather_raw"

# Silver
SILVER_NAMESPACE = os.getenv("SILVER_NAMESPACE", "lakehouse.silver")
# các table trong namespace silver
SILVER_TRAFFIC_CLEANED = f"{SILVER_NAMESPACE}.traffic_cleaned"
SILVER_WEATHER_CLEANED = f"{SILVER_NAMESPACE}.weather_cleaned"
SILVER_LINK_INFO = f"{SILVER_NAMESPACE}.link_info"
SILVER_INGEST_AUDIT = f"{SILVER_NAMESPACE}.ingest_audit"
# bổ sung 1 bảng traffic quarantine
SILVER_TRAFFIC_QUARANTINE = f"{SILVER_NAMESPACE}.traffic_quarantine"

# Gold (silver_to_gold.py vẫn tự ghép tên bảng từ "lakehouse.gold")
GOLD_NAMESPACE = "lakehouse.gold"
GOLD_FACT_TRAFFIC_READING = "lakehouse.gold.fact_traffic_reading"

# Lưu checkpoint để có 2 thông tin: đã đọc tới offset nào ở từng partition của topic, và đợt nào đã ghi xong vào bảng Iceberg
# Gốc mặc định trên MinIO (s3a); biến môi trường CHECKPOINT_ROOT đổi được gốc (vd chạy thử/benchmark) mà không sửa code.
# Mỗi streaming query một thư mục riêng: batch_layer/* cho job nạp Bronze, speed_v2_* cho Speed Layer (bên dưới).
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "s3a://lakehouse/checkpoints")
CHECKPOINT_TRAFFIC = f"{CHECKPOINT_ROOT}/batch_layer/traffic_raw"
CHECKPOINT_WEATHER = f"{CHECKPOINT_ROOT}/batch_layer/weather_raw"

# ---------- Spark Streaming ----------
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

# Mỗi streaming query một checkpoint riêng (không dùng chung thư mục); CHECKPOINT_ROOT định nghĩa ở trên
# v2: checkpoint mới cho sink Iceberg (không dùng lại checkpoint của bản sink ClickHouse cũ)
CHECKPOINT_SPEED_LATEST = f"{CHECKPOINT_ROOT}/speed_v2_latest"
# v4 (chỉ query 5 phút): rút gọn cột + đổi tên thước đo theo gold-dev (sum_speed_mph, ...) làm đổi schema state
# của phép gom -> phải dùng checkpoint mới (v3 là bản thử trên máy local). latest / weather không có state gom: giữ v2.
CHECKPOINT_SPEED_5MIN = f"{CHECKPOINT_ROOT}/speed_v4_5min"
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
SPEED_UI_PORT = 4041                                         # Spark UI của Speed Layer (compose map cổng 4041)
# Latest reading được coi là STALE nếu cũ hơn đồng hồ sự kiện (event_time lớn nhất toàn mạng) quá ngưỡng này
STALE_AFTER_MINUTES = int(os.getenv("STALE_AFTER_MINUTES", "15"))
# Free-flow và chiều dài link do loader của gold-dev tính (batch/transformation/silver_to_gold.py) vào gold.dim_road_link:
#   free_flow_speed_mph = P85 (percentile_cont) speed dương, 00:00-05:59 New York, giờ có mưa = 0 và tuyết = 0;
#   length_m = trung vị speed_mph * 0.44704 * travel_time_sec. Kỳ baseline CỐ ĐỊNH (đổi là sinh phiên bản SCD2 mới):
GOLD_BASELINE_START = "2025-01-01"
GOLD_BASELINE_END = "2025-09-30"

# Speed (Iceberg) - chỉ Speed Layer ghi. SPEED_NAMESPACE đổi được (vd lakehouse.speed_bench) để benchmark cô lập.
SPEED_NAMESPACE = os.getenv("SPEED_NAMESPACE", "lakehouse.speed")
SPEED_TRAFFIC_5MIN = f"{SPEED_NAMESPACE}.traffic_5min_rt"     # grain (link_id, window_start_utc), append-only
SPEED_TRAFFIC_LATEST = f"{SPEED_NAMESPACE}.traffic_latest"    # grain 1 dòng / link, MERGE INTO
SPEED_WEATHER_OBS = f"{SPEED_NAMESPACE}.weather_obs"          # thời tiết nhận realtime, append-only

# Gold (Iceberg) - chỉ Batch Layer ghi
# Quy ước đặt tên theo mô hình sao của gold-dev (jobs/ddl/gold.sql): dim_* / fact_*, đơn vị trong tên cột (_mph, _sec),
# khoá lịch date_key (YYYYMMDD) / hour_key (0-23) theo giờ New York. GOLD_NAMESPACE đổi được để chạy thử cô lập.
GOLD_NAMESPACE = os.getenv("GOLD_NAMESPACE", "lakehouse.gold")
GOLD_TRAFFIC_5MIN = f"{GOLD_NAMESPACE}.fact_traffic_5min"      # Batch View 5 phút (Lambda), grain (link_id, window_start_utc)
GOLD_TRAFFIC_HOURLY = f"{GOLD_NAMESPACE}.fact_traffic_hourly"  # Batch View 1 giờ, grain (link_id, hour_start_utc)
GOLD_DIM_ROAD_LINK = os.getenv("GOLD_DIM_ROAD_LINK", f"{GOLD_NAMESPACE}.dim_road_link")  # SCD2 (gold.sql), do silver_to_gold.py nạp; làn demo dùng chung bảng chính (chỉ đọc)
GOLD_LAMBDA_CONTROL = f"{GOLD_NAMESPACE}.lambda_control"       # nhật ký trạng thái handoff (append-only)
CALENDAR_TIMEZONE = "America/New_York"                         # múi giờ của date_key / hour_key (giống dim_date, dim_hour)

# Lambda handoff: Trino đọc Batch View qua TAG này (trỏ tới snapshot đã kiểm chứng), không đọc nhánh main
LAMBDA_PUBLISHED_TAG = "published"
LAMBDA_LOCK_KEY = "trafficsp.lambda_handoff"                 # PostgreSQL advisory lock: chặn 2 handoff chạy đồng thời

# Serving (Trino): view nằm trong schema riêng, không chứa dữ liệu
SERVING_SCHEMA = os.getenv("SERVING_SCHEMA", "lakehouse.serving")
OPS_LOG_DIR = os.getenv("OPS_LOG_DIR", "/opt/lakehouse/logs")  # JSONL tiến độ/độ trễ của Speed Layer
