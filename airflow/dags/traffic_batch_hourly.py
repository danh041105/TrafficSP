"""Batch layer GĐ2 chạy mỗi giờ: Kafka -> Bronze -> Silver -> Gold.

    kafka_to_bronze -> plan -> bronze_to_silver -> silver_to_gold

- plan hỏi Trino để lấy phạm vi từ DỮ LIỆU, không dùng ngày logic của Airflow
  (Airflow chạy năm 2026, dữ liệu là năm 2025 và phát lại nhanh theo --speedup):
    Gold:   từ giờ NY cuối cùng đã có trong Gold (thường dở dang -> tính lại) tới giờ NY mới nhất trong Bronze.
    Silver: trọn các ngày NY chứa khoảng giờ đó (bronze_to_silver.py chỉ ghi lại theo ngày).
- Mọi job đều chạy lại được: Bronze nhớ offset bằng checkpoint; Silver và Gold xóa đúng phạm vi rồi ghi lại.
  Task lỗi thì retry hoặc Clear trên UI là đủ.
- max_active_runs=1: silver_to_gold yêu cầu chỉ một tiến trình ghi Gold tại một thời điểm.
- Producer GĐ2 phải chạy với --weather-delay-min 0: silver_to_gold dừng nếu giờ traffic cuối chưa có weather.
- Speed layer KHÔNG nằm trong DAG: chạy liên tục bên ngoài và phải khởi động trước producer gd2.
- Spark driver chạy trong container scheduler (LocalExecutor), dùng spark-defaults.conf và PYTHONPATH=/opt/airflow/jobs.
"""
import logging
from datetime import datetime, timedelta

import requests
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

log = logging.getLogger(__name__)

JOBS = "/opt/airflow/jobs"

# Phút thứ 5 mỗi giờ (giờ Asia/Ho_Chi_Minh theo AIRFLOW__CORE__DEFAULT_TIMEZONE).
# Số giờ dữ liệu mỗi lượt ≈ --speedup của producer × chu kỳ DAG (giờ thật).
SCHEDULE = "5 * * * *"

# Mốc hiệu chỉnh dim_road_link (độ dài, tốc độ thông thoáng) phải CỐ ĐỊNH và trùng lần nạp Gold GĐ1.
# Nếu để mặc định (= phạm vi của lượt chạy), số hiệu chỉnh đổi theo từng lượt và silver_to_gold
# sẽ dừng với lỗi "Road attributes/calibration changed".
BASELINE = "--baseline-start-date 2025-01-01 --baseline-end-date 2025-09-30"

# Trino trong mạng compose; catalog lakehouse chỉ đọc
TRINO_URL = "http://trino:8080/v1/statement"
TRINO_HEADERS = {"X-Trino-User": "airflow", "X-Trino-Catalog": "lakehouse"}

# Kết quả của task plan (XCom là dict)
PLAN = "ti.xcom_pull(task_ids='plan')"
SILVER_START = "{{ " + PLAN + "['silver_start'] }}"
SILVER_END = "{{ " + PLAN + "['silver_end'] }}"
GOLD_START = "{{ " + PLAN + "['gold_start'] }}"
GOLD_END = "{{ " + PLAN + "['gold_end'] }}"


def trino_scalar(sql: str):
    # Gửi câu SQL qua REST API của Trino, theo nextUri tới khi xong, trả về ô đầu tiên (None nếu rỗng)
    resp = requests.post(TRINO_URL, data=sql, headers=TRINO_HEADERS, timeout=30).json()
    rows = []
    while True:
        if "error" in resp:
            raise RuntimeError(resp["error"]["message"])
        rows += resp.get("data", [])
        if "nextUri" not in resp:
            break
        resp = requests.get(resp["nextUri"], headers=TRINO_HEADERS, timeout=30).json()
    return rows[0][0] if rows else None


def plan_range(gold_last_key: int, bronze_newest: str | None) -> dict:
    # gold_last_key: max(date_key*100 + hour_key) của Gold, vd 2025093023
    # bronze_newest: max(data_as_of) hợp lệ của Bronze do Trino trả về, vd "2025-10-05 13:42:00.000"
    gold_last = datetime.strptime(str(gold_last_key), "%Y%m%d%H")
    newest = datetime.strptime(bronze_newest[:13], "%Y-%m-%d %H") if bronze_newest else gold_last
    end = max(gold_last, newest)
    return {
        "silver_start": f"{gold_last:%Y-%m-%d}",
        "silver_end": f"{end:%Y-%m-%d}",
        "gold_start": f"{gold_last:%Y-%m-%dT%H}",
        "gold_end": f"{end:%Y-%m-%dT%H}",
    }


with DAG(
    dag_id="traffic_batch_hourly",
    description="GĐ2: Kafka -> Bronze -> Silver -> Gold theo giờ, phạm vi tính từ dữ liệu",
    schedule=SCHEDULE,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=50),
    },
    tags=["traffic", "batch", "gd2"],
) as dag:
    # Đọc mọi message mới kể từ checkpoint rồi tự dừng (0 dòng khi không có gì mới là bình thường)
    kafka_to_bronze = BashOperator(
        task_id="kafka_to_bronze",
        bash_command=f"spark-submit {JOBS}/batch/ingestion/kafka_to_bronze.py",
    )

    @task(retries=2, execution_timeout=timedelta(minutes=5))
    def plan() -> dict:
        last_key = trino_scalar(
            "SELECT max(CAST(date_key AS bigint) * 100 + hour_key) FROM lakehouse.gold.fact_traffic_reading")
        if last_key is None:
            raise ValueError("Gold chưa có dữ liệu: nạp GĐ1 bằng tay trước khi bật DAG")
        # Chỉ quét Bronze từ tháng của giờ cuối trong Gold; data_as_of hỏng -> try(...) ra null, max bỏ qua
        month = f"{str(last_key)[:4]}-{str(last_key)[4:6]}"
        newest = trino_scalar(f"""
            SELECT max(try(date_parse(data_as_of, '%Y-%m-%dT%H:%i:%s')))
            FROM lakehouse.bronze.traffic_raw
            WHERE event_month >= '{month}'""")
        rng = plan_range(last_key, newest)
        log.info("Gold có tới %s, Bronze có tới %s -> phạm vi %s", last_key, newest, rng)
        return rng

    # link_info không đổi trong GĐ2 nên không chạy lại
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(f"spark-submit {JOBS}/batch/transformation/bronze_to_silver.py "
                      f"--steps weather,traffic --start-date {SILVER_START} --end-date {SILVER_END}"),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(f"spark-submit {JOBS}/batch/transformation/silver_to_gold.py "
                      f"--start-hour {GOLD_START} --end-hour {GOLD_END} {BASELINE}"),
    )

    # TODO khi speed_layer.py xong: thêm task dọn các dòng lakehouse.speed.* đã có trong Gold
    kafka_to_bronze >> plan() >> bronze_to_silver >> silver_to_gold
