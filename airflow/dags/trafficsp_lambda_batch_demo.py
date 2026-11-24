"""Batch Layer của phiên phát lại kiểm chứng: đúng các job batch của làn chính, chạy trên namespace riêng của phiên.

scripts/demo_replay.py start ghi /opt/lakehouse/logs/demo_lane.env, gồm:
- topic Kafka của phiên;
- namespace bronze_<run>, silver_<run>, gold_<run>, speed_<run>;
- checkpoint gốc của phiên;
- DEMO_BATCH_ACTIVE.

Mỗi task nạp file đó trước khi spark-submit, nên config.py trỏ sang tài nguyên của phiên. Bảng Gold chính,
batch_state chính và checkpoint Bronze chính không bị đụng tới. Bảng chiều gold.dim_road_link dùng chung, chỉ đọc.

Lịch 10 phút là nhịp kiểm tra. Mỗi lượt:
1. Bronze đọc mọi message mới của topic phiên.
2. plan_batch tính mốc an toàn theo GIỜ SỰ KIỆN; không tiến được thì SKIPPED.
3. Silver, Batch View 5 phút và 1 giờ, DQ.
4. lambda_handoff công bố T_demo mới (PREPARING, VALIDATED, PUBLISHED trong gold_<run>.lambda_control).

Không có phiên, hoặc phiên đã stop (DEMO_BATCH_ACTIVE=0), thì task đầu thoát 99: cả lượt SKIPPED.
"""
import datetime as dt
import json

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:                                   # Airflow 2.x
    from airflow.operators.bash import BashOperator

JOBS = "/opt/airflow/jobs"
ENV_FILE = "/opt/lakehouse/logs/demo_lane.env"
LOAD = f"set -a; . {ENV_FILE}; set +a; "
SUBMIT = LOAD + f"spark-submit --conf spark.cores.max=2 --conf spark.executor.memory=1500m {JOBS}"
PLAN = "ti.xcom_pull(task_ids='plan') | from_json"


def from_json(s):
    return json.loads(s) if isinstance(s, str) else s


with DAG(
    dag_id="trafficsp_lambda_batch_demo",
    description="Batch Layer + Lambda handoff của phiên phát lại kiểm chứng (namespace riêng của phiên)",
    schedule="*/10 * * * *",
    start_date=dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"owner": "trafficsp", "retries": 1, "retry_delay": dt.timedelta(minutes=1),
                  "execution_timeout": dt.timedelta(minutes=30)},
    user_defined_filters={"from_json": from_json},
    tags=["trafficsp", "lambda", "batch", "demo"],
) as dag:
    lane = BashOperator(
        task_id="lane_active",
        bash_command=(f"test -f {ENV_FILE} || {{ echo 'không có phiên phát lại'; exit 99; }}; " + LOAD +
                      "[ \"$DEMO_BATCH_ACTIVE\" = 1 ] || { echo \"phiên $DEMO_RUN_ID đã dừng làn batch\"; exit 99; }; "
                      "echo \"phiên $DEMO_RUN_ID: $BRONZE_NAMESPACE -> $SILVER_NAMESPACE -> $GOLD_NAMESPACE\""),
        skip_on_exit_code=99, retries=0,
    )
    kafka_to_bronze = BashOperator(task_id="kafka_to_bronze", bash_command=f"{SUBMIT}/batch/ingestion/kafka_to_bronze.py")
    plan = BashOperator(
        task_id="plan",
        bash_command=("set -o pipefail; out=$(" + SUBMIT + "/batch/gold/plan_batch.py 2>/dev/null | tail -1) || exit 1; "
                      "echo \"$out\"; echo \"$out\" | grep -q '\"skip\": true' && exit 99 || exit 0"),
        do_xcom_push=True, retries=1, skip_on_exit_code=99,
    )
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(f"{SUBMIT}/batch/transformation/bronze_to_silver.py --steps weather,link_info,traffic "
                      "--start-date {{ (" + PLAN + ").silver_start }} --end-date {{ (" + PLAN + ").silver_end }}"),
    )
    batch_5min = BashOperator(
        task_id="batch_5min",
        bash_command=(f"{SUBMIT}/batch/gold/batch_views.py --only 5min --batch-run-id demo-{{{{ run_id | replace(':', '') }}}} "
                      "--start-day {{ (" + PLAN + ").utc_start }} --end-day {{ (" + PLAN + ").utc_end }}"),
    )
    batch_hourly = BashOperator(
        task_id="batch_hourly",
        bash_command=(f"{SUBMIT}/batch/gold/batch_views.py --only hourly --batch-run-id demo-{{{{ run_id | replace(':', '') }}}} "
                      "--start-day {{ (" + PLAN + ").utc_start }} --end-day {{ (" + PLAN + ").utc_end }}"),
    )
    dq_checks = BashOperator(
        task_id="dq_checks",
        bash_command=(f"{SUBMIT}/batch/gold/dq_checks.py "
                      "--start-day {{ (" + PLAN + ").utc_start }} --end-day {{ (" + PLAN + ").utc_end }}"),
        retries=0,
    )
    lambda_handoff = BashOperator(
        task_id="lambda_handoff",
        bash_command=(f"{SUBMIT}/batch/gold/lambda_handoff.py --no-recompute --until auto "
                      "--from-day {{ (" + PLAN + ").utc_start }}"),
    )
    lane >> kafka_to_bronze >> plan >> bronze_to_silver >> batch_5min >> batch_hourly >> dq_checks >> lambda_handoff
