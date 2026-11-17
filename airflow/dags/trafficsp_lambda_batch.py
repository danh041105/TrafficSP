"""TrafficSP - Batch Layer + Lambda handoff (Airflow 3).

  kafka_to_bronze -> plan -> bronze_to_silver -> [batch_5min, batch_hourly] -> dq_checks -> lambda_handoff -> verify_serving

- Speed Layer KHÔNG nằm trong DAG: nó chạy liên tục (jobs/stream/run_speed_layer.sh), không bị khởi động lại theo lịch Batch.
- Mọi task idempotent (Bronze dùng checkpoint offset; Silver xoá-rồi-ghi theo ngày; Gold overwrite theo khoảng ngày UTC;
  handoff có khoá + nhật ký trạng thái) -> retry an toàn.
- lambda_handoff chỉ chạy khi dq_checks thành công (trigger_rule mặc định all_success) và tự kiểm tra lại trên đúng snapshot
  sẽ công bố, nên T_batch không thể được công bố khi Batch chưa hoàn tất.
- max_active_runs=1: không có 2 lượt Batch ghi đè nhau; handoff còn có PostgreSQL advisory lock phòng chạy tay song song.
- Khoảng xử lý do task plan quyết định từ DỮ LIỆU (T_batch, Bronze), không dùng ngày logic của Airflow.
"""
import datetime as dt
import json

from airflow.sdk import DAG, Param

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:                                   # Airflow 2.x
    from airflow.operators.bash import BashOperator

JOBS = "/opt/airflow/jobs"
SUBMIT = ("spark-submit --conf spark.cores.max={{ params.cores }} --conf spark.executor.memory={{ params.executor_memory }} "
          f"{JOBS}")
PLAN = "ti.xcom_pull(task_ids='plan') | from_json"   # dùng trong template: {{ (… ) }}

default_args = {
    "owner": "trafficsp",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": dt.timedelta(hours=2),
}


def from_json(s):
    return json.loads(s) if isinstance(s, str) else s


with DAG(
    dag_id="trafficsp_lambda_batch",
    description="Kafka -> Bronze -> Silver -> Gold (5 phút, 1 giờ) -> DQ -> Lambda handoff -> kiểm chứng Trino",
    schedule="@hourly",
    start_date=dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    user_defined_filters={"from_json": from_json},
    params={
        "until": Param("auto", type="string", description="T_batch mới (UTC, ví dụ 2025-12-02T10:00:00) hoặc auto"),
        # null được phép -> ô trên UI không bắt buộc (chuỗi rỗng bị Airflow 3 coi là thiếu giá trị)
        "from_day": Param(None, type=["null", "string"], description="ép ngày UTC bắt đầu recompute (backfill), để trống = tự tính"),
        "cores": Param(2, type="integer"),
        "executor_memory": Param("1500m", type="string"),
    },
    tags=["trafficsp", "lambda", "batch"],
) as dag:
    kafka_to_bronze = BashOperator(
        task_id="kafka_to_bronze",
        bash_command=f"{SUBMIT}/batch/ingestion/kafka_to_bronze.py",
    )
    # Dòng JSON cuối của stdout -> XCom
    plan = BashOperator(
        task_id="plan",
        # exit 99 = Airflow đánh dấu SKIPPED -> các task sau cũng skip khi không có dữ liệu mới để xử lý
        bash_command=("set -o pipefail; out=$(" + SUBMIT + "/batch/gold/plan_batch.py "
                      "{% if params.from_day %}--from-day {{ params.from_day }}{% endif %} 2>/dev/null | tail -1) || exit 1; "
                      "echo \"$out\"; echo \"$out\" | grep -q '\"skip\": true' && exit 99 || exit 0"),
        do_xcom_push=True,
        retries=1,
    )
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(f"{SUBMIT}/batch/transformation/bronze_to_silver.py --steps weather,link_info,traffic "
                      "--start-date {{ (" + PLAN + ").silver_start }} --end-date {{ (" + PLAN + ").silver_end }}"),
    )
    batch_5min = BashOperator(
        task_id="batch_5min",
        bash_command=(f"{SUBMIT}/batch/gold/batch_views.py --only 5min --batch-run-id airflow-{{{{ run_id | replace(':', '') }}}} "
                      "--start-day {{ (" + PLAN + ").utc_start }} --end-day {{ (" + PLAN + ").utc_end }}"),
    )
    batch_hourly = BashOperator(
        task_id="batch_hourly",
        bash_command=(f"{SUBMIT}/batch/gold/batch_views.py --only hourly --batch-run-id airflow-{{{{ run_id | replace(':', '') }}}} "
                      "--start-day {{ (" + PLAN + ").utc_start }} --end-day {{ (" + PLAN + ").utc_end }}"),
    )
    dq_checks = BashOperator(
        task_id="dq_checks",
        bash_command=(f"{SUBMIT}/batch/gold/dq_checks.py "
                      "--start-day {{ (" + PLAN + ").utc_start }} --end-day {{ (" + PLAN + ").utc_end }}"),
        retries=0,                       # lỗi chất lượng không tự hết khi chạy lại
    )
    lambda_handoff = BashOperator(
        task_id="lambda_handoff",
        # --no-recompute: công bố đúng Batch View vừa ghi + kiểm tra ở 2 task trước; rc=0 cả khi chưa có cửa sổ mới
        bash_command=(f"{SUBMIT}/batch/gold/lambda_handoff.py --no-recompute --until {{{{ params.until }}}} "
                      "--from-day {{ (" + PLAN + ").utc_start }}"),
    )
    verify_serving = BashOperator(
        task_id="verify_serving",
        bash_command=f"python {JOBS}/batch/gold/verify_serving.py",
        env={"PYTHONPATH": JOBS, "TRINO_URL": "http://trino:8080"},
        append_env=True,
    )

    kafka_to_bronze >> plan >> bronze_to_silver >> [batch_5min, batch_hourly] >> dq_checks >> lambda_handoff >> verify_serving
