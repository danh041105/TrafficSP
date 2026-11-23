"""Suy luận dự báo tốc độ 15' tự động, theo giờ sự kiện của dữ liệu.

Lịch 5 phút chỉ là nhịp kiểm tra. Mỗi lượt, jobs/ml/inference_task.py:
- đọc cửa sổ 5' hoàn tất mới nhất trong Serving Layer của từng làn;
- dự báo mọi gốc còn thiếu kể từ gốc đã ghi gần nhất (tối đa 12 gốc mỗi lượt);
- bỏ qua (SKIPPED) khi không có cửa sổ mới hoặc thiếu dữ liệu hợp lệ.
Vì vậy dữ liệu phát lại nhanh hay chậm so với đồng hồ máy đều không làm sai hay bỏ sót gốc dự báo.

- Spark chạy local trong container scheduler (local[2]): dữ liệu suy luận nhỏ (124 đoạn x 8 ngày cửa sổ 5'),
  không chiếm core của cụm (Speed Layer và batch dùng cụm).
- Ghi bằng MERGE theo (link_id, prediction_origin_utc, model_version): retry/chạy lại không sinh bản trùng.
- Model: models/ml/current.json + manifest sha256 (mount chỉ đọc vào /opt/airflow/models/ml).
- Mỗi lượt ghi một dòng JSON vào /opt/lakehouse/logs/inference_runs.jsonl (dashboard trang Vận hành đọc file này).
"""
import datetime as dt

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:                                   # Airflow 2.x
    from airflow.operators.bash import BashOperator

JOBS = "/opt/airflow/jobs"
ENV = {"PYTHONPATH": JOBS, "ML_CONFIG": "/opt/airflow/conf/ml/forecast.yaml", "TRINO_URL": "http://trino:8080"}


def submit(lane: str) -> str:
    # exit 99 = SKIPPED (không có gốc mới / thiếu dữ liệu hợp lệ / làn chưa tồn tại)
    return (f'spark-submit --master "local[2]" --driver-memory 2g --conf spark.ui.enabled=false '
            f"{JOBS}/ml/inference_task.py --lane {lane}")


with DAG(
    dag_id="trafficsp_ml_inference",
    description="Dự báo tốc độ 15 phút theo giờ sự kiện, cho hệ thống chính và phiên phát lại kiểm chứng",
    schedule="*/5 * * * *",
    start_date=dt.datetime(2026, 11, 1, tzinfo=dt.timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "trafficsp", "retries": 1, "retry_delay": dt.timedelta(minutes=1),
                  "execution_timeout": dt.timedelta(minutes=15)},
    tags=["trafficsp", "ml", "inference"],
) as dag:
    main = BashOperator(task_id="predict_main", bash_command=submit("main"), env=ENV, append_env=True, skip_on_exit_code=99)
    demo = BashOperator(task_id="predict_demo", bash_command=submit("demo"), env=ENV, append_env=True, skip_on_exit_code=99,
                        trigger_rule="all_done")
    main >> demo
