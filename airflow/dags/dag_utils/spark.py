import subprocess
from airflow.sdk import Asset

JOBS = "/opt/airflow/jobs"
# Giới hạn core cho batch để stream (đang giữ 2 core) không bị đói tài nguyên
SPARK_OPTS = "--conf spark.cores.max=4"

GOLD_FACT = Asset("iceberg://lakehouse/gold/fact_link_hour")


def spark_cmd(script: str, args: str = "") -> str:
    """Dựng lệnh spark-submit cho BashOperator / @task.bash."""
    return f"spark-submit {SPARK_OPTS} {JOBS}/{script} {args}".strip()


def run_spark(script: str, args: list[str]) -> str:
    """Chạy spark-submit và trả về stdout (log của Spark nằm ở stderr)."""
    res = subprocess.run(
        ["spark-submit", *SPARK_OPTS.split(), f"{JOBS}/{script}", *args],
        capture_output=True, text=True,
    )
    print(res.stdout)
    print(res.stderr)
    res.check_returncode()
    return res.stdout