# Tạo database/bảng/view phục vụ trên ClickHouse (idempotent, chạy lại an toàn, không xóa dữ liệu).
# Chạy: docker exec -e PYTHONPATH=/opt/spark/jobs traffic-spark-master python3 /opt/spark/jobs/ddl/clickhouse_serving.py
from pathlib import Path

from common import config
from common.clickhouse_io import get_client, run_sql_file

HERE = Path(__file__).resolve().parent


def main():
    client = get_client(database="default")
    n1 = run_sql_file(client, HERE / "clickhouse_serving.sql")
    n2 = run_sql_file(client, HERE / "clickhouse_views.sql", stale_after_minutes=config.STALE_AFTER_MINUTES)
    tables = client.query(f"SELECT name, engine FROM system.tables WHERE database = '{config.CLICKHOUSE_SERVING_DB}' ORDER BY name").result_rows
    print(f"[clickhouse] {n1} DDL + {n2} view; {len(tables)} đối tượng trong {config.CLICKHOUSE_SERVING_DB}:")
    for name, engine in tables:
        print(f"  {name:32s} {engine}")


if __name__ == "__main__":
    main()
