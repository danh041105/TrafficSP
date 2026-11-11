# clickhouse_io.py: kết nối + ghi ClickHouse dùng chung cho Speed Layer (foreachBatch) và Batch.
# Ghi idempotent: mỗi lần insert gắn insert_deduplication_token ổn định (vd "speed_5min:<batchId>").
# Nếu Spark chạy lại đúng micro-batch đó (retry / restart từ checkpoint) với cùng dữ liệu, ClickHouse bỏ qua
# block trùng (bảng có non_replicated_deduplication_window > 0). Ngoài ra bảng là ReplacingMergeTree theo grain,
# nên kể cả khi dữ liệu retry khác chút (vd updated_at), truy vấn FINAL vẫn chỉ thấy 1 dòng / grain.
import time

from common import config


def _backoff(attempt: int) -> float:
    return min(30, 2 ** attempt)


def get_client(database: str | None = None, retries: int | None = None):
    """Tạo client; clickhouse_connect kết nối ngay khi khởi tạo nên cũng phải thử lại khi ClickHouse tạm ngừng."""
    import clickhouse_connect
    retries = retries or config.CLICKHOUSE_RETRIES
    for attempt in range(1, retries + 1):
        try:
            return clickhouse_connect.get_client(host=config.CLICKHOUSE_HOST, port=config.CLICKHOUSE_HTTP_PORT,
                                                 username=config.CLICKHOUSE_USER, password=config.CLICKHOUSE_PASSWORD,
                                                 database=database or config.CLICKHOUSE_SERVING_DB)
        except Exception:
            if attempt == retries:
                raise
            time.sleep(_backoff(attempt))


def insert_df(client, table: str, pdf, token: str | None = None, retries: int | None = None):
    """Insert pandas DataFrame; thử lại với cùng token khi lỗi tạm thời (sink lỗi -> không nhân đôi dữ liệu)."""
    if pdf is None or len(pdf) == 0:
        return 0
    settings = {"insert_deduplication_token": token} if token else {}
    retries = retries or config.CLICKHOUSE_RETRIES
    for attempt in range(1, retries + 1):
        try:
            client.insert_df(table, pdf, settings=settings)
            return len(pdf)
        except Exception:
            if attempt == retries:
                raise
            time.sleep(_backoff(attempt))


def run_sql_file(client, path, **params):
    """Chạy file SQL nhiều câu lệnh (tách theo ';' cuối dòng), bỏ dòng comment."""
    text = open(path, encoding="utf-8").read()
    if config.CLICKHOUSE_SERVING_DB != "traffic":   # vd CLICKHOUSE_SERVING_DB=traffic_bench: bộ bảng tách riêng cho benchmark
        import re
        text = re.sub(r"\btraffic\.", f"{config.CLICKHOUSE_SERVING_DB}.", text)
        text = re.sub(r"DATABASE IF NOT EXISTS traffic\b", f"DATABASE IF NOT EXISTS {config.CLICKHOUSE_SERVING_DB}", text)
    if params:
        text = text.replace("{stale_after_minutes}", str(params.get("stale_after_minutes", config.STALE_AFTER_MINUTES)))
    stmts, buf = [], []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmts.append("\n".join(buf).strip().rstrip(";")); buf = []
    if "".join(buf).strip():
        stmts.append("\n".join(buf).strip())
    for s in stmts:
        client.command(s)
    return len(stmts)
