"""Quy tắc Lambda handoff dùng chung (Python thuần - unit test không cần Spark) + khoá PostgreSQL advisory lock.

gold.lambda_control là nhật ký APPEND-ONLY: mỗi lần đổi trạng thái của một handoff = 1 dòng mới
(PREPARING -> VALIDATED -> PUBLISHED, hoặc -> FAILED). Không UPDATE/DELETE nên không cần giao dịch nhiều dòng.
T_batch hiện hành = t_batch lớn nhất trong các dòng PUBLISHED (1970-01-01 nếu chưa có).
"""
import datetime as dt

EPOCH = dt.datetime(1970, 1, 1)
PREPARING, VALIDATED, PUBLISHED, FAILED = "PREPARING", "VALIDATED", "PUBLISHED", "FAILED"
TERMINAL = {PUBLISHED, FAILED}


def floor5(t: dt.datetime) -> dt.datetime:
    return t.replace(minute=t.minute - t.minute % 5, second=0, microsecond=0)


def naive_utc(t):
    """Spark trả timestamp naive (session UTC); bỏ tzinfo nếu có để so sánh thống nhất."""
    if t is None:
        return None
    return t.astimezone(dt.timezone.utc).replace(tzinfo=None) if t.tzinfo else t


def current_t_batch(rows: list[dict]) -> dt.datetime:
    pub = [naive_utc(r["t_batch"]) for r in rows if r["status"] == PUBLISHED and r.get("t_batch")]
    return max(pub) if pub else EPOCH


def last_status_by_handoff(rows: list[dict]) -> dict[str, dict]:
    last: dict[str, dict] = {}
    for r in sorted(rows, key=lambda r: naive_utc(r["created_at"])):
        last[r["handoff_id"]] = r
    return last


def pending_handoffs(rows: list[dict]) -> list[dict]:
    """Handoff dừng giữa chừng (crash): trạng thái cuối chưa phải PUBLISHED/FAILED."""
    return [r for r in last_status_by_handoff(rows).values() if r["status"] not in TERMINAL]


def recovery_action(pending: dict, tag_5min: int | None, tag_hourly: int | None, t_published: dt.datetime) -> str:
    """Quyết định cho một handoff dang dở (đang giữ khoá nên không còn tiến trình nào khác chạy nó):
      - PREPARING                      -> FAILED (Batch View có thể đã ghi nhưng chưa kiểm chứng; tag chưa đổi -> Trino không thấy)
      - VALIDATED, tag đã trỏ đúng snapshot đã kiểm chứng và t_batch > T_batch hiện hành -> PUBLISHED (hoàn tất bước cuối)
      - VALIDATED còn lại             -> FAILED (tag chưa dời hoặc đã bị handoff sau dời tiếp)."""
    if pending["status"] == VALIDATED and pending.get("snapshot_5min") is not None \
            and tag_5min == pending["snapshot_5min"] and tag_hourly == pending.get("snapshot_hourly") \
            and naive_utc(pending["t_batch"]) > t_published:
        return PUBLISHED
    return FAILED


def choose_t_new(until: str, max_event_ts: dt.datetime | None, lateness_minutes: int) -> tuple[dt.datetime, dt.datetime]:
    """Trả (t_new, safe_bound). safe_bound = floor5(max event_time của Silver - lateness): mọi cửa sổ trước mốc này đã
    đủ dữ liệu theo hợp đồng trễ. --until auto dùng chính safe_bound."""
    if max_event_ts is None:
        raise ValueError("Silver chưa có dữ liệu traffic -> không có gì để bàn giao")
    safe = floor5(naive_utc(max_event_ts) - dt.timedelta(minutes=lateness_minutes))
    t_new = safe if until == "auto" else floor5(dt.datetime.fromisoformat(until))
    return t_new, safe


def validate_move(t_old: dt.datetime, t_new: dt.datetime, safe_bound: dt.datetime, correction: bool) -> str | None:
    """None nếu được phép; ngược lại là lý do từ chối. T_batch không bao giờ giảm; chỉ giữ nguyên khi --correction."""
    if t_new < t_old:
        return f"T_batch không được giảm: {t_old} -> {t_new}"
    if t_new == t_old and not correction:
        return f"T_batch không đổi ({t_old}); dùng --correction để công bố lại dữ liệu đã sửa"
    if t_new > safe_bound:
        return f"T_new {t_new} vượt mốc an toàn {safe_bound} (cửa sổ chưa đủ dữ liệu theo watermark)"
    if t_new.minute % 5 or t_new.second or t_new.microsecond:
        return f"T_new {t_new} không nằm trên ranh giới 5 phút"
    return None


# ------------------------------------------------------------------ khoá chống handoff chạy đồng thời
class AdvisoryLock:
    """pg_try_advisory_lock trên chính PostgreSQL của Iceberg catalog, qua JDBC trong JVM của Spark.
    Khoá gắn với phiên kết nối: tiến trình chết -> kết nối đóng -> khoá tự nhả (không kẹt sau crash)."""

    def __init__(self, spark, key: str):
        self.spark, self.key, self.conn = spark, key, None

    def acquire(self) -> bool:
        c = self.spark.conf
        jvm = self.spark.sparkContext._jvm
        self.conn = jvm.java.sql.DriverManager.getConnection(
            c.get("spark.sql.catalog.lakehouse.uri"), c.get("spark.sql.catalog.lakehouse.jdbc.user"),
            c.get("spark.sql.catalog.lakehouse.jdbc.password"))
        rs = self.conn.createStatement().executeQuery(f"SELECT pg_try_advisory_lock(hashtext('{self.key}'))")
        rs.next()
        ok = bool(rs.getBoolean(1))
        if not ok:
            self.release()
        return ok

    def release(self):
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None
