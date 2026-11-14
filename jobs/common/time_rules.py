# time_rules.py: quy tắc thời gian dùng CHUNG cho Batch (Silver/Gold) và Speed Layer (stream).
# Theo D-03/D-04 của nhánh khai:
#   - data_as_of là giờ New York, không có offset.
#   - Giờ không tồn tại (tiến giờ, vd 2025-03-09 02:xx) và giờ lặp (lùi giờ, vd 2025-11-02 01:xx)
#     KHÔNG được tự gán offset: event_time_utc = NULL + gắn cờ, bản ghi vẫn giữ ở Bronze.
#   - Mọi phép gom theo thời gian (window 5 phút, giờ) dùng event_time_utc, nên các bản ghi này tự động bị loại
#     và được đếm riêng (n_dst_ambiguous).
# Mốc DST được tính từ tzdata (zoneinfo), không viết cứng ngày.
import datetime as dt
from functools import lru_cache
from zoneinfo import ZoneInfo

TRAFFIC_TIMEZONE = "America/New_York"
NY = ZoneInfo(TRAFFIC_TIMEZONE)
LOCAL_FORMAT = "%Y-%m-%dT%H:%M:%S"


@lru_cache(maxsize=None)
def dst_local_hours(year: int) -> dict:
    """Trả về {"ambiguous": [(date, hour)], "nonexistent": [(date, hour)]} theo giờ New York."""
    amb, nonex = [], []
    d = dt.datetime(year, 1, 1)
    while d.year == year:
        off0 = d.replace(tzinfo=NY, fold=0).utcoffset()
        off1 = d.replace(tzinfo=NY, fold=1).utcoffset()
        if off0 != off1:
            back = d.replace(tzinfo=NY, fold=0).astimezone(dt.timezone.utc).astimezone(NY).replace(tzinfo=None)
            (amb if back == d else nonex).append((d.date().isoformat(), d.hour))
        d += dt.timedelta(hours=1)
    return {"ambiguous": amb, "nonexistent": nonex}


def classify_local(local: dt.datetime) -> str:
    """'ok' | 'dst_ambiguous' | 'dst_nonexistent' cho một datetime giờ New York (không tzinfo)."""
    hours = dst_local_hours(local.year)
    key = (local.date().isoformat(), local.hour)
    if key in hours["ambiguous"]:
        return "dst_ambiguous"
    if key in hours["nonexistent"]:
        return "dst_nonexistent"
    return "ok"


def local_to_utc(local: dt.datetime) -> dt.datetime | None:
    """Đổi giờ New York -> UTC (aware). None nếu giờ mơ hồ hoặc không tồn tại."""
    if classify_local(local) != "ok":
        return None
    return local.replace(tzinfo=NY).astimezone(dt.timezone.utc)


def parse_local(s: str) -> dt.datetime:
    return dt.datetime.strptime(s[:19], LOCAL_FORMAT)


# ---------------- Spark ----------------
def dst_condition(local_ts_col, kind: str, years=(2025,)):
    """Biểu thức Spark: True nếu cột timestamp (giờ NY) rơi vào giờ DST loại kind."""
    from pyspark.sql import functions as F
    cond = F.lit(False)
    for y in years:
        for d, h in dst_local_hours(y)[kind]:
            cond = cond | ((F.to_date(local_ts_col) == F.lit(d)) & (F.hour(local_ts_col) == F.lit(h)))
    return cond


def with_event_time(df, local_col: str = "data_as_of", years=(2025,)):
    """Thêm: local_ts (TIMESTAMP giờ NY), event_time_utc (NULL nếu DST mơ hồ/không tồn tại),
    dst_ambiguous, dst_nonexistent. Không xóa dòng nào. Phiên Spark phải đặt spark.sql.session.timeZone=UTC."""
    from pyspark.sql import functions as F
    local_ts = F.to_timestamp(F.substring(F.col(local_col), 1, 19), "yyyy-MM-dd'T'HH:mm:ss")
    out = (df.withColumn("local_ts", local_ts)
             .withColumn("dst_ambiguous", dst_condition(F.col("local_ts"), "ambiguous", years))
             .withColumn("dst_nonexistent", dst_condition(F.col("local_ts"), "nonexistent", years)))
    return out.withColumn(
        "event_time_utc",
        F.when(F.col("dst_ambiguous") | F.col("dst_nonexistent") | F.col("local_ts").isNull(), F.lit(None).cast("timestamp"))
         .otherwise(F.to_utc_timestamp(F.col("local_ts"), TRAFFIC_TIMEZONE)))
