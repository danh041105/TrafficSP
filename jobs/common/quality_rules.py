# quality_rules.py: quy tắc chất lượng traffic dùng CHUNG cho Batch và Speed Layer (D-05 nhánh khai).
#   - Hợp lệ: 0 < speed <= 100 mph và travel_time > 0.
#   - speed = 0 là "có bản ghi nhưng không có số đo", KHÁC với "không có bản ghi" (missing observation).
#   - Không sửa giá trị gốc; chỉ thêm is_valid + invalid_reason.
#   - Trạng thái giao thông (D-18, quy tắc phân tích): tỷ lệ speed / free_flow_speed của từng link.
SPEED_MAX_MPH = 100.0
CONGESTED_LT = 0.5
SLOW_LT = 0.8

# Thứ tự ưu tiên khi một bản ghi có nhiều lỗi
INVALID_REASONS = ("null_value", "speed_negative", "speed_zero_tt_positive", "speed_zero",
                   "speed_gt_max", "travel_time_nonpositive")

# Trạng thái (dùng chung latest / 5 phút / batch)
STATE_CONGESTED = "CONGESTED"
STATE_SLOW = "SLOW"
STATE_FREE_FLOW = "FREE_FLOW"
STATE_NO_BASELINE = "NO_BASELINE"          # link chưa có free-flow (thiếu lịch sử hợp lệ)
STATE_NO_VALID_DATA = "NO_VALID_DATA"      # có bản ghi nhưng không có số đo hợp lệ (vd speed = 0)
STATE_INSUFFICIENT = "INSUFFICIENT_SAMPLES"
STATE_STALE = "STALE"                      # tính lúc truy vấn: quan trắc cuối quá cũ so với đồng hồ sự kiện
STATE_NO_DATA = "NO_DATA"                  # không có bản ghi trong cửa sổ (không suy ra speed = 0)


def invalid_reason(speed, travel_time) -> str | None:
    if speed is None or travel_time is None:
        return "null_value"
    if speed < 0:
        return "speed_negative"
    if speed == 0 and travel_time > 0:
        return "speed_zero_tt_positive"
    if speed == 0:
        return "speed_zero"
    if speed > SPEED_MAX_MPH:
        return "speed_gt_max"
    if travel_time <= 0:
        return "travel_time_nonpositive"
    return None


def traffic_state(speed, free_flow, n_valid: int = 1, min_samples: int = 1) -> str:
    if free_flow is None or free_flow <= 0:
        return STATE_NO_BASELINE
    if n_valid == 0 or speed is None:
        return STATE_NO_VALID_DATA
    if n_valid < min_samples:
        return STATE_INSUFFICIENT
    r = speed / free_flow
    return STATE_CONGESTED if r < CONGESTED_LT else (STATE_SLOW if r < SLOW_LT else STATE_FREE_FLOW)


# ---------------- Spark ----------------
def invalid_reason_col(speed_col="speed", tt_col="travel_time"):
    from pyspark.sql import functions as F
    s, t = F.col(speed_col), F.col(tt_col)
    return (F.when(s.isNull() | t.isNull(), "null_value")
             .when(s < 0, "speed_negative")
             .when((s == 0) & (t > 0), "speed_zero_tt_positive")
             .when(s == 0, "speed_zero")
             .when(s > SPEED_MAX_MPH, "speed_gt_max")
             .when(t <= 0, "travel_time_nonpositive"))


def with_quality(df, speed_col="speed", tt_col="travel_time"):
    from pyspark.sql import functions as F
    return (df.withColumn("invalid_reason", invalid_reason_col(speed_col, tt_col))
              .withColumn("is_valid", F.col("invalid_reason").isNull()))


def traffic_state_col(speed_col, free_flow_col, n_valid_col=None, min_samples: int = 1):
    from pyspark.sql import functions as F
    sp, ff = F.col(speed_col), F.col(free_flow_col)
    expr = F.when(ff.isNull() | (ff <= 0), STATE_NO_BASELINE)
    if n_valid_col is not None:
        nv = F.col(n_valid_col)
        expr = expr.when(nv == 0, STATE_NO_VALID_DATA).when(nv < min_samples, STATE_INSUFFICIENT)
    return (expr.when(sp.isNull(), STATE_NO_VALID_DATA)
                .when(sp / ff < CONGESTED_LT, STATE_CONGESTED)
                .when(sp / ff < SLOW_LT, STATE_SLOW)
                .otherwise(STATE_FREE_FLOW))
