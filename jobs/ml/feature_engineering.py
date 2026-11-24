"""Feature engineering (Spark) cho bài toán dự báo tốc độ 15 phút. Mọi quy tắc làm sạch/DST/gom 5 phút dùng LẠI code chung của
hệ thống (common/time_rules.py, common/quality_rules.py, common/aggregations.py) để dữ liệu ML cùng ngữ nghĩa với
gold.fact_traffic_5min và Speed Layer.

Quy ước thời gian (chi tiết: docs/ml/02_forecasting_design.md), A = availability lag, H = 15':
  - Cửa sổ quan sát [s, s+5') (UTC, căn epoch). Cửa sổ dùng được kể từ s + 5' + A.
  - Mỗi dòng feature gắn với cửa sổ có sẵn cuối cùng L: data_cutoff = L + 5', gốc dự báo t = L + 5' + A.
  - Target = avg_speed_mph của cửa sổ [t+10', t+15') (cửa sổ kết thúc đúng H phút sau gốc), = lead((H + A)/5) trên lưới.
  - Lưới đủ mọi cửa sổ 5' cho mọi link -> lag(k) luôn đúng k*5 phút, cửa sổ thiếu là NULL chứ không bị "dồn" lại.
  - Thời tiết mốc T (tức thời tại T, lượng mưa của (T-1h, T]) dùng được từ T + Lw: lấy T* = floor_hour(data_cutoff - Lw).
"""
import datetime as dt
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from common.aggregations import aggregate_5min, with_avg
from common.config import SILVER_TRAFFIC_CLEANED, SILVER_WEATHER_CLEANED
from common.quality_rules import sensor_ok_col, with_quality
from common.time_rules import TRAFFIC_TIMEZONE, with_event_time

STEP = 300                                    # giây / cửa sổ
WEEK_STEPS, DAY_STEPS = 7 * 288, 288
LAG_STEPS = {"speed_lag5": 1, "speed_lag10": 2, "speed_lag15": 3, "speed_lag30": 6, "speed_lag60": 12}
ROLL_STEPS = {"15": 3, "30": 6, "60": 12}
WEATHER_COLS = {"temperature_2m": "w_temp_c", "relative_humidity_2m": "w_rh_pct", "wind_speed_10m": "w_wind_kmh",
                "wind_gusts_10m": "w_gust_kmh", "precipitation": "w_precip_1h_mm", "snowfall": "w_snow_1h_cm",
                "weather_code": "w_code"}
# Ngày lễ liên bang Mỹ 2025 (OPM federal holidays 2025) - lịch cố định, biết trước nên không phải leakage
US_HOLIDAYS_2025 = ["2025-01-01", "2025-01-20", "2025-02-17", "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01",
                    "2025-10-13", "2025-11-11", "2025-11-27", "2025-12-25"]


def target_steps(horizon_min: int, avail_min: int, window_min: int = 5) -> int:
    """Số bước (cửa sổ) từ cửa sổ có sẵn cuối cùng L tới cửa sổ target [t+H-5', t+H')."""
    assert horizon_min % window_min == 0 and avail_min % window_min == 0
    return (horizon_min + avail_min) // window_min


# ------------------------------------------------------------------ nguồn lịch sử
# Mặc định đọc SILVER (data.source = silver trong conf/ml/forecast.yaml): dữ liệu đã được Batch Layer làm sạch, đổi giờ
# sang UTC, bỏ giờ DST mơ hồ và khử trùng -> ML dùng đúng dữ liệu của cả nhóm, không giữ bản làm sạch riêng.
# Silver chỉ có những ngày đã đi qua Kafka; muốn đủ cả năm thì phát lại dữ liệu vào Kafka trước, hoặc tạm dùng
# data.source = raw (đọc file thô bằng CÙNG hàm làm sạch, xem traffic_records_from_raw).
def traffic_records_from_silver(spark: SparkSession, lo: dt.datetime | None = None, hi: dt.datetime | None = None) -> DataFrame:
    """silver.traffic_cleaned -> link_id, event_time_utc, speed, travel_time, is_valid, invalid_reason.
    Silver đã khử trùng theo (link_id, traffic_utc) và loại giờ DST (nằm ở silver.traffic_quarantine)."""
    df = spark.table(SILVER_TRAFFIC_CLEANED)
    if lo is not None:
        # Silver phân vùng theo ngày New York: lọc rộng theo date trước (partition pruning) rồi lọc chính xác theo UTC
        df = df.filter((F.col("date") >= F.lit((lo - dt.timedelta(days=1)).date())) & (F.col("traffic_utc") >= F.lit(lo)))
    if hi is not None:
        df = df.filter((F.col("date") <= F.lit((hi + dt.timedelta(days=1)).date())) & (F.col("traffic_utc") < F.lit(hi)))
    # Loại status = -101 (lỗi cảm biến) như Silver mới / Speed Layer; Silver dựng bằng code cũ vẫn còn các dòng này
    df = df.filter(sensor_ok_col("status"))
    df = df.select(F.col("link_id").cast("bigint").alias("link_id"), F.col("traffic_utc").alias("event_time_utc"),
                   F.col("speed").cast("double").alias("speed"), F.col("travel_time").cast("bigint").alias("travel_time"))
    return with_quality(df.filter(F.col("link_id").isNotNull() & F.col("event_time_utc").isNotNull()), "speed", "travel_time")


def weather_from_silver(spark: SparkSession) -> DataFrame:
    """silver.weather_cleaned -> cell_id, weather_utc, các biến thời tiết (cùng tên/kiểu với weather_from_raw)."""
    return spark.table(SILVER_WEATHER_CLEANED).select("cell_id", "weather_utc",
                                                      *[F.col(c).cast("double").alias(c) for c in WEATHER_COLS])


def traffic_records(spark: SparkSession, cfg: dict, source: str, lo: dt.datetime | None = None,
                    hi: dt.datetime | None = None) -> DataFrame:
    """Bản ghi lịch sử theo nguồn đã chọn (silver | raw), cùng cột cho cả hai."""
    if source == "silver":
        return traffic_records_from_silver(spark, lo, hi)
    rec = traffic_records_from_raw(spark, cfg["data"]["raw_traffic_glob"])
    if lo is not None:
        rec = rec.filter(F.col("event_time_utc") >= F.lit(lo))
    if hi is not None:
        rec = rec.filter(F.col("event_time_utc") < F.lit(hi))
    return rec


def weather_hist(spark: SparkSession, cfg: dict, source: str) -> DataFrame:
    return weather_from_silver(spark) if source == "silver" else weather_from_raw(spark, cfg["data"]["raw_weather_dir"])


def traffic_records_from_raw(spark: SparkSession, glob: str) -> DataFrame:
    """Bản ghi NYC DOT thô -> link_id, event_time_utc (NULL nếu giờ DST mơ hồ/không tồn tại), speed, travel_time, is_valid.
    Parquet lưu data_as_of dạng TIMESTAMP_NANOS (giờ New York, không offset) -> đọc thành số nguyên rồi đổi sang chuỗi
    'yyyy-MM-ddTHH:mm:ss' đúng như chuỗi producer gửi vào Kafka, sau đó dùng chung time_rules.with_event_time."""
    raw = spark.read.parquet(glob)
    local_str = F.date_format(F.expr("timestamp_micros(data_as_of div 1000)"), "yyyy-MM-dd'T'HH:mm:ss")   # session TZ = UTC
    return clean_records(raw.select("id", "link_id", "speed", "travel_time", "status", local_str.alias("data_as_of")))


def clean_records(df: DataFrame) -> DataFrame:
    """id, link_id, speed, travel_time, status, data_as_of ('yyyy-MM-ddTHH:mm:ss' giờ New York) -> bản ghi sạch, khử trùng.
    Cùng luật với Silver: loại status = -101 (giữ NULL), giờ DST mơ hồ/không tồn tại, khử trùng theo (link_id, event_time)."""
    if "status" in df.columns:
        df = df.filter(sensor_ok_col("status"))
    df = df.select(F.col("id").cast("bigint").alias("id"), F.col("link_id").cast("bigint").alias("link_id"),
                   F.col("speed").cast("double").alias("speed"), F.col("travel_time").cast("bigint").alias("travel_time"),
                   F.col("data_as_of").cast("string").alias("data_as_of"))
    df = with_quality(with_event_time(df, "data_as_of"), "speed", "travel_time")
    w = Window.partitionBy("link_id", "event_time_utc").orderBy(F.col("id").desc())   # khoá nghiệp vụ như Silver
    return (df.filter(F.col("event_time_utc").isNotNull() & F.col("link_id").isNotNull())
              .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn"))


def build_hist_5min(records: DataFrame) -> DataFrame:
    """Cùng công thức gold.fact_traffic_5min (batch_views.build: sum_speed_mph, sum_travel_time_sec, ...) + avg_speed_mph."""
    return with_avg(aggregate_5min(records, "event_time_utc")).withColumnRenamed("window_start", "window_start_utc")


def weather_from_raw(spark: SparkSession, raw_dir: str) -> DataFrame:
    """16 file Open-Meteo (ECMWF IFS, giờ GMT) -> cell_id, weather_utc, các biến. cell_id = tên file bỏ 'ifs_' (như producer)."""
    rows = []
    for path in sorted(Path(raw_dir).glob("ifs_*.json")):
        d = json.loads(path.read_text())
        h, cell = d["hourly"], path.stem.removeprefix("ifs_")
        for i, t in enumerate(h["time"]):
            rows.append((cell, dt.datetime.strptime(t, "%Y-%m-%dT%H:%M"),
                         *(None if h[c][i] is None else float(h[c][i]) for c in WEATHER_COLS)))
    schema = "cell_id string, weather_utc timestamp, " + ", ".join(f"{c} double" for c in WEATHER_COLS)
    return spark.createDataFrame(rows, schema=schema)


# ------------------------------------------------------------------ free-flow theo thời điểm (point-in-time)
def baseline_asof(records: DataFrame, links: DataFrame, weather: DataFrame, history_start: str, as_of: str, cfg: dict) -> DataFrame:
    """Cùng công thức free-flow của gold.dim_road_link (silver_to_gold.road_metrics_sql, gold-dev): P85 chính xác
    (percentile = percentile_cont) của speed DƯƠNG, giờ New York trong cfg.local_hours (00-05), giờ có mưa = 0 và tuyết = 0
    (thời tiết nối tại MỐC ĐẦU GIỜ: date_trunc(hour) = weather_utc). Khác dim_road_link duy nhất ở chỗ CHỈ dùng bản ghi có
    event_time_utc trong [history_start, as_of) -> dùng được cho mọi gốc dự báo >= as_of mà không nhìn trước."""
    lo, hi = dt.datetime.fromisoformat(history_start), dt.datetime.fromisoformat(as_of)
    t = (records.filter(F.col("speed") > 0)
                .filter((F.col("event_time_utc") >= F.lit(lo)) & (F.col("event_time_utc") < F.lit(hi)))
                .withColumn("local_hour", F.hour(F.from_utc_timestamp("event_time_utc", TRAFFIC_TIMEZONE)))
                .filter(F.col("local_hour").isin(*cfg["local_hours"]))
                .withColumn("weather_utc", F.date_trunc("hour", "event_time_utc")))
    w = weather.select("cell_id", "weather_utc", "precipitation", "snowfall")
    dry = (t.join(F.broadcast(links.select("link_id", "cell_id")), "link_id").join(w, ["cell_id", "weather_utc"])
            .filter((F.col("precipitation") <= cfg["max_precip_mm"]) & (F.col("snowfall") == 0)))
    return (dry.groupBy("link_id")
               .agg(F.expr(f"percentile(speed, {cfg['percentile']})").alias("free_flow_speed"), F.count("*").alias("n_samples"))
               .withColumn("free_flow_speed", F.when(F.col("n_samples") >= cfg["min_samples"], F.col("free_flow_speed")))
               .withColumn("as_of_utc", F.lit(hi)).withColumn("history_start_utc", F.lit(lo)))


# ------------------------------------------------------------------ lưới + feature
def time_grid(spark: SparkSession, links: DataFrame, start: dt.datetime, end: dt.datetime) -> DataFrame:
    s, e = int(start.replace(tzinfo=dt.timezone.utc).timestamp()), int(end.replace(tzinfo=dt.timezone.utc).timestamp())
    ws = spark.range(s, e, STEP).select(F.timestamp_seconds("id").alias("window_start_utc"))
    return links.select("link_id").crossJoin(ws)


def weather_features(weather: DataFrame) -> DataFrame:
    """Thêm tổng mưa 3 giờ (mốc T-2h..T) - lưới thời tiết theo giờ được kiểm tra liên tục trong validate_features."""
    w3 = Window.partitionBy("cell_id").orderBy("weather_utc").rowsBetween(-2, 0)
    out = weather.withColumn("w_precip_3h_mm", F.sum("precipitation").over(w3))
    return out.select("cell_id", "weather_utc", *[F.col(c).alias(a) for c, a in WEATHER_COLS.items()], "w_precip_3h_mm")


def join_weather(df: DataFrame, wf: DataFrame, cutoff_col: str, lag_min: int, prefix: str, cols: list[str]) -> DataFrame:
    """Gắn bản ghi thời tiết mới nhất ĐÃ có tại cutoff: T* = floor_hour(cutoff - lag). Đổi tên cột theo prefix."""
    key = f"{prefix}record_utc"
    df = df.withColumn(key, F.date_trunc("hour", F.col(cutoff_col) - F.expr(f"INTERVAL {lag_min} MINUTES")))
    sel = wf.select(F.col("cell_id").alias("_cell"), F.col("weather_utc").alias(key), *[F.col(c).alias(prefix + c[2:]) for c in cols])
    return df.join(sel, (df.cell_id == sel._cell) & (df[key] == sel[key]), "left").drop("_cell").drop(sel[key])


def build_features(spark: SparkSession, hist5: DataFrame, links: DataFrame, weather: DataFrame, cfg: dict,
                   start: dt.datetime, end: dt.datetime) -> DataFrame:
    p = cfg["problem"]
    A, H, Lw = p["availability_lag_minutes"], p["horizon_minutes"], p["weather_lag_minutes"]
    h = target_steps(H, A)
    grid = time_grid(spark, links, start, end)
    g = (grid.join(hist5.select("link_id", "window_start_utc", "n_records", "n_valid", "sum_speed_mph", "sum_speed_sq",
                                "avg_speed_mph"), ["link_id", "window_start_utc"], "left")
             .withColumn("n_records", F.coalesce("n_records", F.lit(0)))
             .withColumn("n_valid", F.coalesce("n_valid", F.lit(0)))
             .withColumn("sum_speed_mph", F.coalesce("sum_speed_mph", F.lit(0.0)))
             .withColumn("sum_speed_sq", F.coalesce("sum_speed_sq", F.lit(0.0)))
             .withColumn("spd", F.when(F.col("n_valid") > 0, F.col("avg_speed_mph")))
             .withColumn("is_missing", (F.col("n_valid") == 0).cast("int")))
    w = Window.partitionBy("link_id").orderBy("window_start_utc")
    roll = lambda k: w.rowsBetween(-(k - 1), 0)
    g = g.withColumn("speed_last", F.col("spd"))
    for name, k in LAG_STEPS.items():
        g = g.withColumn(name, F.lag("spd", k).over(w))
    for name, k in ROLL_STEPS.items():
        n, s, sq = F.sum("n_valid").over(roll(k)), F.sum("sum_speed_mph").over(roll(k)), F.sum("sum_speed_sq").over(roll(k))
        mean = F.when(n > 0, s / n)
        g = g.withColumn(f"roll_mean_{name}", mean).withColumn(f"n_valid_{name}", n)
        if name != "15":
            g = g.withColumn(f"roll_std_{name}", F.when(n > 1, F.sqrt(F.greatest(sq / n - mean * mean, F.lit(0.0)))))
    last_valid_ts = F.last(F.when(F.col("n_valid") > 0, F.col("window_start_utc")), ignorenulls=True).over(
        w.rowsBetween(Window.unboundedPreceding, 0))
    g = (g.withColumn("n_missing_60", F.sum("is_missing").over(roll(12)))
          .withColumn("minutes_since_valid", F.least(F.lit(1440.0), (F.unix_timestamp("window_start_utc") - F.unix_timestamp(last_valid_ts)) / 60.0))
          .withColumn("speed_last_known_60", F.last("spd", ignorenulls=True).over(roll(12)))
          .withColumn("speed_diff_5", F.col("speed_last") - F.col("speed_lag5"))
          .withColumn("speed_diff_15", F.col("speed_last") - F.col("speed_lag15"))
          .withColumn("trend_15_60", F.col("roll_mean_15") - F.col("roll_mean_60"))
          .withColumn("n_valid_last", F.col("n_valid")).withColumn("n_records_last", F.col("n_records"))
          .withColumn("is_missing_last", F.col("is_missing"))
          # cùng cửa sổ target của tuần trước / hôm qua: luôn nằm trong quá khứ so với gốc dự báo
          .withColumn("speed_same_target_last_week", F.lag("spd", WEEK_STEPS - h).over(w))
          .withColumn("speed_same_target_yesterday", F.lag("spd", DAY_STEPS - h).over(w))
          .withColumn("target_speed_15m", F.lead("spd", h).over(w))
          .withColumn("target_n_valid", F.lead("n_valid", h).over(w)))
    for a in p["sensitivity_lags"]:
        ha = target_steps(H, a)
        g = (g.withColumn(f"target_speed_15m_a{a}", F.lead("spd", ha).over(w))
              .withColumn(f"speed_same_target_last_week_a{a}", F.lag("spd", WEEK_STEPS - ha).over(w)))
    g = (g.withColumn("data_cutoff_utc", F.col("window_start_utc") + F.expr("INTERVAL 5 MINUTES"))
          .withColumn("origin_utc", F.col("data_cutoff_utc") + F.expr(f"INTERVAL {A} MINUTES"))
          .withColumn("target_window_start_utc", F.col("origin_utc") + F.expr(f"INTERVAL {H - 5} MINUTES"))
          .withColumn("target_window_end_utc", F.col("origin_utc") + F.expr(f"INTERVAL {H} MINUTES"))
          .withColumnRenamed("window_start_utc", "last_window_start_utc"))
    g = add_calendar(g, "target_window_start_utc")
    g = g.join(F.broadcast(links.select("link_id", "borough", "latitude", "longitude", "cell_id", "cell_distance_km")), "link_id", "left")
    wf = weather_features(weather)
    main_cols = ["w_temp_c", "w_rh_pct", "w_wind_kmh", "w_gust_kmh", "w_precip_1h_mm", "w_snow_1h_cm", "w_code", "w_precip_3h_mm"]
    g = join_weather(g, wf, "data_cutoff_utc", Lw, "w_", main_cols)
    g = g.withColumn("weather_missing", F.col("w_temp_c").isNull().cast("int"))
    g = g.withColumn("weather_age_min", (F.unix_timestamp("origin_utc") - F.unix_timestamp("w_record_utc")) / 60.0)
    sens_cols = ["w_temp_c", "w_rh_pct", "w_wind_kmh", "w_precip_1h_mm", "w_precip_3h_mm"]
    for lw in p["weather_sensitivity_lags"]:
        g = join_weather(g, wf, "data_cutoff_utc", lw, f"w{lw}_", sens_cols)
    # CHỈ để đánh giá (mưa trong giờ chứa cửa sổ target, theo quy tắc FINAL [H,H+1) -> mốc H+1); không bao giờ là feature
    ev = wf.select(F.col("cell_id").alias("_c"), F.col("weather_utc").alias("_t"), F.col("w_precip_1h_mm").alias("eval_precip_target_mm"))
    g = g.withColumn("_tk", F.date_trunc("hour", "target_window_start_utc") + F.expr("INTERVAL 1 HOUR"))
    g = g.join(ev, (g.cell_id == ev._c) & (g._tk == ev._t), "left").drop("_c", "_t", "_tk")
    return g.withColumn("origin_month", F.date_format("origin_utc", "yyyy-MM"))


def add_calendar(df: DataFrame, ts_col: str) -> DataFrame:
    """Lịch sinh hoạt theo giờ New York của cửa sổ target (biết trước tại gốc dự báo)."""
    loc = F.from_utc_timestamp(F.col(ts_col), TRAFFIC_TIMEZONE)
    mod = F.hour(loc) * 60 + F.minute(loc)
    dow = (F.dayofweek(loc) + 5) % 7                               # 0 = thứ Hai ... 6 = Chủ nhật
    hol = F.date_format(loc, "yyyy-MM-dd").isin(*US_HOLIDAYS_2025)
    weekday = dow < 5
    peak = weekday & ~hol & (((mod >= 7 * 60) & (mod < 10 * 60)) | ((mod >= 16 * 60) & (mod < 19 * 60)))
    two_pi = 2 * 3.141592653589793
    return (df.withColumn("local_hour", F.hour(loc)).withColumn("minute_of_day", mod).withColumn("day_of_week", dow)
              .withColumn("is_weekend", (~weekday).cast("int")).withColumn("is_holiday", hol.cast("int"))
              .withColumn("is_peak", peak.cast("int"))
              .withColumn("tod_sin", F.sin(mod * two_pi / 1440)).withColumn("tod_cos", F.cos(mod * two_pi / 1440))
              .withColumn("dow_sin", F.sin(dow * two_pi / 7)).withColumn("dow_cos", F.cos(dow * two_pi / 7)))
