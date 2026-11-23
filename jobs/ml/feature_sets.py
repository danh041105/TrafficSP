"""Nhóm feature, feature phụ thuộc fold (free-flow point-in-time + tỷ lệ) và nạp dữ liệu export (pandas, dùng cả lúc train lẫn
inference -> cùng một định nghĩa, tránh train/serve skew)."""
from pathlib import Path

import numpy as np
import pandas as pd

TRAFFIC = ["speed_last", "speed_lag5", "speed_lag10", "speed_lag15", "speed_lag30", "speed_lag60",
           "roll_mean_15", "roll_mean_30", "roll_mean_60", "roll_std_30", "roll_std_60",
           "n_valid_15", "n_valid_30", "n_valid_60", "n_missing_60", "minutes_since_valid", "speed_last_known_60",
           "speed_diff_5", "speed_diff_15", "trend_15_60", "n_valid_last", "n_records_last", "is_missing_last",
           "speed_same_target_last_week", "speed_same_target_yesterday"]
TEMPORAL = ["local_hour", "minute_of_day", "day_of_week", "is_weekend", "is_holiday", "is_peak",
            "tod_sin", "tod_cos", "dow_sin", "dow_cos"]
SPATIAL_STATIC = ["link_code", "borough_code", "latitude", "longitude"]
SPATIAL_FOLD = ["free_flow_speed", "ratio_last_ff", "ratio_roll60_ff", "ratio_lastweek_ff"]
SPATIAL = SPATIAL_STATIC + SPATIAL_FOLD
WEATHER = ["w_temp_c", "w_rh_pct", "w_wind_kmh", "w_gust_kmh", "w_precip_1h_mm", "w_snow_1h_cm", "w_precip_3h_mm",
           "w_code", "weather_missing", "weather_age_min"]
WEATHER_CORE = ["temp_c", "rh_pct", "wind_kmh", "precip_1h_mm", "precip_3h_mm"]     # dùng cho thí nghiệm độ nhạy Lw
CATEGORICAL = ["link_code", "borough_code"]
# Cột chỉ để đánh giá / định danh - KHÔNG BAO GIỜ là feature (test_ml_leakage.py kiểm tra)
EVAL_ONLY = ["eval_precip_target_mm", "target_speed_15m", "target_n_valid", "target_speed_15m_a0", "target_speed_15m_a5",
             "speed_same_target_last_week_a0", "speed_same_target_last_week_a5", "origin_utc", "target_window_start_utc",
             "target_window_end_utc", "data_cutoff_utc", "last_window_start_utc", "origin_month", "link_id"]

GROUPS = {"traffic": TRAFFIC, "traffic_temporal": TRAFFIC + TEMPORAL, "traffic_temporal_spatial": TRAFFIC + TEMPORAL + SPATIAL,
          "all": TRAFFIC + TEMPORAL + SPATIAL + WEATHER}
BOROUGHS = ["Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"]

LOAD_COLS = sorted(set(TRAFFIC + TEMPORAL + ["latitude", "longitude", "borough"] + WEATHER
                       + [f"w{l}_{c}" for l in (0, 180) for c in WEATHER_CORE]
                       + [c for c in EVAL_ONLY if c not in ("origin_month",)]))


def weather_set(lag: int) -> list[str]:
    pre = "w_" if lag == 60 else f"w{lag}_"
    return [pre + c for c in WEATHER_CORE]


def load_features(root: str, start: str, end: str, columns=None, require_any_target: bool = False) -> pd.DataFrame:
    """Đọc Parquet export (thư mục phân vùng origin_month=YYYY-MM) cho các tháng chạm [start, end).
    require_any_target: bỏ ngay ở tầng Arrow các dòng không có target nào (không train/đánh giá được) -> tiết kiệm RAM."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    months = pd.period_range(pd.Timestamp(start), pd.Timestamp(end) - pd.Timedelta(seconds=1), freq="M").strftime("%Y-%m")
    import ctypes
    cols = columns or LOAD_COLS
    # RAM máy train hạn chế: allocator hệ thống + malloc_trim để trả bộ nhớ ngay; ghép ở tầng Arrow (không copy) rồi
    # to_pandas(self_destruct) từng cột -> đỉnh RAM ~ 1,3 x kích thước dữ liệu (pd.concat sẽ cần ~2-3 x)
    pa.set_memory_pool(pa.system_memory_pool())
    try:
        trim = ctypes.CDLL("libc.so.6").malloc_trim
    except OSError:                                                  # không phải Linux
        trim = lambda _: None  # noqa: E731
    lo, hi = pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC")
    tables = []
    for m in months:
        d = Path(root) / f"origin_month={m}"
        if not d.exists():
            continue
        t = pq.read_table(d, columns=cols, pre_buffer=False, read_dictionary=[c for c in ("borough",) if c in cols])
        if require_any_target:
            tc = [c for c in cols if c.startswith("target_speed_15m")]
            keep = pc.is_valid(t[tc[0]])
            for c in tc[1:]:
                keep = pc.or_(keep, pc.is_valid(t[c]))
            t = t.filter(keep)
        tables.append(t.combine_chunks())
        del t
        trim(0)
    table = pa.concat_tables(tables)
    del tables
    df = table.to_pandas(self_destruct=True, split_blocks=True)
    del table
    trim(0)
    for c in ("origin_utc", "target_window_start_utc", "target_window_end_utc", "data_cutoff_utc", "last_window_start_utc"):
        if c in df:
            df[c] = pd.to_datetime(df[c], utc=True)
    if "borough" in df:
        df["borough"] = pd.Categorical(df["borough"].astype(str), categories=BOROUGHS)
    keep = (df["origin_utc"] >= lo) & (df["origin_utc"] < hi)
    if not keep.all():
        df = df[keep].reset_index(drop=True)
    return df


def add_static_codes(df: pd.DataFrame, link_index: dict) -> pd.DataFrame:
    """link_id -> mã số nguyên ổn định (lưu trong metadata model); link lạ -> -1 (LightGBM đi nhánh phải)."""
    df["link_code"] = df["link_id"].map(link_index).fillna(-1).astype("int32")
    df["borough_code"] = df["borough"].astype(str).map({b: i for i, b in enumerate(BOROUGHS)}).fillna(-1).astype("int32")
    return df


def add_fold_features(df: pd.DataFrame, baseline: pd.DataFrame, as_of) -> pd.DataFrame:
    """Free-flow tính CHỈ từ lịch sử trước as_of (bảng ml.link_baseline_asof) + các tỷ lệ tốc độ/free-flow."""
    b = baseline[pd.to_datetime(baseline["as_of_utc"], utc=True) == pd.Timestamp(as_of)]
    ff = b.set_index("link_id")["free_flow_speed"]
    out = df.copy() if "free_flow_speed" in df else df
    out["free_flow_speed"] = out["link_id"].map(ff).astype("float32")
    f = out["free_flow_speed"].where(out["free_flow_speed"] > 0)
    out["ratio_last_ff"] = (out["speed_last_known_60"] / f).astype("float32")
    out["ratio_roll60_ff"] = (out["roll_mean_60"] / f).astype("float32")
    out["ratio_lastweek_ff"] = (out["speed_same_target_last_week"] / f).astype("float32")
    return out


US_HOLIDAYS_2025 = {"2025-01-01", "2025-01-20", "2025-02-17", "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01",
                    "2025-10-13", "2025-11-11", "2025-11-27", "2025-12-25"}


def calendar_pandas(target_start_utc: pd.Series) -> pd.DataFrame:
    """Bản pandas của feature_engineering.add_calendar (dùng cho thí nghiệm độ nhạy A, khi cửa sổ target dịch đi)."""
    loc = target_start_utc.dt.tz_convert("America/New_York")
    mod = loc.dt.hour * 60 + loc.dt.minute
    dow = loc.dt.dayofweek
    hol = loc.dt.strftime("%Y-%m-%d").isin(US_HOLIDAYS_2025)
    weekday = dow < 5
    peak = weekday & ~hol & (((mod >= 420) & (mod < 600)) | ((mod >= 960) & (mod < 1140)))
    return pd.DataFrame({"local_hour": loc.dt.hour, "minute_of_day": mod, "day_of_week": dow,
                         "is_weekend": (~weekday).astype(int), "is_holiday": hol.astype(int), "is_peak": peak.astype(int),
                         "tod_sin": np.sin(mod * 2 * np.pi / 1440), "tod_cos": np.cos(mod * 2 * np.pi / 1440),
                         "dow_sin": np.sin(dow * 2 * np.pi / 7), "dow_cos": np.cos(dow * 2 * np.pi / 7)})


def ff_map(baseline: pd.DataFrame, as_of) -> pd.Series:
    b = baseline[pd.to_datetime(baseline["as_of_utc"], utc=True) == pd.Timestamp(as_of)]
    return b.set_index("link_id")["free_flow_speed"]


def build_matrix(df: pd.DataFrame, idx: np.ndarray, feats: list[str], ffm: pd.Series) -> np.ndarray:
    """Giống feature_sets.add_fold_features (free-flow as_of + tỷ lệ) nhưng ghi thẳng vào mảng float32."""
    ff = pd.Series(df["link_id"].to_numpy()[idx]).map(ffm).to_numpy(np.float32)
    fpos = np.where(ff > 0, ff, np.nan).astype(np.float32)
    col = lambda c: df[c].to_numpy()[idx]
    special = {"free_flow_speed": lambda: ff, "ratio_last_ff": lambda: col("speed_last_known_60") / fpos,
               "ratio_roll60_ff": lambda: col("roll_mean_60") / fpos,
               "ratio_lastweek_ff": lambda: col("speed_same_target_last_week") / fpos}
    X = np.empty((len(idx), len(feats)), np.float32)
    for j, c in enumerate(feats):
        X[:, j] = special[c]() if c in special else col(c)
    return X
