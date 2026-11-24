"""Unit test (Spark local, dữ liệu tổng hợp) cho feature engineering: lag khi thiếu cửa sổ, rolling, căn target, DST, trùng
bản ghi, availability thời tiết, lag mùa vụ.

Chạy được bằng pytest nếu máy có pyspark; container Spark không có pytest nên file tự chạy được:
  docker exec -e PYTHONPATH=/opt/spark/jobs traffic-spark-master /opt/spark/bin/spark-submit --master local[2] \
      /opt/spark/jobs/tests/ml/test_ml_spark_features.py
"""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

try:
    import pytest  # noqa: E402
except ImportError:                       # container Spark không có pytest: shim tối thiểu cho decorator fixture
    import types
    pytest = types.SimpleNamespace(fixture=lambda *a, **k: (lambda f: f))
    pytest.importorskip = lambda name: __import__(name)
pytest.importorskip("pyspark")
pytest.importorskip("common.aggregations")   # máy train chỉ có jobs/ml, không có code Spark dùng chung

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from ml.feature_engineering import baseline_asof, build_features, build_hist_5min, clean_records, target_steps  # noqa: E402

T0 = dt.datetime(2025, 6, 10, 12, 0)          # UTC
CFG = {"problem": {"window_minutes": 5, "horizon_minutes": 15, "availability_lag_minutes": 10, "sensitivity_lags": [0, 5],
                   "weather_lag_minutes": 60, "weather_sensitivity_lags": [0, 180]}}


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("test_ml_features")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s


def hist_from(spark, rows):
    """rows: (link_id, phút kể từ T0, n_valid, avg_speed_mph) -> bảng giống ml.traffic_5min_hist."""
    data = [(l, T0 + dt.timedelta(minutes=m), n, n, 0, a * n, a * a * n, 10 * n, a) for l, m, n, a in rows]
    cols = ["link_id", "window_start_utc", "n_records", "n_valid", "n_zero", "sum_speed_mph", "sum_speed_sq", "sum_travel_time_sec",
            "avg_speed_mph"]
    return spark.createDataFrame(data, cols)


def links_df(spark):
    return spark.createDataFrame([(1, "Queens", 40.7, -73.8, "c1", 1.0)],
                                 "link_id long, borough string, latitude double, longitude double, cell_id string, cell_distance_km double")


def weather_df(spark, hours=range(-3, 6)):
    return spark.createDataFrame([("c1", T0 + dt.timedelta(hours=h), 20.0 + h, 50.0, 10.0, 15.0, float(h), 0.0, 1.0) for h in hours],
                                 "cell_id string, weather_utc timestamp, temperature_2m double, relative_humidity_2m double, "
                                 "wind_speed_10m double, wind_gusts_10m double, precipitation double, snowfall double, weather_code double")


def feats(spark, rows, start=T0, end=None, weather=None):
    end = end or T0 + dt.timedelta(hours=3)
    f = build_features(spark, hist_from(spark, rows), links_df(spark), weather or weather_df(spark), CFG, start, end)
    return {r["last_window_start_utc"]: r for r in f.collect()}


def at(m):
    return T0 + dt.timedelta(minutes=m)


def test_lag_respects_missing_window(spark):
    # cửa sổ 10' bị thiếu: lag5 tại 15' phải NULL, lag10 = cửa sổ 5', không "dồn" cửa sổ 0' vào lag
    f = feats(spark, [(1, 0, 3, 40.0), (1, 5, 3, 41.0), (1, 15, 3, 43.0)])
    r = f[at(15)]
    assert r["speed_last"] == 43.0 and r["speed_lag5"] is None and r["speed_lag10"] == 41.0 and r["speed_lag15"] == 40.0
    assert r["is_missing_last"] == 0 and f[at(10)]["is_missing_last"] == 1 and f[at(10)]["speed_last"] is None
    assert f[at(10)]["speed_last_known_60"] == 41.0 and f[at(10)]["minutes_since_valid"] == 5.0


def test_rolling_mean_weighted_by_n_valid(spark):
    f = feats(spark, [(1, 0, 1, 30.0), (1, 5, 3, 50.0), (1, 10, 0, 0.0)])
    r = f[at(10)]
    assert abs(r["roll_mean_15"] - (30 * 1 + 50 * 3) / 4) < 1e-9 and r["n_valid_15"] == 4
    assert r["speed_last"] is None                          # n_valid = 0 -> không có tốc độ, không coi là 0


def test_target_alignment(spark):
    h = target_steps(15, 10)
    assert h == 5
    f = feats(spark, [(1, 0, 2, 40.0), (1, 25, 2, 33.0)])
    r = f[at(0)]
    assert r["origin_utc"] == at(15) and r["target_window_start_utc"] == at(25) and r["target_window_end_utc"] == at(30)
    assert r["target_speed_15m"] == 33.0 and r["data_cutoff_utc"] == at(5)
    assert r["target_speed_15m_a0"] is None                 # A=0 -> target là cửa sổ 15' (không có dữ liệu)


def test_seasonal_lag_is_past(spark):
    start = T0
    rows = [(1, -7 * 24 * 60 + 25, 2, 55.0), (1, 0, 2, 40.0)]
    f = build_features(spark, hist_from(spark, rows), links_df(spark), weather_df(spark), CFG,
                       start - dt.timedelta(days=8), start + dt.timedelta(hours=1))
    r = {x["last_window_start_utc"]: x for x in f.collect()}[at(0)]
    assert r["speed_same_target_last_week"] == 55.0          # = cửa sổ target (25') của 7 ngày trước, đã nằm trong quá khứ


def test_weather_point_in_time(spark):
    f = feats(spark, [(1, 0, 2, 40.0), (1, 60, 2, 40.0)])
    r = f[at(0)]                                             # data_cutoff 12:05 -> Lw=60: T* = 11:00; Lw=0: 12:00; Lw=180: 09:00
    assert r["w_record_utc"] == T0 - dt.timedelta(hours=1) and r["w_temp_c"] == 19.0
    assert r["w0_record_utc"] == T0 and r["w0_temp_c"] == 20.0
    assert r["w180_record_utc"] == T0 - dt.timedelta(hours=3)
    assert r["w_precip_3h_mm"] == (-3.0) + (-2.0) + (-1.0)   # mốc 09,10,11 (giá trị tổng hợp = h)
    assert r["eval_precip_target_mm"] == 1.0                 # mưa giờ chứa target (12:25) = mốc 13:00, CHỈ để đánh giá
    for row in f.values():
        assert row["w_record_utc"] + dt.timedelta(minutes=60) <= row["data_cutoff_utc"]


def test_dst_and_duplicates_in_records(spark):
    raw = spark.createDataFrame([
        (1, 7, 30.0, 100, "2025-11-02T01:30:00"),     # giờ lặp (mơ hồ) -> bỏ
        (2, 7, 30.0, 100, "2025-03-09T02:30:00"),     # giờ không tồn tại -> bỏ
        (3, 7, 30.0, 100, "2025-06-10T08:01:00"),     # EDT -> 12:01 UTC
        (4, 7, 32.0, 100, "2025-06-10T08:01:00"),     # trùng khoá (link, thời điểm) -> giữ id lớn nhất
        (5, 7, 0.0, 100, "2025-06-10T08:02:00"),      # speed = 0 -> có bản ghi nhưng không hợp lệ
    ], "id long, link_id long, speed double, travel_time long, data_as_of string")
    rec = clean_records(raw).collect()
    assert sorted(r["id"] for r in rec) == [4, 5]
    assert {r["id"]: r["event_time_utc"] for r in rec}[4] == dt.datetime(2025, 6, 10, 12, 1)
    h = build_hist_5min(clean_records(raw)).collect()
    assert len(h) == 1 and h[0]["n_records"] == 2 and h[0]["n_valid"] == 1 and h[0]["avg_speed_mph"] == 32.0


def test_calendar_uses_new_york_time(spark):
    f = feats(spark, [(1, 0, 2, 40.0)])
    r = f[at(0)]                                             # target 12:25 UTC = 08:25 EDT, thứ Ba 10/06/2025
    assert r["local_hour"] == 8 and r["day_of_week"] == 1 and r["is_peak"] == 1 and r["is_weekend"] == 0



def test_sensor_error_status_dropped(spark):
    # status = -101 (lỗi cảm biến) bị loại như Silver/Speed Layer; status NULL và 0 được giữ
    raw = spark.createDataFrame([
        (1, 7, 30.0, 100, "-101", "2025-06-10T08:01:00"),
        (2, 7, 31.0, 100, None, "2025-06-10T08:02:00"),
        (3, 7, 32.0, 100, "0", "2025-06-10T08:03:00"),
    ], "id long, link_id long, speed double, travel_time long, status string, data_as_of string")
    assert sorted(r["id"] for r in clean_records(raw).collect()) == [2, 3]


def test_baseline_matches_gold_dev_rule(spark):
    # 00:xx-05:xx New York; thời tiết tại MỐC ĐẦU GIỜ, mưa = 0 và tuyết = 0; speed > 0; P85 chính xác
    cfg = {"percentile": 0.85, "local_hours": [0, 1, 2, 3, 4, 5], "min_samples": 1, "max_precip_mm": 0}
    base = dt.datetime(2025, 6, 10, 4, 0)                      # 04:00 UTC = 00:00 EDT
    rows = [(7, base + dt.timedelta(minutes=m), float(s), True) for m, s in [(1, 40), (2, 50), (3, 60), (4, 0)]]
    rows += [(7, base + dt.timedelta(hours=1, minutes=1), 99.0, True)]   # giờ 01 EDT: mốc thời tiết 05:00 có mưa -> loại
    rows += [(7, base + dt.timedelta(hours=8), 99.0, True)]               # 08 EDT: ngoài khung đêm -> loại
    rec = spark.createDataFrame(rows, "link_id long, event_time_utc timestamp, speed double, is_valid boolean")
    links = spark.createDataFrame([(7, "c1")], "link_id long, cell_id string")
    wx = spark.createDataFrame([("c1", base, 0.0, 0.0), ("c1", base + dt.timedelta(hours=1), 0.4, 0.0)],
                               "cell_id string, weather_utc timestamp, precipitation double, snowfall double")
    r = baseline_asof(rec, links, wx, "2025-01-01", "2025-07-01", cfg).collect()[0]
    assert r["n_samples"] == 3                                 # 40, 50, 60 (speed 0 bị loại)
    assert abs(r["free_flow_speed"] - 57.0) < 1e-9             # percentile_cont(0.85) của [40, 50, 60] = 57

if __name__ == "__main__":
    s = (SparkSession.builder.master("local[2]").appName("test_ml_features").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    failed = 0
    for name, fn in sorted((k, v) for k, v in dict(globals()).items() if k.startswith("test_")):
        try:
            fn(s)
            print(f"PASS {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {name}: {type(e).__name__}: {e}")
    print(f"{'OK' if not failed else 'FAILED'}: {failed} failed")
    sys.exit(1 if failed else 0)
