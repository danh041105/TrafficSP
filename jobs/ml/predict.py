"""Inference dự báo tốc độ 15' -> Iceberg lakehouse.ml.traffic_forecast_15m (Trino: serving.v_traffic_forecast_15m*).

  spark-submit predict.py --model-dir /opt/lakehouse/ml_models/<version> [--origin 2025-12-18T23:10] [--source serving|hist]

  --source serving (mặc định): đọc dữ liệu 5' HỢP NHẤT Batch + Speed từ Trino (serving.v_traffic_5min_unified, đã xử lý batch_state)
           và thời tiết từ serving.v_weather_hourly. Không tự xử lý lại batch_state.
  --source hist: đọc ml.traffic_5min_hist / ml.weather_hourly_hist (dùng cho kiểm tra tái lập với kết quả backtest).

Quy trình: chọn gốc dự báo t (mặc định: cửa sổ HOÀN TẤT mới nhất L trong view, t = L + 5' + A) -> dựng feature bằng ĐÚNG hàm
Spark lúc train (feature_engineering.build_features) trên lưới [L-8 ngày, L] -> feature phụ thuộc fold bằng ĐÚNG hàm pandas lúc train
(feature_sets) với free-flow as_of ghi trong metadata model -> chấm điểm bằng NumpyBooster -> MERGE INTO theo khoá
(link_id, prediction_origin_utc, model_version): chạy lại bao nhiêu lần cũng chỉ có một dòng mỗi khoá.
"""
import argparse
import datetime as dt
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from common.config import SILVER_LINK_INFO
from common.spark_session import build_spark
from common.trino_http import TRINO_URL, query
from ml.feature_engineering import build_features
from ml.feature_sets import add_fold_features, add_static_codes
from ml.forecast_table import FORECAST_DDL
from ml.lgbm_numpy import NumpyBooster, model_file
from ml.metrics import traffic_state
from ml.mlconfig import load_config

KEY = ["link_id", "prediction_origin_utc", "model_version"]


def floor5(t: dt.datetime) -> dt.datetime:
    return t.replace(second=0, microsecond=0, minute=t.minute - t.minute % 5)


def trino(sql):
    return query(sql, url=TRINO_URL, user="ml_predict")


# Làn dữ liệu: main = Serving Layer chính; demo = phiên phát lại kiểm chứng (scripts/demo_replay.py). Đặt bởi set_lane().
UNIFIED_VIEW = "lakehouse.serving.v_traffic_5min_unified"
WEATHER_VIEW = "lakehouse.serving.v_weather_hourly"


def set_lane(cfg: dict, lane: str) -> str:
    """Chọn view đọc + bảng ghi theo làn (conf/ml/forecast.yaml mục lanes). Trả về bảng forecast."""
    global UNIFIED_VIEW, WEATHER_VIEW
    ln = dict((cfg.get("lanes") or {}).get(lane) or {})
    if lane != "main" and not ln:
        raise SystemExit(f"conf/ml/forecast.yaml không khai báo làn {lane}")
    if lane == "demo":
        # phiên phát lại kiểm chứng đang chọn: bảng forecast riêng của phiên (scripts/demo_replay.py ghi file này)
        f = Path(cfg.get("inference", {}).get("demo_lane_file", "/opt/lakehouse/logs/demo_lane.json"))
        if f.exists():
            ln.update({k: v for k, v in json.loads(f.read_text()).items() if k in ("forecast_table", "unified_view", "weather_view")})
    UNIFIED_VIEW = ln.get("unified_view", UNIFIED_VIEW)
    WEATHER_VIEW = ln.get("weather_view", WEATHER_VIEW)
    return ln.get("forecast_table", cfg["data"]["forecast_table"])


def latest_window_serving() -> dt.datetime:
    v = trino(f"SELECT CAST(max(window_start_utc) AS varchar) FROM {UNIFIED_VIEW}")[0][0]
    return dt.datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S")


def hist_serving(spark, lo: dt.datetime, hi: dt.datetime):
    rows = trino(f"""SELECT link_id, CAST(window_start_utc AS varchar), n_records, n_valid, sum_speed_mph, sum_speed_sq, avg_speed_mph
                     FROM {UNIFIED_VIEW}
                     WHERE window_start_utc >= TIMESTAMP '{lo:%Y-%m-%d %H:%M:%S} UTC'
                       AND window_start_utc <= TIMESTAMP '{hi:%Y-%m-%d %H:%M:%S} UTC'""")
    data = [(int(r[0]), dt.datetime.strptime(r[1][:19], "%Y-%m-%d %H:%M:%S"), int(r[2]), int(r[3]), float(r[4] or 0),
             float(r[5] or 0), None if r[6] is None else float(r[6])) for r in rows]
    return spark.createDataFrame(data, "link_id long, window_start_utc timestamp, n_records int, n_valid int, sum_speed_mph double, "
                                       "sum_speed_sq double, avg_speed_mph double")


def weather_serving(spark, lo: dt.datetime, hi: dt.datetime):
    rows = trino(f"""SELECT cell_id, CAST(time_utc AS varchar), temperature_2m, relative_humidity_2m, wind_speed_10m,
                            wind_gusts_10m, precipitation, snowfall, weather_code
                     FROM {WEATHER_VIEW}
                     WHERE time_utc >= TIMESTAMP '{lo:%Y-%m-%d %H:%M:%S} UTC' AND time_utc <= TIMESTAMP '{hi:%Y-%m-%d %H:%M:%S} UTC'""")
    f = lambda x: None if x is None else float(x)
    data = [(r[0], dt.datetime.strptime(r[1][:19], "%Y-%m-%d %H:%M:%S"), *map(f, r[2:])) for r in rows]
    return spark.createDataFrame(data, "cell_id string, weather_utc timestamp, temperature_2m double, relative_humidity_2m double, "
                                       "wind_speed_10m double, wind_gusts_10m double, precipitation double, snowfall double, weather_code double")


def run(spark, cfg, model_dir: str, origin: dt.datetime | None, source: str, write: bool = True,
        origins: list[dt.datetime] | None = None, forecast_table: str | None = None, booster=None) -> pd.DataFrame:
    """Dự báo cho một gốc (origin) hoặc nhiều gốc (origins). Gốc t dùng cửa sổ hoàn tất cuối L = t - 5' - A."""
    meta = json.loads((Path(model_dir) / "metadata.json").read_text())
    booster = booster or NumpyBooster.load(str(model_file(model_dir)))
    A, H = meta["availability_lag_minutes"], meta["horizon_minutes"]
    if origins:
        Ls = sorted({floor5(o) - dt.timedelta(minutes=5 + A) for o in origins})
    elif origin is None:
        if source != "serving":
            raise SystemExit("--origin là bắt buộc với --source hist")
        Ls = [latest_window_serving()]
    else:
        Ls = [floor5(origin) - dt.timedelta(minutes=5 + A)]
    L = Ls[-1]
    lo = Ls[0] - dt.timedelta(days=8)
    links = spark.table(SILVER_LINK_INFO).select("link_id", "borough", "latitude", "longitude", "cell_id", "cell_distance_km")
    if source == "serving":
        hist = hist_serving(spark, lo, L)
        weather = weather_serving(spark, Ls[0] - dt.timedelta(hours=12), L + dt.timedelta(hours=1))
    else:
        hist = spark.table(cfg["data"]["hist_5min_table"]).filter((F.col("window_start_utc") >= F.lit(lo)) & (F.col("window_start_utc") <= F.lit(L)))
        weather = spark.table(cfg["data"]["weather_hist_table"]).filter(
            (F.col("weather_utc") >= F.lit(Ls[0] - dt.timedelta(hours=12))) & (F.col("weather_utc") <= F.lit(L + dt.timedelta(hours=1))))
    feats = build_features(spark, hist, links, weather, cfg, lo, L + dt.timedelta(minutes=5))
    pdf = feats.filter(F.col("last_window_start_utc").isin(*Ls)).toPandas()
    if pdf.empty:
        raise SystemExit(f"không dựng được feature cho cửa sổ {Ls[0]}..{L}")
    for c in ("origin_utc", "target_window_start_utc", "target_window_end_utc", "data_cutoff_utc", "w_record_utc"):
        pdf[c] = pd.to_datetime(pdf[c], utc=True)
    link_index = {int(k): v for k, v in meta["link_index"].items()}
    pdf = add_static_codes(pdf, link_index)
    as_of = pd.Timestamp(meta["baseline_as_of_utc"])
    base = spark.table(cfg["data"]["baseline_table"]).toPandas()
    as_utc = as_of.tz_convert("UTC") if as_of.tzinfo else as_of.tz_localize("UTC")
    n_asof = int((pd.to_datetime(base["as_of_utc"], utc=True) == as_utc).sum()) if len(base) else 0
    if n_asof == 0:
        raise SystemExit(f"{cfg['data']['baseline_table']} không có free-flow as_of {as_of} của model {meta['model_version']}")
    pdf = add_fold_features(pdf, base, as_of)
    X = pdf[meta["features"]].astype("float64").values
    pred = booster.predict(X)
    has_recent = pdf["speed_last_known_60"].notna().values
    pred = np.where(has_recent, pred, np.nan)            # không có quan sát hợp lệ trong 60': không dự báo (giống tập đánh giá)
    out = pd.DataFrame({
        "link_id": pdf["link_id"].astype("int64"), "prediction_origin_utc": pdf["origin_utc"],
        "target_window_start_utc": pdf["target_window_start_utc"], "target_window_end_utc": pdf["target_window_end_utc"],
        "data_cutoff_utc": pdf["data_cutoff_utc"], "predicted_speed": pred,
        "predicted_traffic_state": traffic_state(pred, pdf["free_flow_speed"], cfg["state"]["congested_lt"], cfg["state"]["slow_lt"]),
        "free_flow_speed": pdf["free_flow_speed"].astype("float64"), "speed_last_known_60": pdf["speed_last_known_60"],
        "n_valid_60": pdf["n_valid_60"].astype("int32"), "minutes_since_valid": pdf["minutes_since_valid"],
        "weather_record_utc": pdf["w_record_utc"], "weather_missing": pdf["weather_missing"].astype("int32"),
        "feature_complete": has_recent & (pdf["weather_missing"].values == 0),
        "model_version": meta["model_version"], "feature_version": meta["feature_version"],
        "baseline_as_of_utc": as_of, "input_source": source,
        "created_at": pd.Timestamp.now(tz="UTC").floor("s")})
    out.loc[~has_recent, "predicted_traffic_state"] = "NO_RECENT_DATA"
    if write:
        table = forecast_table or cfg["data"]["forecast_table"]
        spark.sql(FORECAST_DDL.format(t=table))
        sdf = spark.createDataFrame(out.assign(**{c: out[c].dt.tz_convert(None) for c in out.columns
                                                  if str(out[c].dtype).startswith("datetime64[ns, UTC]") or str(out[c].dtype).startswith("datetime64[us, UTC]")}))
        # NaN của pandas (link không có quan sát 60') phải thành NULL: NaN làm avg/abs_error trên Trino ra NaN
        sdf = sdf.select([F.when(F.isnan(c), None).otherwise(F.col(c)).alias(c) if t == "double" else F.col(c) for c, t in sdf.dtypes])
        sdf.createOrReplaceTempView("new_forecast")
        on = " AND ".join(f"t.{k} = s.{k}" for k in KEY)
        spark.sql(f"MERGE INTO {table} t USING new_forecast s ON {on} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-dir", required=True)
    ap.add_argument("--origin", help="gốc dự báo UTC (ISO). Mặc định: mới nhất có thể từ Serving Layer")
    ap.add_argument("--source", choices=["serving", "hist"], default="serving")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--dump-json", help="ghi kết quả ra file JSON (kiểm thử)")
    ap.add_argument("--lane", default="main", help="main | demo (view đọc và bảng ghi theo conf/ml/forecast.yaml lanes)")
    args = ap.parse_args()
    cfg = load_config()
    table = set_lane(cfg, args.lane)
    spark = build_spark("ml_predict", {"spark.sql.shuffle.partitions": "16"})
    t0 = time.time()
    origin = dt.datetime.fromisoformat(args.origin) if args.origin else None
    out = run(spark, cfg, args.model_dir, origin, args.source, write=not args.no_write, forecast_table=table)
    summ = {"origin": str(out["prediction_origin_utc"].iloc[0]), "links": int(len(out)),
            "predicted": int(out["predicted_speed"].notna().sum()), "states": out["predicted_traffic_state"].value_counts().to_dict(),
            "model_version": out["model_version"].iloc[0], "seconds": round(time.time() - t0, 1), "written": not args.no_write}
    print(json.dumps(summ, default=str), flush=True)
    if args.dump_json:
        out.to_json(args.dump_json, orient="records", date_format="iso")
    spark.stop()


if __name__ == "__main__":
    main()
