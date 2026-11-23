"""Tạo bảng dự báo (cùng DDL với predict.py) nếu chưa có - dùng khi dựng làn phát lại kiểm chứng.
  spark-submit jobs/ml/forecast_table.py --table lakehouse.ml.traffic_forecast_15m_<run>"""
import argparse

from common.spark_session import build_spark
from ml.predict import FORECAST_DDL

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    t = ap.parse_args().table
    spark = build_spark("forecast_table")
    spark.sql(FORECAST_DDL.format(t=t))
    print(f"FORECAST_TABLE_READY {t}", flush=True)
    spark.stop()
