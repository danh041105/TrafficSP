"""Tạo bảng dự báo (cùng DDL với predict.py) nếu chưa có - dùng khi dựng làn phát lại kiểm chứng.
  spark-submit jobs/ml/forecast_table.py --table lakehouse.ml.traffic_forecast_15m_<run>

FORECAST_DDL đặt ở đây (không ở predict.py) vì file này chạy trên traffic-spark-master: image Spark không có numpy/pandas,
còn import predict.py sẽ kéo theo numpy/pandas. predict.py import ngược FORECAST_DDL từ file này."""
import argparse

FORECAST_DDL = """CREATE TABLE IF NOT EXISTS {t} (
  link_id BIGINT, prediction_origin_utc TIMESTAMP, target_window_start_utc TIMESTAMP, target_window_end_utc TIMESTAMP,
  data_cutoff_utc TIMESTAMP, predicted_speed DOUBLE, predicted_traffic_state STRING, free_flow_speed DOUBLE,
  speed_last_known_60 DOUBLE, n_valid_60 INT, minutes_since_valid DOUBLE, weather_record_utc TIMESTAMP, weather_missing INT,
  feature_complete BOOLEAN, model_version STRING, feature_version STRING, baseline_as_of_utc TIMESTAMP, input_source STRING,
  created_at TIMESTAMP)
USING iceberg PARTITIONED BY (days(prediction_origin_utc)) TBLPROPERTIES ('format-version' = '2')"""

if __name__ == "__main__":
    from common.spark_session import build_spark

    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    t = ap.parse_args().table
    spark = build_spark("forecast_table")
    spark.sql(FORECAST_DDL.format(t=t))
    print(f"FORECAST_TABLE_READY {t}", flush=True)
    spark.stop()
