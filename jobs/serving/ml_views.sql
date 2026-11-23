-- View Trino cho kết quả dự báo ML (bảng Iceberg lakehouse.ml.traffic_forecast_15m do jobs/ml/predict.py ghi).
-- Áp dụng: docker exec traffic-trino trino --catalog lakehouse --file /opt/trino-sql/ml_views.sql
-- Quyền: chỉ serving.v_* được tạo view (conf/trino/access/lakehouse-rules.json), bảng ml.* chỉ SELECT.

-- Mọi dự báo + thông tin đoạn đường + tốc độ thực tế khi cửa sổ target đã có trong view hợp nhất (để theo dõi sai số trực tiếp)
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_forecast_15m SECURITY INVOKER AS
SELECT f.link_id, d.link_name, d.borough, d.latitude, d.longitude,
       f.prediction_origin_utc, f.target_window_start_utc, f.target_window_end_utc, f.data_cutoff_utc,
       f.predicted_speed, f.predicted_traffic_state, f.free_flow_speed, f.speed_last_known_60,
       f.feature_complete, f.weather_missing, f.minutes_since_valid,
       f.model_version, f.feature_version, f.baseline_as_of_utc, f.input_source, f.created_at,
       u.avg_speed_mph AS actual_speed,
       u.layer AS actual_layer,
       abs(u.avg_speed_mph - f.predicted_speed) AS abs_error
FROM lakehouse.ml.traffic_forecast_15m f
LEFT JOIN lakehouse.serving.v_link_dim d ON d.link_id = f.link_id
LEFT JOIN lakehouse.serving.v_traffic_5min_unified u
       ON u.link_id = f.link_id AND u.window_start_utc = CAST(f.target_window_start_utc AS timestamp(6) with time zone)
      AND u.n_valid > 0;

-- Dự báo mới nhất của mỗi link (model mới nhất), cho dashboard
CREATE OR REPLACE VIEW lakehouse.serving.v_traffic_forecast_15m_latest SECURITY INVOKER AS
SELECT *
FROM (SELECT v.*, row_number() OVER (PARTITION BY link_id ORDER BY prediction_origin_utc DESC, created_at DESC) AS rn
      FROM lakehouse.serving.v_traffic_forecast_15m v)
WHERE rn = 1;
