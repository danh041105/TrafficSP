#!/usr/bin/env bash
# End-to-end kiểm tra inference trên hệ thống thật (dev compose):
#   1. Tái lập: chạy predict.py --source hist cho một gốc dự báo trong final test -> so với dự báo backtest (preds/final_test).
#   2. Idempotent: chạy predict.py --source serving HAI lần cùng gốc -> bảng forecast vẫn một dòng mỗi (link, gốc, model).
#   3. Trino đọc được serving.v_traffic_forecast_15m.
#   MODEL=models/ml/current ORIGIN=2025-12-10T17:30 bash scripts/ml/e2e_inference_check.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1 PYTHONIOENCODING=utf-8
cd "$(dirname "$0")/../.."
MODEL="${MODEL:-models/ml/current}"
ORIGIN="${ORIGIN:-2025-12-10T17:30}"
VER=$(python -c "import json;print(json.load(open('$MODEL/metadata.json'))['model_version'])")
C=/opt/lakehouse/ml_models/$VER
docker exec traffic-spark-master mkdir -p "$C"
for f in model.json metadata.json; do docker cp "$MODEL/$f" "traffic-spark-master:$C/$f" >/dev/null; done
RUN="bash scripts/ml/run_spark.sh jobs/ml/predict.py --model-dir $C"

echo "== 1. tái lập backtest tại $ORIGIN (nguồn: ml.traffic_5min_hist)"
$RUN --source hist --origin "$ORIGIN" --no-write --dump-json /opt/lakehouse/logs/ml_e2e_hist.json | tail -1
docker cp traffic-spark-master:/opt/lakehouse/logs/ml_e2e_hist.json logs/ml/e2e_hist.json >/dev/null
python - "$ORIGIN" <<'EOF'
import sys, glob, pandas as pd
o = pd.Timestamp(sys.argv[1], tz="UTC")
inf = pd.read_json("logs/ml/e2e_hist.json")
inf["prediction_origin_utc"] = pd.to_datetime(inf["prediction_origin_utc"], utc=True)
bt = pd.read_parquet(sorted(glob.glob("data/ml/runs/*/preds/final_test__lightgbm.parquet"))[-1])
bt = bt[pd.to_datetime(bt.origin_utc, utc=True) == o]
m = inf.merge(bt, on="link_id")
d = (m.predicted_speed - m.lightgbm).abs()
print(f"   so khớp {len(m)} link: max |diff| = {d.max():.2e} mph, trung bình = {d.mean():.2e}")
sys.exit(0 if len(m) > 0 and d.max() < 0.05 else 1)
EOF

echo "== 2. idempotent: ghi 2 lần cùng gốc (nguồn: Serving Layer)"
$RUN | tail -1
$RUN | tail -1
docker exec traffic-trino trino --catalog lakehouse --output-format CSV --execute \
  "SELECT count(*), count(DISTINCT (link_id, prediction_origin_utc, model_version)) FROM lakehouse.ml.traffic_forecast_15m"

echo "== 3. Trino view"
docker exec traffic-trino trino --catalog lakehouse --file /opt/trino-sql/ml_views.sql
docker exec traffic-trino trino --catalog lakehouse --output-format ALIGNED --execute \
  "SELECT link_id, link_name, prediction_origin_utc, target_window_start_utc, round(predicted_speed,1) AS pred, predicted_traffic_state
   FROM lakehouse.serving.v_traffic_forecast_15m_latest ORDER BY predicted_speed LIMIT 5"
