#!/usr/bin/env bash
# Chạy một job ML Spark trong container traffic-spark-master (dev compose). Chép conf/ml/forecast.yaml vào container
# (/opt/ml_conf) vì thư mục conf/ml không được mount; driver chạy ở client mode trên spark-master nên chỉ driver cần file này.
#   bash scripts/ml/run_spark.sh jobs/ml/build_dataset.py --stage all --sample-links 8 --start 2025-08-01 --end 2025-10-01
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/../.."
SCRIPT="$1"; shift
docker exec traffic-spark-master mkdir -p /opt/ml_conf
docker cp conf/ml/forecast.yaml traffic-spark-master:/opt/ml_conf/forecast.yaml >/dev/null
MSYS_NO_PATHCONV=1 docker exec -e PYTHONPATH=/opt/spark/jobs traffic-spark-master \
  /opt/spark/bin/spark-submit --conf spark.executor.memory=3g --conf spark.driver.memory=2g \
  "/opt/spark/${SCRIPT}" "$@" 2>&1 \
  | grep -vE " INFO |WARN (NativeCodeLoader|Utils|SparkConf|SparkStringUtils)|^\s*$"
