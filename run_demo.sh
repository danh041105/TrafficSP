#!/usr/bin/env bash
# Demo TỔNG phần việc của Mai: giao diện (Serving Layer) -> monitoring, chạy bằng 1 lệnh.
#
#   bash run_demo.sh up [số_phút]   # mặc định 20 phút
#   bash run_demo.sh status         # xem container + target Prometheus
#   bash run_demo.sh down           # tắt (giữ dữ liệu)
#
# Bật:
#   1. Monitoring: Kafka, Spark, kafka-exporter, Prometheus, Grafana + dữ liệu giả + 1 job Spark streaming
#      (dùng lại scripts/monitoring_demo/run_demo.sh)
#   2. Serving demo: ClickHouse tạm + nạp data/raw + replay 10/2025 + Serving API + dashboard React
#   3. Prometheus đọc thêm số đo của Serving API -> dashboard Grafana "Serving - Traffic"
#
# Cần Docker Desktop, khoảng 3,5 GB RAM cho Docker. Máy 8 GB: tắt bớt ứng dụng khác.
set -euo pipefail

cd "$(dirname "$0")"
MINUTES="${2:-20}"

compose() { docker compose "$@" 2>&1 | grep -v -e 'variable is not set' -e 'level=warning' || true; }
ch() { docker exec traffic-serving-clickhouse clickhouse-client --user demo --password demo -q "$1" 2>/dev/null; }
wait_http() { local url=$1 name=$2; printf '   chờ %s' "$name"; until curl -sf "$url" >/dev/null; do printf '.'; sleep 3; done; echo ' OK'; }

up() {
  docker info >/dev/null 2>&1 || { echo "!! Docker chưa chạy. Mở Docker Desktop rồi chạy lại."; exit 1; }

  echo "== 1/4 Monitoring: Kafka + Spark + Prometheus + Grafana (${MINUTES} phút dữ liệu giả)"
  DEMO_SPARK_CORES="${DEMO_SPARK_CORES:-1}" bash scripts/monitoring_demo/run_demo.sh up "$MINUTES" | sed 's/^/   /' | grep -v -e '^   Demo đang chạy' -e 'Tắt demo' || true

  echo "== 2/4 Serving demo: build image + ClickHouse"
  compose --profile demo build serving-api serving-web
  compose --profile demo up -d serving-clickhouse
  until ch "SELECT 1" >/dev/null; do sleep 2; done
  n_links="$(ch "SELECT count() FROM traffic.dim_link" || true)"
  if [ "${n_links:-0}" = "0" ]; then
    echo "   Nạp dữ liệu nền lần đầu (01-09/2025, khoảng 2 phút)..."
    compose --profile demo run --rm serving-demo-load
  else
    echo "   Đã có dữ liệu nền, bỏ qua bước nạp."
  fi

  echo "== 3/4 Replay 10/2025 + Serving API + dashboard"
  SERVING_SOURCE=demo compose --profile demo up -d --force-recreate serving-demo-replay serving-api serving-web
  wait_http localhost:8000/api/meta "Serving API"
  wait_http localhost:3000 "dashboard"

  echo "== 4/4 Prometheus nạp lại cấu hình (thêm job serving-api)"
  curl -sf -X POST localhost:9090/-/reload >/dev/null && echo "   OK"

  cat <<EOF

Đã chạy xong. Chờ 1-2 phút để biểu đồ có điểm, rồi mở:
  Dashboard giao thông : http://localhost:3000
  Serving API (docs)   : http://localhost:8000/docs      số đo: http://localhost:8000/metrics
  Grafana              : http://localhost:3001  -> Dashboards -> Monitoring
                         (Kafka - Traffic, Spark - Traffic, Serving - Traffic)
  Prometheus targets   : http://localhost:9090/targets
  Spark master         : http://localhost:8090

Hướng dẫn đầy đủ: README.md
Tắt: bash run_demo.sh down
EOF
}

status() {
  docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -E 'NAMES|traffic|kafka' || true
  echo
  echo "Target Prometheus (down ở spark-driver/speed-layer là bình thường khi không có job đó):"
  curl -s localhost:9090/api/v1/targets 2>/dev/null | python3 -c '
import json, sys
try:
    for t in json.load(sys.stdin)["data"]["activeTargets"]:
        job, health = t["labels"]["job"], t["health"]
        print("  %-28s %s" % (job, health))
except Exception:
    print("  Prometheus chưa chạy")'
}

down() {
  echo "== Tắt serving demo"
  compose --profile demo stop serving-demo-replay serving-api serving-web serving-clickhouse
  echo "== Tắt monitoring demo"
  bash scripts/monitoring_demo/run_demo.sh down
}

case "${1:-}" in
  up) up ;;
  status) status ;;
  down) down ;;
  *) echo "Cách dùng: bash run_demo.sh up [số_phút] | status | down"; exit 1 ;;
esac
