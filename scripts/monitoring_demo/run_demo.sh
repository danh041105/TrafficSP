#!/usr/bin/env bash
# Demo monitoring: bật Kafka + Spark + Prometheus + Grafana, bơm dữ liệu giả vào Kafka,
# chạy 1 job Spark streaming để mọi panel trên dashboard đều có số liệu.
#
#   bash scripts/monitoring_demo/run_demo.sh up [số_phút]   # mặc định 15 phút
#   bash scripts/monitoring_demo/run_demo.sh down           # tắt demo (giữ dữ liệu)
#
# Cần khoảng 3 GB RAM trống cho Docker.
set -euo pipefail

cd "$(dirname "$0")/../.."          # về thư mục gốc repo
ROOT="$(pwd)"
SERVICES="kafka kafka-init kafka-exporter prometheus grafana spark-master spark-worker-1 spark-worker-2"
KAFKA_IMAGE="apache/kafka:4.1.0"

# Tài khoản Grafana lấy từ .env (docker compose tự đọc). Chưa có .env thì dùng admin/admin cho demo.
# Lưu ý: Grafana chỉ dùng mật khẩu này ở lần khởi tạo đầu tiên (docker-data/grafana còn trống).
if ! grep -qs '^GRAFANA_ADMIN_PASSWORD=' .env; then
  export GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"
  export GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-admin}"
fi

compose() { docker compose "$@" 2>&1 | grep -v 'variable is not set' || true; }

down() {
  echo ">> Tắt job Spark demo và producer giả lập"
  docker rm -f traffic-demo-driver traffic-demo-producer traffic-demo-weather traffic-demo-consumer >/dev/null 2>&1 || true
  echo ">> Dừng các service (dữ liệu vẫn giữ lại)"
  compose stop $SERVICES
}

up() {
  local minutes="${1:-15}"
  local seconds=$((minutes * 60))

  if docker ps --format '{{.Names}}' | grep -qx traffic-airflow-scheduler; then
    echo "!! traffic-airflow-scheduler đang chạy: demo mượn tên mạng 'airflow-scheduler' nên sẽ bị trùng."
    echo "   Hãy dừng Airflow trước (docker compose stop airflow-scheduler) rồi chạy lại."
    exit 1
  fi

  echo ">> 1/4 Bật Kafka, Spark, kafka-exporter, Prometheus, Grafana"
  compose up -d --build $SERVICES

  echo ">> 2/4 Chờ Prometheus và Grafana sẵn sàng"
  until curl -sf localhost:9090/-/ready >/dev/null; do sleep 2; done
  until curl -sf localhost:3001/api/health >/dev/null; do sleep 2; done

  echo ">> 3/4 Bơm dữ liệu giả vào Kafka trong ${minutes} phút"
  docker rm -f traffic-demo-producer traffic-demo-weather traffic-demo-consumer >/dev/null 2>&1 || true
  # traffic_raw: 20 / 35 / 50 message mỗi giây, đổi nhịp 10 phút một lần
  docker run -d --rm --name traffic-demo-producer --network traffic-net --entrypoint sh "$KAFKA_IMAGE" -c "
    end=\$((\$(date +%s) + $seconds)); i=0
    while [ \$(date +%s) -lt \$end ]; do
      n=\$((20 + (i/600)%3*15)); j=0
      while [ \$j -lt \$n ]; do echo \"{\\\"link_id\\\":\$((j%40)),\\\"speed\\\":\$((20+j%30)),\\\"seq\\\":\$i}\"; j=\$((j+1)); done
      i=\$((i+n)); sleep 1
    done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic traffic_raw" >/dev/null
  # weather_raw: 1 message mỗi 2 giây
  docker run -d --rm --name traffic-demo-weather --network traffic-net --entrypoint sh "$KAFKA_IMAGE" -c "
    end=\$((\$(date +%s) + $seconds)); i=0
    while [ \$(date +%s) -lt \$end ]; do echo \"{\\\"temp\\\":\$((i%10))}\"; i=\$((i+1)); sleep 2; done \
    | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic weather_raw" >/dev/null
  # consumer group cố tình đọc chậm (400 message / 20 giây) để thấy lag trên dashboard
  docker run -d --rm --name traffic-demo-consumer --network traffic-net --entrypoint sh "$KAFKA_IMAGE" -c "
    end=\$((\$(date +%s) + $seconds))
    while [ \$(date +%s) -lt \$end ]; do
      /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic traffic_raw \
        --group demo-slow-consumer --max-messages 400 --timeout-ms 20000 >/dev/null 2>&1; sleep 20
    done" >/dev/null

  echo ">> 4/4 Chạy job Spark streaming demo (driver mang tên mạng airflow-scheduler như khi chạy qua Airflow)"
  docker rm -f traffic-demo-driver >/dev/null 2>&1 || true
  docker run -d --rm --name traffic-demo-driver \
    --hostname airflow-scheduler --network traffic-net --network-alias airflow-scheduler \
    -v "$ROOT/conf/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf:ro" \
    -v "$ROOT/scripts/monitoring_demo:/demo:ro" \
    ad/spark:3.5.0 /opt/spark/bin/spark-submit \
      --conf spark.sql.defaultCatalog=spark_catalog \
      --conf spark.driver.host=airflow-scheduler \
      --driver-memory 512m --executor-memory 512m \
      --conf spark.executor.cores=1 --conf spark.cores.max="${DEMO_SPARK_CORES:-2}" \
      /demo/speed_demo.py "$seconds" >/dev/null

  cat <<EOF

Demo đang chạy ${minutes} phút. Đợi khoảng 2-3 phút để biểu đồ có đủ điểm, rồi mở:
  Grafana     : http://localhost:3001  (Dashboards -> Monitoring)   tài khoản: \$GRAFANA_ADMIN_USER trong .env (mặc định admin/admin)
  Prometheus  : http://localhost:9090/targets
  Spark master: http://localhost:8090

Tắt demo: bash scripts/monitoring_demo/run_demo.sh down
EOF
}

case "${1:-}" in
  up)   up "${2:-15}" ;;
  down) down ;;
  *)    echo "Cách dùng: $0 up [số_phút] | down"; exit 1 ;;
esac
