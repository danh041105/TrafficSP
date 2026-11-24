#!/usr/bin/env bash
# Dừng Speed Layer có kiểm soát (không mất offset/state): đặt cờ dừng, gửi SIGTERM cho driver, CHỜ supervisor nhả khoá.
# Chạy TRONG container Spark:  docker exec traffic-spark-master bash /opt/spark/jobs/stream/stop_speed_layer.sh
# Mẫu pgrep/pkill neo ở đầu dòng lệnh (^...) để không khớp nhầm chính shell đang chạy lệnh này.
set -u
LOG_DIR=${LOG_DIR:-/opt/lakehouse/logs}
STOP_FILE=${STOP_FILE:-$LOG_DIR/speed_layer.stop}
LOCK_FILE=${LOCK_FILE:-$LOG_DIR/speed_layer.lock}
TIMEOUT=${STOP_TIMEOUT_SEC:-120}

touch "$STOP_FILE"
pkill -TERM -f "^/opt/java/openjdk/bin/java .*speed_layer\.py" 2>/dev/null
for _ in $(seq 1 "$TIMEOUT"); do
  if flock -n "$LOCK_FILE" true 2>/dev/null; then echo "[stop] Speed Layer đã dừng"; exit 0; fi
  sleep 1
done
echo "[stop] hết thời gian chờ ${TIMEOUT}s, supervisor vẫn giữ khoá" >&2
exit 1
