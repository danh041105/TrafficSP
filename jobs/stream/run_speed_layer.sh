#!/usr/bin/env bash
# Chạy Speed Layer có tự khởi động lại (supervisor đơn giản). Chạy TRONG container Spark:
#   docker exec -d traffic-spark-master bash /opt/spark/jobs/stream/run_speed_layer.sh [tham số speed_layer.py ...]
# Khi query chết (vd ClickHouse ngừng lâu hơn ngân sách retry CLICKHOUSE_RETRIES), ứng dụng thoát != 0;
# script đợi rồi chạy lại. Checkpoint giữ offset/state -> chạy lại micro-batch dang dở, sink idempotent nên không nhân đôi.
# Dừng hẳn: tạo file $STOP_FILE (mặc định /opt/lakehouse/logs/speed_layer.stop) rồi kill tiến trình spark-submit.
set -u
LOG_DIR=${LOG_DIR:-/opt/lakehouse/logs}
STOP_FILE=${STOP_FILE:-$LOG_DIR/speed_layer.stop}
CORES=${SPEED_CORES_MAX:-2}
EXEC_MEM=${SPEED_EXECUTOR_MEMORY:-1500m}
BACKOFF=${RESTART_BACKOFF_SEC:-15}
MAX_RESTARTS=${MAX_RESTARTS:-100}

mkdir -p "$LOG_DIR"
rm -f "$STOP_FILE"
n=0
while :; do
  echo "[supervisor] $(date -u +%FT%TZ) start #$n: $*" >> "$LOG_DIR/speed_supervisor.log"
  /opt/spark/bin/spark-submit --conf spark.cores.max="$CORES" --conf spark.executor.memory="$EXEC_MEM" \
    /opt/spark/jobs/stream/speed_layer.py "$@" >> "$LOG_DIR/speed_layer.log" 2>&1
  rc=$?
  echo "[supervisor] $(date -u +%FT%TZ) exit rc=$rc" >> "$LOG_DIR/speed_supervisor.log"
  if [ -f "$STOP_FILE" ] || [ "$rc" -eq 0 ]; then break; fi
  n=$((n + 1))
  if [ "$n" -gt "$MAX_RESTARTS" ]; then echo "[supervisor] quá $MAX_RESTARTS lần, dừng" >> "$LOG_DIR/speed_supervisor.log"; exit 1; fi
  sleep "$BACKOFF"
done
