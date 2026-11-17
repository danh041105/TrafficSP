#!/usr/bin/env bash
# Chạy Speed Layer có tự khởi động lại (supervisor đơn giản). Chạy TRONG container Spark:
#   docker exec -d traffic-spark-master bash /opt/spark/jobs/stream/run_speed_layer.sh [tham số speed_layer.py ...]
# Dừng (chờ tới khi supervisor thoát hẳn):
#   docker exec traffic-spark-master bash /opt/spark/jobs/stream/stop_speed_layer.sh
# - CHỈ MỘT supervisor được chạy (flock trên $LOCK_FILE): hai ứng dụng dùng chung checkpoint sẽ tranh nhau offset log.
#   Spark tự chặn (CONCURRENT_STREAM_LOG_UPDATE) và sink Iceberg bỏ qua epoch đã commit nên không mất/trùng dữ liệu,
#   nhưng các ứng dụng sẽ chết đi chết lại -> khoá ở đây để không bao giờ xảy ra.
# - Ứng dụng thoát != 0 (Kafka/catalog tạm ngừng, commit Iceberg lỗi hết số lần retry): đợt rồi chạy lại từ checkpoint;
#   micro-batch dang dở được chạy lại, sink Iceberg bỏ qua epoch đã commit, MERGE của latest là no-op với dữ liệu cũ.
# - Airflow KHÔNG quản lý tiến trình này (Speed Layer chạy liên tục, không theo lịch Batch).
set -u
LOG_DIR=${LOG_DIR:-/opt/lakehouse/logs}
STOP_FILE=${STOP_FILE:-$LOG_DIR/speed_layer.stop}
LOCK_FILE=${LOCK_FILE:-$LOG_DIR/speed_layer.lock}
CORES=${SPEED_CORES_MAX:-2}
EXEC_MEM=${SPEED_EXECUTOR_MEMORY:-1500m}
BACKOFF=${RESTART_BACKOFF_SEC:-15}
MAX_RESTARTS=${MAX_RESTARTS:-100}

mkdir -p "$LOG_DIR"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[supervisor] $(date -u +%FT%TZ) đã có supervisor khác đang chạy -> thoát" | tee -a "$LOG_DIR/speed_supervisor.log"
  exit 1
fi
rm -f "$STOP_FILE"          # an toàn: đang giữ khoá nên không supervisor nào khác đọc file này
n=0
while :; do
  echo "[supervisor] $(date -u +%FT%TZ) start #$n pid=$$: $*" >> "$LOG_DIR/speed_supervisor.log"
  /opt/spark/bin/spark-submit --conf spark.cores.max="$CORES" --conf spark.executor.memory="$EXEC_MEM" \
    /opt/spark/jobs/stream/speed_layer.py --ops-log-dir "$LOG_DIR" "$@" >> "$LOG_DIR/speed_layer.log" 2>&1
  rc=$?
  echo "[supervisor] $(date -u +%FT%TZ) exit rc=$rc pid=$$" >> "$LOG_DIR/speed_supervisor.log"
  if [ -f "$STOP_FILE" ] || [ "$rc" -eq 0 ]; then break; fi
  n=$((n + 1))
  if [ "$n" -gt "$MAX_RESTARTS" ]; then echo "[supervisor] quá $MAX_RESTARTS lần, dừng" >> "$LOG_DIR/speed_supervisor.log"; exit 1; fi
  sleep "$BACKOFF"
done
