#!/usr/bin/env bash
# Spark worker + watchdog đăng ký.
# Master giữ worker DEAD trong danh sách (spark.dead.worker.persistence+1) x spark.worker.timeout giây (mặc định 16 x 60 s),
# sau đó xóa hẳn. Worker sống lại SAU mốc đó bị master bỏ qua -> chỉ watchdog cứu được (sự cố 21/11: worker treo 28 phút).
# Worker sống lại TRƯỚC mốc đó tự kết nối lại (đã kiểm thử: dừng JVM 150 s -> DEAD -> ALIVE sau 15 s).
# Vấn đề thật (2026-11-21): khi VM thiếu RAM, worker ngừng heartbeat > spark.worker.timeout -> master xóa worker khỏi
# danh sách. Spark 3.5 master sau đó BỎ QUA heartbeat của worker đó ("This worker was never registered, so ignoring the
# heartbeat") và worker không tự đăng ký lại, nên cụm còn 0 core cho tới khi restart tay.
# Watchdog: cứ WATCH_INTERVAL giây hỏi master JSON xem worker của container này (theo IP) có ALIVE không; sai
# WATCH_FAILS lần liên tiếp -> dừng worker -> container thoát -> Docker (restart: unless-stopped) khởi động lại -> đăng ký mới.
set -u
MASTER_URL=${MASTER_URL:-spark://spark-master:7077}
MASTER_UI=${MASTER_UI:-http://spark-master:8080}
WATCH_INTERVAL=${WATCH_INTERVAL:-30}
WATCH_FAILS=${WATCH_FAILS:-3}
WATCH_GRACE=${WATCH_GRACE:-90}

/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "$MASTER_URL" "$@" &
WORKER=$!
trap 'kill -TERM $WORKER 2>/dev/null; wait $WORKER; exit 0' TERM INT

MY_IP=$(hostname -i | awk '{print $1}')
sleep "$WATCH_GRACE"
fails=0
while kill -0 "$WORKER" 2>/dev/null; do
  state=$(curl -s -m 10 "$MASTER_UI/json/" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    print('MASTER_UNREACHABLE'); sys.exit()
ws = [w for w in d.get('workers', []) if w.get('host') == '$MY_IP' and w.get('state') == 'ALIVE']
print('ALIVE' if ws else 'NOT_REGISTERED')")
  if [ "$state" = "NOT_REGISTERED" ]; then
    fails=$((fails + 1))
    echo "[watchdog] $(date -u +%FT%TZ) master không thấy worker $MY_IP ALIVE ($fails/$WATCH_FAILS)"
    if [ "$fails" -ge "$WATCH_FAILS" ]; then
      echo "[watchdog] dừng worker để Docker khởi động lại và đăng ký mới"
      # JVM đang treo/bị dừng không nhận SIGTERM -> sau 10 s chuyển sang SIGKILL
      kill -TERM "$WORKER" 2>/dev/null
      for _ in $(seq 1 10); do kill -0 "$WORKER" 2>/dev/null || exit 1; sleep 1; done
      kill -KILL "$WORKER" 2>/dev/null; exit 1
    fi
  else
    fails=0          # ALIVE hoặc master tạm không trả lời (không trừng phạt worker vì master)
  fi
  sleep "$WATCH_INTERVAL"
done
wait "$WORKER"
