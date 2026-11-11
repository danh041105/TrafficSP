#!/usr/bin/env bash
# Quét tham số Speed Layer trên CÙNG một khối lượng cố định (offset Kafka chốt trước), --available-now.
# Mỗi cấu hình: checkpoint riêng + database ClickHouse riêng (traffic_bench) -> không đụng bảng/checkpoint đang chạy.
# Spark local[2] trong container master (cluster đang bận với Speed Layer + stream_to_bronze).
# Chạy trên host ở thư mục gốc repo:   scripts/benchmark/run_sweep.sh
# Kết quả: logs/bench/sweep_<cfg>.json + logs/bench/sweep_summary.tsv
set -euo pipefail
export MSYS_NO_PATHCONV=1
mkdir -p logs/bench
BENCH_DB=traffic_bench
TAG=${SWEEP_TAG:-$(date -u +%Y%m%d%H%M)}
ENVS="-e PYTHONPATH=/opt/spark/jobs -e CLICKHOUSE_SERVING_DB=$BENCH_DB"

# Khối lượng cố định: SWEEP_LAST_N message cuối của mỗi partition traffic_raw (offset chốt 1 lần cho mọi cấu hình;
# không chạy producer trong lúc quét). SWEEP_STARTING_OFFSETS='earliest' để quét cả topic.
LAST_N=${SWEEP_LAST_N:-60000}
if [ -z "${SWEEP_STARTING_OFFSETS:-}" ]; then
  SWEEP_STARTING_OFFSETS=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic traffic_raw \
    | awk -F: -v n="$LAST_N" 'BEGIN{printf "{\"traffic_raw\":{"} {s=$3-n; if (s<0) s=0; printf "%s\"%s\":%d", (NR>1?",":""), $2, s} END{printf "}}"}')
fi
echo "[sweep] startingOffsets=$SWEEP_STARTING_OFFSETS" | tee logs/bench/sweep_offsets.txt
docker exec $ENVS traffic-spark-master python3 /opt/spark/jobs/ddl/clickhouse_serving.py > /dev/null
docker exec traffic-clickhouse bash -c "clickhouse-client -u \"\$CLICKHOUSE_USER\" --password \"\$CLICKHOUSE_PASSWORD\" -q \
  'INSERT INTO $BENCH_DB.link_dim SELECT * FROM traffic.link_dim FINAL'"

# tên | shuffle partitions | maxOffsetsPerTrigger | watermark
CONFIGS=${SWEEP_CONFIGS:-"default|8|100000|10 minutes
maxoff20k|8|20000|10 minutes
shuffle2|2|100000|10 minutes
shuffle32|32|100000|10 minutes
wm2m|8|100000|2 minutes
wm30m|8|100000|30 minutes"}

printf "cfg\twall_sec\tquery\tbatches\trows\trows_per_sec_processing\tp50_ms\tp95_ms\tmax_state_rows\tmax_state_mb\tdropped_by_watermark\n" \
  > logs/bench/sweep_summary.tsv
while IFS='|' read -r name shuffle maxoff wm; do
  [ -z "$name" ] && continue
  since=$(date -u +%FT%T); t0=$(date +%s)
  docker exec $ENVS -e SPEED_SHUFFLE_PARTITIONS="$shuffle" traffic-spark-master /opt/spark/bin/spark-submit \
      --master "local[2]" --driver-memory 2g /opt/spark/jobs/stream/speed_layer.py --queries latest,5min \
      --starting-offsets "$SWEEP_STARTING_OFFSETS" --available-now --max-offsets "$maxoff" --watermark "$wm" \
      --checkpoint-root "/opt/lakehouse/checkpoints_bench/${TAG}_${name}" > "logs/bench/sweep_${name}.log" 2>&1
  wall=$(( $(date +%s) - t0 )); until=$(date -u +%FT%T)
  docker exec $ENVS traffic-spark-master python3 /opt/spark/jobs/tests/benchmark/summarize.py \
      --label "sweep_${name}" --since "$since" --until "$until" > "logs/bench/sweep_${name}.json"
  python -I -c "
import json, sys
d = json.load(open('logs/bench/sweep_${name}.json'))
for q, r in sorted(d['queries'].items()):
    print('\t'.join(str(x) for x in ['${name}', ${wall}, q, r.get('batches'), r.get('rows'), r.get('rows_per_sec_processing'),
          r.get('p50_ms'), r.get('p95_ms'), r.get('max_state_rows'), round((r.get('max_state_bytes') or 0) / 2**20, 1),
          r.get('dropped_by_watermark')]))" >> logs/bench/sweep_summary.tsv
  echo "[sweep] $name xong sau ${wall}s"
done <<< "$CONFIGS"
column -t -s $'\t' logs/bench/sweep_summary.tsv 2>/dev/null || cat logs/bench/sweep_summary.tsv
