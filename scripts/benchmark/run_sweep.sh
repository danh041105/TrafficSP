#!/usr/bin/env bash
# Quét tham số Speed Layer (Spark -> Iceberg) trên CÙNG một khối lượng cố định (offset Kafka chốt trước), --available-now.
# Mỗi cấu hình ghi vào NAMESPACE Iceberg riêng (lakehouse.speed_bench_<cfg>) + checkpoint riêng + thư mục log riêng
# -> không đụng bảng/checkpoint của Speed Layer đang chạy. Spark local[2] trong container master.
# Chạy trên host ở thư mục gốc repo:   scripts/benchmark/run_sweep.sh
# Kết quả: logs/bench/sweep_<cfg>.json + logs/bench/sweep_summary.tsv
# Dọn sau khi xem kết quả (XOÁ dữ liệu benchmark - chỉ chạy khi chắc chắn):
#   docker exec traffic-spark-master /opt/spark/bin/spark-sql -e "DROP TABLE lakehouse.speed_bench_<cfg>.traffic_5min_rt PURGE; ..."
set -euo pipefail
export MSYS_NO_PATHCONV=1
mkdir -p logs/bench
TAG=${SWEEP_TAG:-$(date -u +%Y%m%d%H%M)}

# Khối lượng cố định: SWEEP_LAST_N message cuối của mỗi partition traffic_raw (không chạy producer trong lúc quét)
LAST_N=${SWEEP_LAST_N:-60000}
if [ -z "${SWEEP_STARTING_OFFSETS:-}" ]; then
  SWEEP_STARTING_OFFSETS=$(docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic traffic_raw \
    | awk -F: -v n="$LAST_N" 'BEGIN{printf "{\"traffic_raw\":{"} {s=$3-n; if (s<0) s=0; printf "%s\"%s\":%d", (NR>1?",":""), $2, s} END{printf "}}"}')
fi
echo "[sweep] startingOffsets=$SWEEP_STARTING_OFFSETS" | tee logs/bench/sweep_offsets.txt

# tên | shuffle partitions | maxOffsetsPerTrigger | watermark
CONFIGS=${SWEEP_CONFIGS:-"default|8|100000|10 minutes
maxoff20k|8|20000|10 minutes
shuffle2|2|100000|10 minutes"}

printf "cfg\twall_sec\tquery\tbatches\trows\trows_per_sec_processing\ttrigger_ms_p50\ttrigger_ms_p95\tadd_batch_ms_p95\tmax_state_rows\tdropped_by_watermark\tsnapshots\tdata_files\tavg_file_kb\n" \
  > logs/bench/sweep_summary.tsv
while IFS='|' read -r name shuffle maxoff wm; do
  [ -z "$name" ] && continue
  ns="lakehouse.speed_bench_${name}"; logdir="/opt/lakehouse/logs/bench_${TAG}_${name}"
  ENVS="-e PYTHONPATH=/opt/spark/jobs -e SPEED_NAMESPACE=$ns -e SPEED_SHUFFLE_PARTITIONS=$shuffle"
  docker exec $ENVS traffic-spark-master /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 1g \
      /opt/spark/jobs/ddl/speed.py > "logs/bench/sweep_${name}_ddl.log" 2>&1
  since=$(date -u +%FT%T); t0=$(date +%s)
  docker exec $ENVS traffic-spark-master /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 2g \
      /opt/spark/jobs/stream/speed_layer.py --queries latest,5min \
      --starting-offsets "$SWEEP_STARTING_OFFSETS" --available-now --max-offsets "$maxoff" --watermark "$wm" \
      --checkpoint-root "/opt/lakehouse/checkpoints_bench/${TAG}_${name}" --ops-log-dir "$logdir" > "logs/bench/sweep_${name}.log" 2>&1
  wall=$(( $(date +%s) - t0 )); until=$(date -u +%FT%T)
  docker exec -e PYTHONPATH=/opt/spark/jobs -e TRINO_URL=http://trino:8080 traffic-spark-master \
      python3 /opt/spark/jobs/tests/benchmark/summarize.py --label "sweep_${name}" --since "$since" --until "$until" \
      --log-dir "$logdir" --namespace "$ns" > "logs/bench/sweep_${name}.json"
  python -I -c "
import json
d = json.load(open('logs/bench/sweep_${name}.json')); ice = d['iceberg']['traffic_5min_rt']
for q, r in sorted(d['queries'].items()):
    if 'batches' not in r: continue
    print('\t'.join(str(x) for x in ['${name}', ${wall}, q, r['batches'], r['rows'], r['rows_per_sec_processing'], r['trigger_ms_p50'],
          r['trigger_ms_p95'], r['add_batch_ms_p95'], r['max_state_rows'], r['dropped_by_watermark'],
          ice['snapshots_total'], ice['data_files'], ice['avg_file_kb']]))" >> logs/bench/sweep_summary.tsv
  echo "[sweep] $name xong sau ${wall}s"
done <<< "$CONFIGS"
column -t -s $'\t' logs/bench/sweep_summary.tsv 2>/dev/null || cat logs/bench/sweep_summary.tsv
