#!/usr/bin/env bash
# Chạy 1 kịch bản benchmark với Speed Layer đang chạy: phát GD2 bằng producer, lấy mẫu docker stats, rồi tóm tắt số đo
# (JSONL của Speed Layer + metadata Iceberg qua Trino). Chạy trên host (Git Bash/WSL/Linux) ở thư mục gốc repo:
#   scripts/benchmark/run_live.sh b1_baseline --start 2025-12-04T05:00 --end 2025-12-05T05:00 --speedup 1440
# Kết quả: logs/bench/<label>.json, <label>_producer.json, <label>_docker_stats.tsv, <label>_docker_peak.tsv
set -euo pipefail
label=$1; shift
DRAIN_SEC=${DRAIN_SEC:-60}
COMPOSE_FILES=${COMPOSE_FILES:-"-f docker-compose.yml -f docker-compose.dev.yml"}
C="docker compose $COMPOSE_FILES --profile tools"
mkdir -p logs/bench
export MSYS_NO_PATHCONV=1

since=$(date -u +%FT%T)
( while :; do docker stats --no-stream --format "$(date -u +%T)\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" >> "logs/bench/${label}_docker_stats.tsv"; sleep 3; done ) &
sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

$C run --rm producer python scripts/replay_producer.py --phase gd2 --log-every-sec 600 \
   --summary-file "/app/logs/bench/${label}_producer.json" "$@" > "logs/bench/${label}_producer.log" 2>&1
sleep "$DRAIN_SEC"
until=$(date -u +%FT%T)
kill $sampler 2>/dev/null || true

docker exec -e PYTHONPATH=/opt/spark/jobs -e TRINO_URL=http://trino:8080 traffic-spark-master \
  python3 /opt/spark/jobs/tests/benchmark/summarize.py --label "$label" --since "$since" --until "$until" > "logs/bench/${label}.json"
# CPU/RAM lớn nhất theo container trong lúc chạy
awk -F'\t' '{cpu=$3; sub("%","",cpu); mp=$5; sub("%","",mp); split($4,m," / ");
     if (!($2 in c) || cpu+0>c[$2]) c[$2]=cpu+0; if (!($2 in pm) || mp+0>pm[$2]) {pm[$2]=mp+0; mm[$2]=m[1]}}
     END {for (k in c) printf "%s\tcpu_max=%.1f%%\tmem_max=%s\n", k, c[k], mm[k]}' "logs/bench/${label}_docker_stats.tsv" \
  > "logs/bench/${label}_docker_peak.tsv"
echo "[bench] $label $since -> $until"; cat "logs/bench/${label}_docker_peak.tsv"
