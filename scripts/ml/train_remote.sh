#!/usr/bin/env bash
# Đồng bộ code + dữ liệu export sang máy train (CPU/GPU, có lightgbm) và chạy train trong thư mục riêng ~/trafficsp-ml.
#   ML_HOST=user@<may-train> bash scripts/ml/train_remote.sh sync          # chép code + conf + Parquet export
#   ML_HOST=... bash scripts/ml/train_remote.sh run "<lệnh python trong ~/trafficsp-ml>"
#   ML_HOST=... bash scripts/ml/train_remote.sh fetch                           # lấy runs/ về data/ml/runs/
# Không cài gói nào trên máy train; không đụng tới thư mục khác ngoài ~/trafficsp-ml.
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/../.."
HOST="${ML_HOST:?đặt ML_HOST=user@host}"
REMOTE='~/trafficsp-ml'
case "${1:-}" in
  sync)
    ssh -o StrictHostKeyChecking=accept-new "$HOST" "mkdir -p $REMOTE/jobs $REMOTE/conf/ml $REMOTE/data"
    tar -cf - jobs/ml jobs/tests/ml conf/ml scripts/ml/train_all.sh | ssh -o StrictHostKeyChecking=accept-new "$HOST" "cd $REMOTE && tar -xf -"
    if [ -d data/ml/export ]; then
      tar -cf - -C data/ml export | ssh -o StrictHostKeyChecking=accept-new "$HOST" "cd $REMOTE/data && tar -xf -"
    fi
    ssh -o StrictHostKeyChecking=accept-new "$HOST" "cd $REMOTE && du -sh data/export/* 2>/dev/null; ls jobs/ml"
    ;;
  run)
    shift
    ssh -o StrictHostKeyChecking=accept-new "$HOST" "cd $REMOTE && export PYTHONPATH=$REMOTE/jobs PYTHONUNBUFFERED=1 && $*"
    ;;
  fetch)
    mkdir -p data/ml/runs
    ssh -o StrictHostKeyChecking=accept-new "$HOST" "cd $REMOTE && tar -cf - runs" | tar -xf - -C data/ml
    ;;
  *) echo "dùng: sync | run <lệnh> | fetch"; exit 2 ;;
esac
