#!/usr/bin/env bash
# Chạy TRÊN MÁY TRAIN, trong ~/trafficsp-ml (sau train_remote.sh sync): mỗi nhóm thí nghiệm một tiến trình để giới hạn RAM.
cd ~/trafficsp-ml
export PYTHONPATH=$HOME/trafficsp-ml/jobs PYTHONUNBUFFERED=1
V="${ML_VERSION:-v2}"     # phiên bản dữ liệu/kết quả (conf/ml/forecast.yaml: version)
D="--data data/export/features_$V --baseline data/export/baseline_$V --out runs/$V"
# Baseline (persistence, seasonal naive) trước: train_lightgbm và evaluate.py đọc preds/frames của baseline cùng phiên bản
echo "=== baselines $(date -u +%FT%TZ)"
/usr/bin/time -f "peak_rss_kb=%M wall=%e" python3 jobs/ml/train_baselines.py $D || { echo "train_baselines lỗi, dừng"; exit 1; }
for E in tune,walk_forward,sliding,ablation,weighting,spatial weather_lag,avail_lag final; do
  echo "=== $E $(date -u +%FT%TZ)"
  /usr/bin/time -f "peak_rss_kb=%M wall=%e" python3 jobs/ml/train_lightgbm.py $D --experiments $E 2>&1 | grep -vE "^\[LightGBM\] \[Warning\]"
done
echo "=== ALL DONE $(date -u +%FT%TZ)"
