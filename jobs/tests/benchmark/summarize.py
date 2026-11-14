"""Tóm tắt 1 kịch bản benchmark Speed Layer (Spark -> Iceberg) trong khoảng giờ hệ thống [since, until].

Chạy trong container spark-master (có /opt/lakehouse/logs và mạng tới Trino):
  docker exec -e PYTHONPATH=/opt/spark/jobs traffic-spark-master python3 /opt/spark/jobs/tests/benchmark/summarize.py \
      --label b1 --since 2026-11-13T09:00:00 --until 2026-11-13T09:05:00
Nguồn số đo:
  - speed_progress.jsonl (StreamingQueryListener): số batch, dòng, thời lượng trigger, add_batch (gồm commit Iceberg của
    sink gốc), lag Kafka (maxOffsetsBehindLatest), watermark, state, dòng bị loại vì watermark.
  - speed_latency.jsonl (sink latest): độ trễ end-to-end = lúc MERGE xong - kafka_ts; thời gian MERGE INTO.
  - Trino: độ trễ 5 phút = $file_modified_time (file Parquet được ghi) - kafka_ts_max của cửa sổ;
           số snapshot đã commit, số file dữ liệu và kích thước trung bình (đánh giá file nhỏ).
"""
import argparse
import datetime as dt
import json
import os
import statistics

from common import config
from common.trino_http import query


def pct(xs, p):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    k = max(0, min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1)))))
    return xs[k]


def ts(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(dt.timezone.utc)


def load(path, since, until, key):
    out = []
    if not os.path.exists(path):
        return out
    for line in open(path, encoding="utf-8"):
        try:
            r = json.loads(line)
            t = ts(r[key]) if key == "ts" else dt.datetime.fromtimestamp(r[key], dt.timezone.utc)
        except (KeyError, ValueError, TypeError):
            continue
        if since <= t <= until:
            out.append(r)
    return out


def summarize(label, since, until, log_dir, ns):
    s, u = ts(since + "Z"), ts(until + "Z")
    prog = load(os.path.join(log_dir, "speed_progress.jsonl"), s, u, "ts")
    res = {"label": label, "since": since, "until": until, "queries": {}}
    for q in sorted({r["query"] for r in prog}):
        rs = [r for r in prog if r["query"] == q and r.get("num_input_rows")]
        busy = sum(r["trigger_execution_ms"] for r in rs)
        res["queries"][q] = {
            "batches": len(rs), "rows": sum(r["num_input_rows"] for r in rs),
            "rows_per_sec_processing": round(sum(r["num_input_rows"] for r in rs) / (busy / 1000), 1) if busy else None,
            "trigger_ms_p50": pct([r["trigger_execution_ms"] for r in rs], 50), "trigger_ms_p95": pct([r["trigger_execution_ms"] for r in rs], 95),
            "trigger_ms_max": max((r["trigger_execution_ms"] for r in rs), default=None),
            "add_batch_ms_p50": pct([r["add_batch_ms"] for r in rs], 50), "add_batch_ms_p95": pct([r["add_batch_ms"] for r in rs], 95),
            "max_offsets_behind": max((int(float(r["max_offsets_behind"] or 0)) for r in rs), default=0),
            "max_state_rows": max((r["state_rows"] for r in rs), default=0),
            "max_state_mb": round(max((r["state_memory_bytes"] for r in rs), default=0) / 2 ** 20, 1),
            "dropped_by_watermark": sum(r["rows_dropped_by_watermark"] for r in rs),
            "last_watermark": rs[-1]["watermark"] if rs else None}
    lat = load(os.path.join(log_dir, "speed_latency.jsonl"), s, u, "done_at_epoch")
    if lat:
        e2e = lambda k: [(r["done_at_epoch"] - ts(r[k]).timestamp()) * 1000 for r in lat if r.get(k)]
        res["queries"].setdefault("speed_latest", {})["latency"] = {
            "batches": len(lat), "e2e_ms_p50": round(statistics.median(e2e("kafka_ts_p50"))),
            "e2e_ms_p95": round(pct(e2e("kafka_ts_p05"), 95)), "e2e_ms_max": round(max(e2e("kafka_ts_min"))),
            "merge_ms_p50": pct([r["merge_ms"] for r in lat], 50), "merge_ms_p95": pct([r["merge_ms"] for r in lat], 95)}
    sch = ns.split(".", 1)[1]
    rng = f"BETWEEN TIMESTAMP '{since.replace('T', ' ')} UTC' AND TIMESTAMP '{until.replace('T', ' ')} UTC'"
    l5 = query(f"SELECT approx_percentile(ms, 0.5), approx_percentile(ms, 0.95), max(ms), count(*) FROM ("
               f"SELECT date_diff('millisecond', kafka_ts_max, \"$file_modified_time\") ms FROM lakehouse.{sch}.traffic_5min_rt "
               f"WHERE processed_at {rng})")[0]
    res["queries"].setdefault("speed_5min", {})["latency"] = {"windows": l5[3], "e2e_ms_p50": l5[0], "e2e_ms_p95": l5[1], "e2e_ms_max": l5[2]}
    res["iceberg"] = {}
    for t in ("traffic_5min_rt", "traffic_latest", "weather_obs"):
        snaps = query(f"SELECT count(*) FROM lakehouse.{sch}.\"{t}$snapshots\" WHERE committed_at {rng}")[0][0]
        f = query(f"SELECT count(*), coalesce(avg(file_size_in_bytes), 0), coalesce(sum(record_count), 0) FROM lakehouse.{sch}.\"{t}$files\"")[0]
        total_snaps = query(f"SELECT count(*) FROM lakehouse.{sch}.\"{t}$snapshots\"")[0][0]
        res["iceberg"][t] = {"snapshots_in_window": snaps, "snapshots_total": total_snaps, "data_files": f[0],
                             "avg_file_kb": round(f[1] / 1024, 1), "records": f[2]}
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--since", required=True, help="giờ hệ thống UTC, vd 2026-11-13T09:00:00")
    ap.add_argument("--until", required=True)
    ap.add_argument("--log-dir", default=config.OPS_LOG_DIR)
    ap.add_argument("--namespace", default=config.SPEED_NAMESPACE)
    ap.add_argument("--out")
    a = ap.parse_args()
    r = summarize(a.label, a.since, a.until, a.log_dir, a.namespace)
    print(json.dumps(r, indent=1, default=str))
    if a.out:
        with open(a.out, "a", encoding="utf-8") as f:
            f.write(json.dumps(r, default=str) + "\n")
