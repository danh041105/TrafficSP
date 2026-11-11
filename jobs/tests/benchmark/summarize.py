"""Tóm tắt 1 kịch bản benchmark Speed Layer từ ClickHouse (stream_progress + speed_latency) trong khoảng giờ hệ thống.

Chạy trong container Spark (có clickhouse_connect):
  docker exec -e PYTHONPATH=/opt/spark/jobs traffic-spark-master python3 /opt/spark/jobs/tests/benchmark/summarize.py \
      --label baseline --since 2026-11-11T16:40:00 --until 2026-11-11T16:45:00 [--out /opt/lakehouse/logs/benchmark.jsonl]
Số đo:
  - micro-batch: số batch có dữ liệu, tổng dòng, thời lượng trigger p50/p95/max, rows/s (= dòng / tổng thời gian xử lý)
  - lag: maxOffsetsBehindLatest lớn nhất (Kafka), watermark cuối, dòng bị loại vì watermark
  - state: số dòng state và bộ nhớ state lớn nhất (RocksDB)
  - độ trễ end-to-end (đồng hồ thật): done_at - kafka_ts; p50 = trung vị các batch của (done_at - kafka_ts_p50),
    p95 = phân vị 95 các batch của (done_at - kafka_ts_p05), max = max(done_at - kafka_ts_min)
"""
import argparse
import json

from common.clickhouse_io import get_client


def summarize(label: str, since: str, until: str) -> dict:
    c = get_client()
    rng = f"toDateTime64('{since.replace('T', ' ')}', 3, 'UTC') AND toDateTime64('{until.replace('T', ' ')}', 3, 'UTC')"
    out = {"label": label, "since": since, "until": until, "queries": {}}
    prog = c.query(f"""
        SELECT query, countIf(num_input_rows > 0) AS batches, sum(num_input_rows) AS rows,
               sum(trigger_execution_ms) AS busy_ms,
               quantileExactIf(0.5)(trigger_execution_ms, num_input_rows > 0) AS p50_ms,
               quantileExactIf(0.95)(trigger_execution_ms, num_input_rows > 0) AS p95_ms,
               maxIf(trigger_execution_ms, num_input_rows > 0) AS max_ms,
               max(num_input_rows) AS max_batch_rows,
               max(ifNull(max_offsets_behind, 0)) AS max_offsets_behind,
               max(state_rows) AS max_state_rows, max(state_memory_bytes) AS max_state_bytes,
               sum(rows_dropped_by_watermark) AS dropped_by_watermark,
               argMax(watermark, ts) AS last_watermark,
               dateDiff('millisecond', minIf(ts, num_input_rows > 0), maxIf(ts, num_input_rows > 0)) AS active_span_ms
        FROM stream_progress WHERE ts BETWEEN {rng} GROUP BY query ORDER BY query""").named_results()
    for r in prog:
        r = dict(r)
        r["rows_per_sec_processing"] = round(r["rows"] / (r["busy_ms"] / 1000), 1) if r["busy_ms"] else None
        out["queries"][r.pop("query")] = r
    lat = c.query(f"""
        SELECT query, count() AS batches, sum(n_items) AS items,
               quantileExact(0.5)(dateDiff('millisecond', kafka_ts_p50, done_at)) AS e2e_p50_ms,
               quantileExact(0.95)(dateDiff('millisecond', kafka_ts_p05, done_at)) AS e2e_p95_ms,
               max(dateDiff('millisecond', kafka_ts_min, done_at)) AS e2e_max_ms
        FROM speed_latency WHERE done_at BETWEEN {rng} GROUP BY query""").named_results()
    for r in lat:
        r = dict(r)
        out["queries"].setdefault(r.pop("query"), {})["latency"] = r
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--since", required=True, help="giờ hệ thống UTC, vd 2026-11-11T16:40:00")
    ap.add_argument("--until", required=True)
    ap.add_argument("--out", help="nối kết quả vào file JSONL")
    a = ap.parse_args()
    res = summarize(a.label, a.since, a.until)
    print(json.dumps(res, indent=1, default=str))
    if a.out:
        with open(a.out, "a", encoding="utf-8") as f:
            f.write(json.dumps(res, default=str) + "\n")
