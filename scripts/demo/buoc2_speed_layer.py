"""BƯỚC 2 — Speed Layer (làn nhanh) đọc Kafka và ghi Iceberg mỗi 30 giây.
Code chính: jobs/stream/speed_layer.py
  parse_traffic  : đổi giờ New York -> UTC (time_rules.with_event_time), gắn cờ hợp lệ (quality_rules.with_quality)
  window_5min    : watermark 10' + khử trùng + gom cửa sổ 5 phút -> speed.traffic_5min_rt
  latest_source + merge_sql : MERGE INTO speed.traffic_latest (bản mới mới được ghi đè)
"""
import json

from _demo import run, say, short, table, title, tq

title("BƯỚC 2: SPEED LAYER — Spark Structured Streaming (speed_layer.py)")

say("Tiến trình Speed Layer đang chạy trong container spark-master (run_speed_layer.sh giữ nó sống):")
run(["docker", "exec", "traffic-spark-master", "bash", "-c",
     "tail -1 /opt/lakehouse/logs/speed_supervisor.log; "
     "echo \"   số tiến trình Speed Layer: $(pgrep -fc '^bash /opt/spark/jobs/stream/run_speed_layer')\""])

say("8 micro-batch gần nhất (mỗi dòng = 1 lần đọc Kafka ~30 giây):")
out = run(["docker", "exec", "traffic-spark-master", "bash", "-c",
           "grep -E '\"num_input_rows\": [1-9]' /opt/lakehouse/logs/speed_progress.jsonl | tail -n 8"], show=False)
rows = []
for line in out.splitlines():
    try:
        r = json.loads(line)
    except ValueError:
        continue
    rows.append([r["ts"][11:19], r["query"], r["batch_id"], r["num_input_rows"], f'{r["trigger_execution_ms"] / 1000:.1f}s',
                 (r.get("watermark") or "")[:16].replace("T", " "), r.get("rows_dropped_by_watermark", 0)])
table(rows, ["giờ thật (UTC)", "luồng", "batch", "dòng đọc", "thời gian", "watermark (giờ dữ liệu)", "bị loại (trễ)"])

say("Các lần ghi (commit) vào bảng Iceberg speed.traffic_5min_rt — mỗi micro-batch = 1 snapshot:")
table(tq("""SELECT CAST(committed_at AS varchar), element_at(summary, 'spark.sql.streaming.epochId'),
                   element_at(summary, 'added-records'), element_at(summary, 'added-data-files')
            FROM lakehouse.speed."traffic_5min_rt$snapshots"
            WHERE element_at(summary, 'spark.sql.streaming.epochId') IS NOT NULL
            ORDER BY committed_at DESC LIMIT 6"""),
      ["lúc commit (giờ thật)", "micro-batch", "dòng 5 phút thêm", "file thêm"])

say("Vài dòng 5 phút mới nhất của 1 con đường (speed.traffic_5min_rt):")
link = tq("SELECT link_id FROM lakehouse.speed.traffic_5min_rt ORDER BY window_start_utc DESC LIMIT 1")[0][0]
table(tq(f"""SELECT CAST(window_start_utc AS varchar), n_records, n_valid, n_zero, avg_speed, min_speed, max_speed, quality_flag
             FROM lakehouse.speed.traffic_5min_rt WHERE link_id = {link}
             ORDER BY window_start_utc DESC LIMIT 6"""),
      ["cửa sổ 5 phút (UTC)", "bản ghi", "hợp lệ", "speed=0", "avg", "min", "max", "cờ"])
print(f"(link_id = {link}). Cửa sổ chỉ được ghi khi watermark (giờ dữ liệu mới nhất - 10 phút) vượt qua cuối cửa sổ.")
