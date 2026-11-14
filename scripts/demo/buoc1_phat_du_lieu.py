"""BƯỚC 1 — Phát dữ liệu vào Kafka (giả lập cảm biến giao thông + trạm thời tiết).
Code chính: scripts/replay_producer.py  (hàm iter_traffic, realtime_schedule, SimClock, send)
Tự chọn 6 giờ dữ liệu NGAY SAU dữ liệu đã phát, phát ở tốc độ x720 (6 giờ dữ liệu ~ 30 giây + khởi động).
"""
import datetime as dt
import json

from _demo import COMPOSE, kafka_total, run, say, short, table, title, tq

title("BƯỚC 1: PHÁT DỮ LIỆU VÀO KAFKA (replay_producer.py)")

clock = tq("SELECT CAST(max(obs_event_time) AS varchar) FROM lakehouse.speed.traffic_latest")[0][0]
last = dt.datetime.strptime(clock[:19], "%Y-%m-%d %H:%M:%S")
start = (last + dt.timedelta(minutes=59)).replace(minute=0, second=0)
end = start + dt.timedelta(hours=6)
say(f"Dữ liệu đã phát tới: {short(clock)} UTC  →  lần này phát {start:%Y-%m-%d %H:%M} → {end:%H:%M} UTC (6 giờ dữ liệu)")

before = kafka_total()
say(f"Kafka topic traffic_raw đang có {before:,} message")

say("Chạy producer: đọc file Parquet gốc (data/raw/nyc_dot), xếp theo giờ dữ liệu 2025, gửi vào Kafka x720")
run(COMPOSE + ["run", "--rm", "-e", "PYTHONUNBUFFERED=1", "producer", "python", "scripts/replay_producer.py",
               "--phase", "gd2", "--start", f"{start:%Y-%m-%dT%H:%M}", "--end", f"{end:%Y-%m-%dT%H:%M}",
               "--speedup", "720", "--log-every-sec", "5"])

after = kafka_total()
say(f"Kafka giờ có {after:,} message  (+{after - before:,} message mới)")

# 3 message cuối cùng của partition 0, đọc thẳng từ Kafka (đúng dạng JSON producer đã gửi)
offs = run(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "kafka:29092",
            "--topic", "traffic_raw", "--partitions", "0"], show=False)
end0 = int(offs.strip().splitlines()[-1].rsplit(":", 1)[1])
raw = run(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:29092",
           "--topic", "traffic_raw", "--partition", "0", "--offset", str(max(0, end0 - 3)), "--max-messages", "3",
           "--timeout-ms", "10000"], show=False)
rows = []
for line in raw.splitlines():
    try:
        m = json.loads(line)
        rows.append([m.get("link_id"), m.get("data_as_of"), m.get("speed"), m.get("travel_time"), (m.get("link_name") or "")[:40]])
    except ValueError:
        pass
say("3 message vừa gửi (đọc thẳng từ Kafka, giữ nguyên giá trị gốc):")
table(rows, ["link_id", "data_as_of (giờ New York)", "speed", "travel_time", "link_name"])
print("Xong bước 1. Dữ liệu đã nằm trong Kafka; Speed Layer sẽ đọc trong vòng 30 giây.")
