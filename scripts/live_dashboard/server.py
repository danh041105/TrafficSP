"""Dashboard trực tiếp TrafficSP (demo): server nhỏ chạy trên máy host, hỏi Trino qua REST rồi trả JSON cho index.html.
Không ghi gì vào hệ thống, chỉ đọc.

Chạy (ở thư mục gốc repo, Trino đã mở cổng 8088 trên host):
    python scripts/live_dashboard/server.py            -> mở http://localhost:8765
Biến môi trường: TRINO_URL (mặc định http://localhost:8088), DASH_PORT (8765).
"""
import datetime as dt
import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "jobs"))
from common.trino_http import query  # noqa: E402

TRINO = os.getenv("TRINO_URL", "http://localhost:8088")
PORT = int(os.getenv("DASH_PORT", "8765"))
CACHE_SEC = 15          # tối đa 1 lượt truy vấn Trino / 15 giây dù nhiều tab cùng mở (đỡ tải máy)
_cache = {"at": 0.0, "data": None}
_lock = threading.Lock()
_kafka = {"offsets": None, "at": None}


def q(sql):
    return query(sql, url=TRINO, user="dashboard")


def kafka_loop():
    """Tổng số message trong topic traffic_raw (đọc offset cuối mỗi partition) - 10 giây/lần."""
    while True:
        try:
            out = subprocess.run(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh",
                                  "--bootstrap-server", "kafka:29092", "--topic", "traffic_raw"],
                                 capture_output=True, text=True, timeout=30).stdout
            _kafka["offsets"] = sum(int(l.rsplit(":", 1)[1]) for l in out.splitlines() if l.count(":") == 2)
            _kafka["at"] = time.time()
        except Exception:
            pass
        time.sleep(10)


def collect() -> dict:
    status = q("SELECT CAST(t_batch AS varchar), speed_ahead_of_batch_min, last_status, CAST(last_change_at AS varchar) "
               "FROM lakehouse.serving.v_lambda_status")[0]
    clock = q("SELECT CAST(max(obs_event_time) AS varchar) FROM lakehouse.speed.traffic_latest")[0][0]
    links = q("""SELECT l.link_id, d.link_name, d.borough, d.latitude, d.longitude, l.traffic_state, l.speed,
                        d.free_flow_speed, CAST(l.last_observation_time AS varchar), l.valid_age_min
                 FROM lakehouse.serving.v_traffic_latest l JOIN lakehouse.serving.v_link_dim d ON d.link_id = l.link_id
                 WHERE d.latitude IS NOT NULL""")
    series = []
    if clock:
        c = dt.datetime.strptime(clock[:19], "%Y-%m-%d %H:%M:%S")
        lo = (c - dt.timedelta(hours=30)).strftime("%Y-%m-%d %H:%M:%S")
        series = q(f"""SELECT CAST(window_start_utc AS varchar), layer, sum(sum_speed) / nullif(sum(n_valid), 0), sum(n_records),
                              count_if(n_valid = 0)
                       FROM lakehouse.serving.v_traffic_5min_unified
                       WHERE window_start_utc >= TIMESTAMP '{lo} UTC'
                       GROUP BY 1, 2 ORDER BY 1""")
    # chỉ các commit của Speed Layer (có epochId); snapshot do compaction tạo không có các khoá này
    commits = q("""SELECT CAST(committed_at AS varchar), element_at(summary, 'added-records'),
                          element_at(summary, 'spark.sql.streaming.epochId'), operation
                   FROM lakehouse.speed."traffic_5min_rt$snapshots"
                   WHERE element_at(summary, 'spark.sql.streaming.epochId') IS NOT NULL
                   ORDER BY committed_at DESC LIMIT 12""")
    latest_commits = q("""SELECT CAST(committed_at AS varchar) FROM lakehouse.speed."traffic_latest$snapshots"
                          ORDER BY committed_at DESC LIMIT 1""")
    totals = q("""SELECT (SELECT count(*) FROM lakehouse.speed.traffic_5min_rt),
                         (SELECT count(*) FROM lakehouse.gold.traffic_5min_batch FOR VERSION AS OF 'published')""")[0]
    recent = q("""SELECT l.link_id, d.link_name, CAST(l.obs_event_time AS varchar), l.obs_speed, l.obs_is_valid,
                         coalesce(l.obs_invalid_reason, '')
                  FROM lakehouse.speed.traffic_latest l LEFT JOIN lakehouse.serving.v_link_dim d ON d.link_id = l.link_id
                  ORDER BY l.obs_event_time DESC, l.link_id LIMIT 12""")
    return {"now": dt.datetime.now().strftime("%H:%M:%S"), "event_clock": clock,
            "t_batch": status[0], "speed_ahead_min": status[1], "last_status": status[2], "last_change_at": status[3],
            "links": links, "series": series, "commits": commits,
            "latest_commit": latest_commits[0][0] if latest_commits else None,
            "rows_speed_total": totals[0], "rows_batch_published": totals[1],
            "kafka_messages": _kafka["offsets"], "recent": recent}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/data"):
            with _lock:
                if _cache["data"] is None or time.time() - _cache["at"] > CACHE_SEC:
                    try:
                        _cache["data"] = collect()
                    except Exception as e:                      # Trino tạm lỗi: báo lên trang, không chết server
                        _cache["data"] = {"error": str(e)}
                    _cache["at"] = time.time()
                body = json.dumps(_cache["data"], default=str).encode("utf-8")
            self._send(200, body, "application/json; charset=utf-8")
        elif self.path in ("/", "/index.html"):
            self._send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
        else:
            self._send(404, b"not found", "text/plain")


if __name__ == "__main__":
    threading.Thread(target=kafka_loop, daemon=True).start()
    print(f"[dashboard] http://localhost:{PORT}  (Trino: {TRINO})", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
