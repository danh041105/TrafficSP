"""Phiên phát lại kiểm chứng (demo) chạy qua Kafka và Spark Structured Streaming thật, tách khỏi hệ thống chính.

Vì sao cần làn riêng: toàn bộ dữ liệu 2025 đã được phát vào hệ thống chính, nên batch_state đã ở cuối dữ liệu.
Phát lại cùng dữ liệu vào topic chính có hai hậu quả:
- Bronze bị nhân bản;
- event cũ hơn watermark bị Speed Layer bỏ, nên không chứng minh được gì.
Mỗi phiên <run> có tài nguyên riêng, đủ cả hai lớp Lambda:
- topic Kafka traffic_raw_<run>, weather_raw_<run>;
- Speed Layer: namespace lakehouse.speed_<run>, checkpoint checkpoints/demo/<run>;
- Batch Layer: lakehouse.bronze_<run> -> silver_<run> -> gold_<run> (fact_traffic_5min/hourly + lambda_control),
  do DAG trafficsp_lambda_batch_demo chạy định kỳ với đúng các job batch của làn chính; handoff thật dời T_demo;
- bảng dự báo lakehouse.ml.traffic_forecast_15m_<run>;
- view serving.v_demo_* (jobs/serving/demo_views.py): batch chính trước mốc bắt đầu, batch của phiên tới T_demo,
  Speed của phiên từ T_demo.
batch_state, bảng Gold và Speed chính không bị đụng tới; không xóa dữ liệu nào. Bảng chiều gold.dim_road_link dùng chung (chỉ đọc).

Giờ sự kiện giữ nguyên (không dịch timestamp): đồng hồ sự kiện của làn demo đi từ --start tới --end
với tốc độ gấp --speedup lần thời gian thực.

  python scripts/demo_replay.py start --start 2025-12-30T13:00 --end 2025-12-30T17:00 --speedup 10 --data-dir <thư mục data có raw/>
  python scripts/demo_replay.py status
  python scripts/demo_replay.py stop        # dừng Speed Layer demo, chạy lại Speed Layer chính; giữ nguyên dữ liệu phiên

Lệnh start chạy tiền cảnh cho tới khi producer phát xong, ghi trạng thái vào
/opt/lakehouse/logs/replay_status_<run>.json mỗi 10 giây.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "jobs"))
from serving.demo_views import views_sql  # noqa: E402
SPARK = "traffic-spark-master"
TRINO = "traffic-trino"
KAFKA = "kafka"
NETWORK = os.getenv("TRAFFIC_NETWORK", "traffic-net")
LOGS = "/opt/lakehouse/logs"              # thư mục log vận hành dùng chung (spark-master, scheduler, dashboard)
LANE_FILE = f"{LOGS}/demo_lane.json"
ENV_FILE = f"{LOGS}/demo_lane.env"       # DAG trafficsp_lambda_batch_demo nạp file này trước mỗi job (không chứa bí mật)
ENV = {**os.environ, "MSYS_NO_PATHCONV": "1"}


def sh(*cmd, check=True, capture=True, input_=None) -> str:
    r = subprocess.run(list(cmd), capture_output=capture, text=True, encoding="utf-8", errors="replace", env=ENV, input=input_)
    if check and r.returncode != 0:
        # spark-submit in log ShutdownHookManager sau traceback Python: lấy từ traceback cuối nếu có, không chỉ đuôi stderr
        err = r.stderr or r.stdout or ""
        i = err.rfind("Traceback (most recent call last)")
        detail = err[i:i + 2000] if i >= 0 else err[-800:]
        raise RuntimeError(f"{' '.join(cmd[:6])}… lỗi {r.returncode}: {detail}")
    return r.stdout if capture else ""


def ny_hm(t_utc: dt.datetime) -> str:
    """Giờ New York của một mốc UTC (naive). Windows thiếu tzdata thì dùng quy tắc DST của Mỹ."""
    try:
        from zoneinfo import ZoneInfo
        return t_utc.replace(tzinfo=dt.timezone.utc).astimezone(ZoneInfo("America/New_York")).strftime("%H:%M")
    except Exception:  # noqa: BLE001
        y = t_utc.year
        dst_start = dt.datetime(y, 3, 8 + (6 - dt.date(y, 3, 8).weekday()) % 7, 7)    # Chủ nhật thứ 2 tháng 3, 2h EST
        dst_end = dt.datetime(y, 11, 1 + (6 - dt.date(y, 11, 1).weekday()) % 7, 6)    # Chủ nhật đầu tháng 11, 2h EDT
        return (t_utc - dt.timedelta(hours=4 if dst_start <= t_utc < dst_end else 5)).strftime("%H:%M")


def put_file(path: str, text: str):
    # Ghi dạng byte: subprocess chế độ text trên Windows đổi LF thành CRLF, file env thành "1<CR>" (DAG demo tưởng phiên dừng)
    r = subprocess.run(["docker", "exec", "-i", SPARK, "sh", "-c", f"mkdir -p $(dirname {path}) && cat > {path}"],
                       input=text.encode("utf-8"), capture_output=True, env=ENV)
    if r.returncode != 0:
        raise RuntimeError(f"ghi {path} lỗi: {r.stderr.decode('utf-8', 'replace')[-400:]}")


def read_file(path: str) -> str | None:
    r = subprocess.run(["docker", "exec", SPARK, "cat", path], capture_output=True, text=True, encoding="utf-8", env=ENV)
    return r.stdout if r.returncode == 0 else None


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def trino_sql(sql: str):
    sh("docker", "exec", "-i", TRINO, "trino", "--catalog", "lakehouse", "--file", "/dev/stdin", input_=sql)


def spark_submit(script: str, env: dict | None = None, *args):
    e = sum((["-e", f"{k}={v}"] for k, v in (env or {}).items()), [])
    return sh("docker", "exec", *e, "-e", "PYTHONPATH=/opt/spark/jobs", SPARK, "/opt/spark/bin/spark-submit",
              "--conf", "spark.cores.max=1", "--conf", "spark.ui.enabled=false", f"/opt/spark/jobs/{script}", *args)


# ---------------------------------------------------------------- Speed Layer
def speed_queries() -> int:
    # Hỏi Spark UI từ TRONG spark-master (driver Speed Layer chạy ở đó). Cổng 4041 trên host map vào airflow-scheduler,
    # nên curl localhost:4041 từ host không thấy Speed Layer và luôn đếm ra 0.
    r = subprocess.run(["docker", "exec", SPARK, "curl", "-s", "-m", "5", "http://localhost:4041/StreamingQuery/"],
                       capture_output=True, text=True, encoding="utf-8", errors="replace", env=ENV)
    m = re.search(r"Active Streaming Queries \((\d+)\)", r.stdout or "")
    return int(m.group(1)) if m else 0


def stop_speed(log_dir: str):
    sh("docker", "exec", "-e", f"LOG_DIR={log_dir}", SPARK, "bash", "/opt/spark/jobs/stream/stop_speed_layer.sh", check=False)


def start_speed(env: dict, log_dir: str, starting: str, trigger: str):
    e = sum((["-e", f"{k}={v}"] for k, v in env.items()), [])
    sh("docker", "exec", "-d", *e, "-e", f"LOG_DIR={log_dir}", SPARK, "bash", "/opt/spark/jobs/stream/run_speed_layer.sh",
       "--starting-offsets", starting, "--trigger", trigger, "--ops-log-dir", log_dir)
    for _ in range(60):
        if speed_queries() == 3:
            return
        time.sleep(5)
    raise RuntimeError("Speed Layer chưa chạy đủ 3 query sau 5 phút")


# ---------------------------------------------------------------- lệnh
def cmd_start(a):
    start = dt.datetime.fromisoformat(a.start).replace(tzinfo=None)
    end = dt.datetime.fromisoformat(a.end).replace(tzinfo=None)
    run = a.run_id or f"demo{start:%m%d%H%M}_{dt.datetime.now():%H%M%S}"
    topics = {"TOPIC_TRAFFIC": f"traffic_raw_{run}", "TOPIC_WEATHER": f"weather_raw_{run}"}
    ns = {k: f"lakehouse.{k.split('_')[0].lower()}_{run}" for k in ("SPEED_NAMESPACE", "BRONZE_NAMESPACE", "SILVER_NAMESPACE", "GOLD_NAMESPACE")}
    ftable = f"lakehouse.ml.traffic_forecast_15m_{run}"
    ckpt = f"s3a://lakehouse/checkpoints/demo/{run}"
    log_dir, status_path = f"{LOGS}/demo/{run}", f"{LOGS}/replay_status_{run}.json"
    env = {"DEMO_RUN_ID": run, **topics, **ns, "GOLD_DIM_ROAD_LINK": "lakehouse.gold.dim_road_link", "CHECKPOINT_ROOT": ckpt,
           "DEMO_BATCH_ACTIVE": "0" if a.no_batch else "1"}
    status = {"run_id": run, "state": "starting", "start_utc": start.isoformat() + "Z", "end_utc": end.isoformat() + "Z",
              "cut_utc": start.isoformat() + "Z", "speedup": a.speedup, "speed_namespace": ns["SPEED_NAMESPACE"],
              "gold_namespace": ns["GOLD_NAMESPACE"], "forecast_table": ftable, "topics": list(topics.values()),
              "batch_lane": not a.no_batch, "event_time_utc": None, "sent": 0, "failed": 0, "started_at": now(), "updated_at": now()}

    def save(**kv):
        status.update(kv, updated_at=now())
        put_file(status_path, json.dumps(status, ensure_ascii=False, indent=1))
        if kv or status["state"] != "running":
            print(f"[demo] {status['state']}: {kv}", flush=True)
            return
        # nhịp 10 giây khi đang phát: in tiến độ thay vì "running: {}"
        ev = status.get("event_time_utc")
        if not ev:
            print("[demo] đang phát: producer đang khởi động (đọc parquet, kết nối Kafka)", flush=True)
            return
        t = dt.datetime.fromisoformat(ev.replace("Z", ""))
        pct = 100 * (t - start).total_seconds() / max(1.0, (end - start).total_seconds())
        print(f"[demo] đang phát: giờ dữ liệu {t:%H:%M} UTC ({ny_hm(t)} New York), "
              f"đã gửi {status['sent']:,} message, {pct:.0f}% khoảng giờ", flush=True)

    save()
    # Số partition giống topic chính: traffic_raw 4 (key link_id), weather_raw 1 (key cell_id)
    for key, t in topics.items():
        parts = "4" if key == "TOPIC_TRAFFIC" else "1"
        sh("docker", "exec", KAFKA, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create",
           "--if-not-exists", "--topic", t, "--partitions", parts, "--replication-factor", "1")
    ddl_env = {k: v for k, v in env.items() if k.endswith("_NAMESPACE") or k == "GOLD_DIM_ROAD_LINK"}
    for ddl in ("ddl/speed.py", "ddl/bronze.py", "ddl/silver.py", "ddl/gold.py"):   # gold.py tạo tag published (rỗng)
        spark_submit(ddl, ddl_env)
    spark_submit("ml/forecast_table.py", None, "--table", ftable)
    lane = {"run_id": run, "forecast_table": ftable, "speed_namespace": ns["SPEED_NAMESPACE"],
            "bronze_namespace": ns["BRONZE_NAMESPACE"], "silver_namespace": ns["SILVER_NAMESPACE"],
            "gold_namespace": ns["GOLD_NAMESPACE"], "topics": topics, "cut_utc": status["cut_utc"],
            "ops_log_dir": log_dir, "status_file": status_path, "batch_lane": not a.no_batch}
    trino_sql(views_sql(lane))
    put_file(LANE_FILE, json.dumps(lane, indent=1))
    put_file(ENV_FILE, "".join(f"{k}={v}" + chr(10) for k, v in env.items()))
    save(state="views_ready")

    # Speed Layer chính đang rảnh (đã đọc hết dữ liệu) -> dừng để nhường core/RAM cho Speed Layer của phiên demo
    stop_speed(LOGS)
    start_speed({"SPEED_NAMESPACE": ns["SPEED_NAMESPACE"], "CHECKPOINT_ROOT": ckpt, **topics}, log_dir, "latest", a.trigger)
    save(state="speed_ready")

    name = f"traffic-replay-{run}"
    data_dir = Path(a.data_dir).resolve()
    sh("docker", "run", "-d", "--name", name, "--network", NETWORK, "-e", "KAFKA_BOOTSTRAP=kafka:29092",
       "-e", "PYTHONPATH=/app/jobs", *sum((["-e", f"{k}={v}"] for k, v in topics.items()), []),
       "-v", f"{REPO / 'jobs'}:/app/jobs:ro", "-v", f"{REPO / 'scripts'}:/app/scripts:ro", "-v", f"{data_dir}:/app/data:ro",
       "ad/producer:3.10", "python", "/app/scripts/replay_producer.py", "--phase", "gd2", "--run-id", run,
       "--start", f"{start:%Y-%m-%dT%H:%M}", "--end", f"{end:%Y-%m-%dT%H:%M}", "--speedup", str(a.speedup),
       "--weather-delay-min", "0", "--log-every-sec", "10")
    save(state="running", producer_container=name)

    pat = re.compile(r"giờ dữ liệu (\d{4}-\d\d-\d\d \d\d:\d\d) UTC, traffic ([\d,]+), weather ([\d,]+)")
    while True:
        logs = sh("docker", "logs", "--tail", "20", name, check=False)
        m = None
        for m in pat.finditer(logs):
            pass
        if m:
            status.update(event_time_utc=m.group(1).replace(" ", "T") + ":00Z",
                          sent=int(m.group(2).replace(",", "")) + int(m.group(3).replace(",", "")))
        st = sh("docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", name, check=False).split()
        if st and st[0] == "exited":
            fin = re.search(r"\[gd2\] ([\d,]+) message .* đến (\d{4}-\d\d-\d\d \d\d:\d\d) UTC", logs)
            if fin:
                status.update(sent=int(fin.group(1).replace(",", "")), event_time_utc=fin.group(2).replace(" ", "T") + ":00Z")
            save(state="finished" if st[1] == "0" else "failed", producer_exit_code=int(st[1]))
            break
        save()
        time.sleep(10)
    print("[demo] Producer đã phát xong. Speed Layer demo vẫn chạy để cửa sổ cuối được phát; "
          "chạy `python scripts/demo_replay.py stop` khi kết thúc phiên.", flush=True)


def cmd_stop(_):
    lane = json.loads(read_file(LANE_FILE) or "{}")
    env = read_file(ENV_FILE)
    if env:   # DAG batch demo thấy DEMO_BATCH_ACTIVE=0 -> bỏ qua các lượt sau
        put_file(ENV_FILE, env.replace("DEMO_BATCH_ACTIVE=1", "DEMO_BATCH_ACTIVE=0"))
    if lane:
        stop_speed(lane["ops_log_dir"])
        st = json.loads(read_file(lane["status_file"]) or "{}")
        if st and st.get("state") not in ("finished", "failed"):   # phiên đã phát xong hoặc lỗi giữ nguyên trạng thái thật
            st.update(state="stopped", updated_at=now())
            put_file(lane["status_file"], json.dumps(st, ensure_ascii=False, indent=1))
    # chạy lại Speed Layer chính từ checkpoint của nó (không mất offset)
    sh("docker", "exec", SPARK, "rm", "-f", f"{LOGS}/speed_layer.stop", check=False)
    start_speed({}, LOGS, "latest", "30 seconds")
    print("[demo] Đã dừng Speed Layer demo và chạy lại Speed Layer chính. Dữ liệu và view của phiên được giữ nguyên.")


def cmd_status(_):
    lane = json.loads(read_file(LANE_FILE) or "{}")
    print(json.dumps({"lane": lane, "status": json.loads(read_file(lane["status_file"]) or "{}") if lane else None,
                      "speed_queries": speed_queries()}, ensure_ascii=False, indent=1))


def main():
    # console Windows mặc định cp1252 không in được tiếng Việt (lệnh stop từng dừng ở dòng print cuối)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start")
    s.add_argument("--start", required=True, help="giờ sự kiện UTC bắt đầu, vd 2025-12-30T13:00")
    s.add_argument("--end", required=True, help="giờ sự kiện UTC kết thúc (không gồm)")
    s.add_argument("--speedup", type=float, default=10)
    s.add_argument("--trigger", default="15 seconds")
    s.add_argument("--data-dir", default=str(REPO / "data"), help="thư mục chứa raw/nyc_dot và raw/open_meteo")
    s.add_argument("--run-id")
    s.add_argument("--no-batch", action="store_true", help="chỉ Speed Layer (không chạy làn batch, T_demo giữ ở mốc bắt đầu)")
    sub.add_parser("stop")
    sub.add_parser("status")
    a = ap.parse_args()
    {"start": cmd_start, "stop": cmd_stop, "status": cmd_status}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
