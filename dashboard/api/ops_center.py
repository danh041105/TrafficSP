"""API trung tâm vận hành (/api/ops/*). Mọi số liệu lấy từ nguồn thật; nguồn nào không có thì trả null (không mặc định 0).

Nguồn và phạm vi đo:
- Cửa sổ Batch/Speed/phục vụ: bảng Iceberg qua Trino. Batch đọc qua tag `published` (snapshot của từng handoff).
- Lịch sử handoff: gold[_<run>].lambda_control (append-only: PREPARING -> VALIDATED -> PUBLISHED | FAILED).
- Kafka messages/s: Prometheus, rate(kafka_topic_partition_current_offset) (kafka-exporter).
- Spark Structured Streaming: speed_progress.jsonl (StreamingQueryProgress của từng micro-batch).
- Độ trễ xử lý: speed_latency.jsonl, query speed_latest. Đo từ thời điểm message vào Kafka (kafka timestamp) tới
  lúc MERGE vào Iceberg xong: p50 = done_at - kafka_ts_p50; p95 = done_at - kafka_ts_p05 (5% message cũ nhất của lô).
- Trino: thời gian các truy vấn backend dashboard tự gửi (bộ đệm vòng trong bộ nhớ, mất khi restart container).
- Airflow: REST v2 (thời lượng lượt chạy và task).
- Suy luận: inference_runs.jsonl.
"""
from __future__ import annotations

import datetime as dt
import json
import urllib.parse
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

import app as core

router = APIRouter(prefix="/api/ops")
EPOCH_YEAR = 1971


# ---------------------------------------------------------------- tài nguyên theo làn
def lane_res(lane: str) -> dict:
    if lane == "main":
        return {"lane": "main", "bronze": "lakehouse.bronze.traffic_raw", "silver": "lakehouse.silver.traffic_cleaned",
                "gold5": "lakehouse.gold.fact_traffic_5min", "control": "lakehouse.gold.lambda_control",
                "speed5": "lakehouse.speed.traffic_5min_rt", "speed_latest": "lakehouse.speed.traffic_latest",
                "forecast": "lakehouse.ml.traffic_forecast_15m", "topic": "traffic_raw", "log_dir": core.OPS_LOG_DIR,
                "dag": "trafficsp_lambda_batch", "cut": None, "batch_state_view": "lakehouse.serving.v_batch_state",
                "run_id": None}
    ln = core.demo_lane()
    if not ln:
        raise HTTPException(404, "Chưa có phiên phát lại kiểm chứng")
    gold = ln.get("gold_namespace")
    return {"lane": "demo", "run_id": ln["run_id"],
            "bronze": f"{ln['bronze_namespace']}.traffic_raw" if ln.get("bronze_namespace") else None,
            "silver": f"{ln['silver_namespace']}.traffic_cleaned" if ln.get("silver_namespace") else None,
            "gold5": f"{gold}.fact_traffic_5min" if gold else None, "control": f"{gold}.lambda_control" if gold else None,
            "speed5": f"{ln['speed_namespace']}.traffic_5min_rt", "speed_latest": f"{ln['speed_namespace']}.traffic_latest",
            "forecast": ln["forecast_table"], "topic": (ln.get("topics") or {}).get("TOPIC_TRAFFIC", "traffic_raw_demo"),
            "log_dir": core.OPS_LOG_DIR / "demo" / ln["run_id"], "dag": "trafficsp_lambda_batch_demo" if gold else None,
            "cut": ln.get("cut_utc"), "batch_state_view": "lakehouse.serving.v_demo_batch_state" if gold else None}


def iso(t: dt.datetime | None) -> str | None:
    return t.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z") if t else None


def floor5(t: dt.datetime) -> dt.datetime:
    return t.replace(minute=t.minute - t.minute % 5, second=0, microsecond=0)


def batch_state(r: dict) -> dt.datetime | None:
    if r["batch_state_view"]:
        v = core.q(f"SELECT to_iso8601(batch_state) AS b FROM {r['batch_state_view']}", ttl=20)
        if v and v[0]["b"]:
            return core.parse_ts(v[0]["b"])
    return core.parse_ts(r["cut"]) if r["cut"] else None


def control_rows(r: dict) -> list[dict]:
    if not r["control"]:
        return []
    rows = core.q(f"""SELECT handoff_id, status, to_iso8601(batch_state_prev) AS batch_state_prev,
                             to_iso8601(batch_state) AS batch_state, batch_run_id, snapshot_5min, snapshot_hourly,
                             checks, message, to_iso8601(created_at) AS created_at
                      FROM {r['control']} ORDER BY created_at""", ttl=20)
    out = []
    for x in map(dict, rows):
        try:
            x["checks"] = json.loads(x.get("checks") or "null")
        except json.JSONDecodeError:
            x["checks"] = None
        out.append(x)
    return out


def jsonl(path: Path, n: int = 400) -> list[dict]:
    out = []
    for line in core.tail_lines(path, n):
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


# ---------------------------------------------------------------- 1. timeline Batch vs Speed
@router.get("/timeline")
def timeline(lane: str = "main", before_hours: float = Query(2, ge=0.5, le=24)):
    core.require_lane(lane)
    r = lane_res(lane)

    def run():
        clk = core.clock(lane)
        ev = core.parse_ts(clk["event_clock_utc"])
        tb = batch_state(r)
        if ev is None:
            return {"lane": lane, "run_id": r["run_id"], "range": None, "event_clock_utc": None, "batch_state": iso(tb),
                    "cut_utc": r["cut"], "speed_watermark_utc": None, "last_speed_window_utc": None, "windows": [],
                    "handoffs": [], "speed_track": [], "batch_lane": bool(r["control"]),
                    "empty": "Speed Layer của làn chưa ghi bản ghi nào vào traffic_latest (chưa xử lý lô đầu tiên)."}
        anchor = min(tb, ev) if tb else ev
        lo = floor5(anchor - dt.timedelta(hours=before_hours))
        hi = floor5(ev) + dt.timedelta(minutes=15)
        rng = f"window_start_utc >= {core.lit(lo)} AND window_start_utc < {core.lit(hi)}"
        if lane == "main":
            batch_sql = f"""SELECT to_iso8601(window_start_utc) AS t, count(*) AS n FROM {r['gold5']} FOR VERSION AS OF 'published'
                            WHERE {rng} GROUP BY 1"""
        else:
            cut = core.lit(core.parse_ts(r["cut"]))
            parts = [f"""SELECT window_start_utc FROM lakehouse.gold.fact_traffic_5min FOR VERSION AS OF 'published'
                         WHERE {rng} AND window_start_utc < {cut}"""]
            if r["gold5"]:
                parts.append(f"""SELECT window_start_utc FROM {r['gold5']} FOR VERSION AS OF 'published'
                                 WHERE {rng} AND window_start_utc >= {cut}""")
            batch_sql = f"SELECT to_iso8601(window_start_utc) AS t, count(*) AS n FROM ({' UNION ALL '.join(parts)}) GROUP BY 1"
        res = core.parallel(
            batch=lambda: core.q(batch_sql, ttl=20),
            speed=lambda: core.q(f"""SELECT to_iso8601(window_start_utc) AS t, count(*) AS n,
                                            to_iso8601(min(processed_at)) AS first_processed FROM {r['speed5']}
                                     WHERE {rng} GROUP BY 1""", ttl=20),
            serving=lambda: core.q(f"""SELECT to_iso8601(window_start_utc) AS t, layer, count(*) AS n
                                       FROM {core.view(lane, 'traffic_5min_unified')} WHERE {rng} GROUP BY 1, 2""", ttl=20),
            handoffs=lambda: control_rows(r))
        key = lambda v: iso(core.parse_ts(v))  # noqa: E731 - chuẩn hoá khoá thời gian của Trino
        b = {key(x["t"]): x["n"] for x in res["batch"]}
        s = {key(x["t"]): x for x in res["speed"]}
        sv: dict = {}
        for x in res["serving"]:
            sv.setdefault(key(x["t"]), {})[x["layer"]] = x["n"]
        prog = [p for p in jsonl(Path(r["log_dir"]) / "speed_progress.jsonl", 300) if p.get("query") == "speed_5min"]
        wm = core.parse_ts(prog[-1]["watermark"]) if prog and prog[-1].get("watermark") and not str(prog[-1]["watermark"]).startswith("1970") else None
        last_speed = max((core.parse_ts(k) for k in s), default=None)
        windows = []
        t = lo
        while t < hi:
            k = iso(t)
            nb, ns_ = b.get(k), (s.get(k) or {}).get("n")
            serv = sv.get(k, {})
            windows.append({"t": iso(t), "batch": nb, "speed": ns_, "serving_batch": serv.get("batch"),
                            "serving_speed": serv.get("speed"),
                            "superseded": bool(ns_ and tb and t < tb),
                            "pending": bool(last_speed is not None and t > last_speed and t <= ev) or
                                       bool(last_speed is None and tb is not None and t >= tb and t <= ev)})
            t += dt.timedelta(minutes=5)
        stairs = [{"at": h["created_at"], "batch_state": h["batch_state"], "batch_state_prev": h["batch_state_prev"],
                   "status": h["status"], "handoff_id": h["handoff_id"]} for h in res["handoffs"]]
        # Spark báo watermark 1970-01-01 cho các lô trước khi có dữ liệu -> bỏ (không phải mốc giờ sự kiện thật)
        speed_track = [{"at": p["ts"], "watermark": p["watermark"], "batch_id": p.get("batch_id")}
                       for p in prog if p.get("watermark") and not str(p["watermark"]).startswith("1970")]
        return {"lane": lane, "run_id": r["run_id"], "range": [iso(lo), iso(hi)], "event_clock_utc": iso(ev),
                "batch_state": iso(tb), "cut_utc": r["cut"], "speed_watermark_utc": iso(wm),
                "last_speed_window_utc": iso(last_speed), "windows": windows, "handoffs": stairs, "speed_track": speed_track,
                "batch_lane": bool(r["control"])}
    return core.cached(("ops_timeline", lane, before_hours), 15, run)


# ---------------------------------------------------------------- 2. handoff
def group_handoffs(rows: list[dict]) -> list[dict]:
    by: dict = {}
    for x in rows:
        h = by.setdefault(x["handoff_id"], {"handoff_id": x["handoff_id"], "events": [], "batch_state_prev": None,
                                            "batch_state": None, "snapshot_5min": None, "snapshot_hourly": None,
                                            "checks": None, "batch_run_id": None, "message": None})
        h["events"].append({"status": x["status"], "at": x["created_at"]})
        for k in ("batch_state_prev", "batch_state", "batch_run_id", "message"):
            h[k] = x[k] if x[k] is not None else h[k]
        if x["snapshot_5min"]:
            h["snapshot_5min"], h["snapshot_hourly"] = x["snapshot_5min"], x["snapshot_hourly"]
        if x["checks"]:
            h["checks"] = x["checks"]
    out = []
    for h in by.values():
        h["status"] = h["events"][-1]["status"]
        t0, t1 = core.parse_ts(h["events"][0]["at"]), core.parse_ts(h["events"][-1]["at"])
        h["started_at"], h["finished_at"] = h["events"][0]["at"], h["events"][-1]["at"]
        h["duration_s"] = round((t1 - t0).total_seconds(), 1) if t0 and t1 else None
        prev = core.parse_ts(h["batch_state_prev"]) if h["batch_state_prev"] else None
        if prev and prev.year < EPOCH_YEAR:
            h["batch_state_prev"] = None           # lượt đầu: chưa có batch_state trước
        c = h["checks"] or {}
        h["published_windows"] = c.get("batch_rows")
        h["speed_windows_compared"] = c.get("rt_windows_compared")
        h["speed_windows_without_batch"] = c.get("rt_windows_without_batch")
        out.append(h)
    return sorted(out, key=lambda h: h["started_at"], reverse=True)


@router.get("/handoffs")
def handoffs(lane: str = "main"):
    core.require_lane(lane)
    r = lane_res(lane)
    return core.cached(("ops_handoffs", lane), 15, lambda: {
        "lane": lane, "batch_state": iso(batch_state(r)), "handoffs": group_handoffs(control_rows(r)),
        "control_table": r["control"], "batch_lane": bool(r["control"])})


@router.get("/handoffs/{handoff_id}/windows")
def handoff_windows(handoff_id: str, lane: str = "main"):
    """Trước / sau một handoff, theo từng cửa sổ trong [T_trước, T_mới): Speed phục vụ trước, Batch (đúng snapshot
    mà handoff đó công bố) phục vụ sau."""
    core.require_lane(lane)
    r = lane_res(lane)
    h = next((x for x in group_handoffs(control_rows(r)) if x["handoff_id"] == handoff_id), None)
    if not h or not h["batch_state"]:
        raise HTTPException(404, f"Không có handoff {handoff_id}")
    hi = core.parse_ts(h["batch_state"])
    lo = core.parse_ts(h["batch_state_prev"]) if h["batch_state_prev"] else hi - dt.timedelta(hours=3)
    if (hi - lo) > dt.timedelta(days=2):
        lo = hi - dt.timedelta(days=2)
    rng = f"window_start_utc >= {core.lit(lo)} AND window_start_utc < {core.lit(hi)}"

    def run():
        snap = h["snapshot_5min"]
        batch = core.q(f"""SELECT to_iso8601(window_start_utc) AS t, count(*) AS n FROM {r['gold5']}
                           FOR VERSION AS OF {int(snap)} WHERE {rng} GROUP BY 1 ORDER BY 1""", ttl=300) if snap else []
        speed = core.q(f"""SELECT to_iso8601(window_start_utc) AS t, count(*) AS n FROM {r['speed5']}
                           WHERE {rng} GROUP BY 1 ORDER BY 1""", ttl=60)
        return {"handoff": h, "range": [iso(lo), iso(hi)], "batch_after": batch, "speed_before": speed,
                "note": "Sau handoff: số đoạn có cửa sổ Batch trong đúng snapshot được công bố. Trước handoff: số đoạn có cửa sổ "
                        "Speed (lớp phục vụ các cửa sổ ≥ batch_state trước đó)."}
    return core.cached(("ops_handoff_windows", lane, handoff_id), 30, run)


# ---------------------------------------------------------------- 3. metrics
def prom_range(expr: str, minutes: int = 60, step: int = 30) -> list[list]:
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(minutes=minutes)
    url = (f"{core.PROMETHEUS_URL}/api/v1/query_range?query={urllib.parse.quote(expr)}"
           f"&start={start.timestamp():.0f}&end={end.timestamp():.0f}&step={step}")
    res = core.http_json(url, timeout=10)["data"]["result"]
    return [[int(t), float(v)] for t, v in res[0]["values"]] if res else []


def epoch_to_iso(e: float) -> str:
    return dt.datetime.fromtimestamp(e, dt.timezone.utc).isoformat().replace("+00:00", "Z")


@router.get("/metrics")
def metrics(lane: str = "main", minutes: int = Query(60, ge=10, le=720)):
    core.require_lane(lane)
    r = lane_res(lane)

    def run():
        log_dir = Path(r["log_dir"])
        prog = jsonl(log_dir / "speed_progress.jsonl", 600)
        streaming = {}
        for p in prog:
            streaming.setdefault(p.get("query"), []).append(
                {k: p.get(k) for k in ("ts", "batch_id", "num_input_rows", "input_rows_per_sec", "processed_rows_per_sec",
                                       "trigger_execution_ms", "add_batch_ms", "commit_offsets_ms", "max_offsets_behind")})
        latency = []
        for x in jsonl(log_dir / "speed_latency.jsonl", 300):
            done = x.get("done_at_epoch")
            p50, p05 = (core.parse_ts((x.get(k) or "").replace(" ", "T") + "+00:00") if x.get(k) else None
                        for k in ("kafka_ts_p50", "kafka_ts_p05"))
            if done and p50 and p05:
                latency.append({"ts": epoch_to_iso(done), "batch_id": x.get("batch_id"), "n_items": x.get("n_items"),
                                "p50_s": round(done - p50.timestamp(), 2), "p95_s": round(done - p05.timestamp(), 2),
                                "merge_ms": x.get("merge_ms")})
        kafka = core.safe(lambda: {
            "topic": r["topic"],
            "rate": [[epoch_to_iso(t), v] for t, v in prom_range(
                f'sum(rate(kafka_topic_partition_current_offset{{topic="{r["topic"]}"}}[1m]))', minutes)],
            "total": [[epoch_to_iso(t), v] for t, v in prom_range(
                f'sum(kafka_topic_partition_current_offset{{topic="{r["topic"]}"}})', minutes, 60)]})
        dag_runs = core.safe(lambda: dag_history(r["dag"])) if r["dag"] else None
        inference = [x for x in jsonl(core.OPS_LOG_DIR / "inference_runs.jsonl", 400) if x.get("lane") == lane][-60:]
        trino = list(core.TRINO_TIMINGS)[-200:]
        return {"lane": lane, "streaming": streaming, "latency": latency[-200:], "kafka": kafka, "dag": dag_runs,
                "inference": inference, "trino": trino, "minutes": minutes,
                "not_available": {
                    "kafka_consumer_lag": "Spark Structured Streaming không commit offset về consumer group Kafka, nên kafka-exporter "
                                          "không có lag. Thay bằng số offset chưa đọc do chính query báo (max_offsets_behind).",
                    "iceberg_commit_duration": "Không có metric riêng cho commit Iceberg. add_batch_ms là toàn bộ thời gian ghi lô "
                                               "(gồm commit Iceberg) do Spark báo.",
                    "latency_scope": "Chỉ query speed_latest ghi phân vị thời điểm Kafka; độ trễ tính từ lúc message vào Kafka tới "
                                     "lúc MERGE vào Iceberg xong."}}
    return core.cached(("ops_metrics", lane, minutes), 15, run)


def dag_history(dag: str) -> dict:
    runs = core.airflow_get(f"/api/v2/dags/{dag}/dagRuns?order_by=-run_after&limit=20")["dag_runs"]
    out = []
    for x in runs:
        s, e = core.parse_ts(x.get("start_date")), core.parse_ts(x.get("end_date"))
        out.append({"run_id": x["dag_run_id"], "state": x["state"], "start": x.get("start_date"), "end": x.get("end_date"),
                    "duration_s": round((e - s).total_seconds(), 1) if s and e else None})
    tasks = []
    if runs:
        rid = urllib.parse.quote(runs[0]["dag_run_id"], safe="")
        for t in core.airflow_get(f"/api/v2/dags/{dag}/dagRuns/{rid}/taskInstances?limit=50")["task_instances"]:
            tasks.append({"task_id": t["task_id"], "state": t["state"], "start": t.get("start_date"), "end": t.get("end_date"),
                          "duration_s": t.get("duration"), "try_number": t.get("try_number")})
    d = core.airflow_get(f"/api/v2/dags/{dag}")
    return {"dag_id": dag, "is_paused": d.get("is_paused"), "schedule": d.get("timetable_summary"), "runs": out, "tasks": tasks}


# ---------------------------------------------------------------- 4. sơ đồ pipeline
def snapshot(table: str | None):
    if not table:
        return None
    schema, name = table.split(".")[1:]
    rows = core.safe(lambda: core.q(f"""SELECT to_iso8601(committed_at) AS committed_at, operation,
                                               element_at(summary, 'total-records') AS total_records,
                                               element_at(summary, 'added-records') AS added_records
                                        FROM lakehouse.{schema}."{name}$snapshots" ORDER BY committed_at DESC LIMIT 1""", ttl=30))
    return rows[0] if isinstance(rows, list) and rows else None


def health(ok: bool | None, stale: bool = False) -> str:
    return "unknown" if ok is None else ("degraded" if ok and stale else ("ok" if ok else "error"))


@router.get("/flow")
def flow(lane: str = "main"):
    core.require_lane(lane)
    r = lane_res(lane)

    def run():
        now = dt.datetime.now(dt.timezone.utc)
        m = metrics(lane, 30)
        st = m["streaming"]
        nodes = {}
        k = m["kafka"] if isinstance(m["kafka"], dict) and "rate" in m["kafka"] else None
        rate = k["rate"][-1][1] if k and k["rate"] else None
        total = k["total"][-1][1] if k and k["total"] else None
        nodes["kafka"] = {"label": "Kafka", "sub": f"topic {r['topic']}", "status": health(total is not None),
                          "metrics": {"messages_per_s": rate, "offset_total": total},
                          "last_update": k["total"][-1][0] if k and k["total"] else None,
                          "detail": {"series": "messages_per_s", "error": m["kafka"].get("error") if isinstance(m["kafka"], dict) else None}}
        sp = st.get("speed_5min") or []
        sl = st.get("speed_latest") or []
        last = (sp or sl)[-1] if (sp or sl) else None
        stale = bool(last and core.parse_ts(last["ts"]) and (now - core.parse_ts(last["ts"])) > dt.timedelta(minutes=5))
        sup = core.tail_lines(Path(r["log_dir"]) / "speed_supervisor.log", 50)
        restarts = sum(1 for l in sup if "exit rc=" in l)
        nodes["speed"] = {"label": "Spark Speed Layer", "sub": "Structured Streaming, 3 query",
                          "status": health(True if last is not None else None, stale),
                          "metrics": {"input_rows_per_s": last.get("input_rows_per_sec") if last else None,
                                      "micro_batch_ms": last.get("trigger_execution_ms") if last else None,
                                      "offsets_behind": int(last["max_offsets_behind"]) if last and last.get("max_offsets_behind") not in (None, "") else None,
                                      "supervisor_restarts": restarts},
                          "last_update": last["ts"] if last else None,
                          "detail": {"supervisor": [l.strip() for l in sup[-6:]]}}
        snaps = core.parallel(speed=lambda: snapshot(r["speed5"]), bronze=lambda: snapshot(r["bronze"]),
                              silver=lambda: snapshot(r["silver"]), gold=lambda: snapshot(r["gold5"]),
                              forecast=lambda: snapshot(r["forecast"]))
        nodes["iceberg_speed"] = {"label": "Iceberg Speed", "sub": r["speed5"], "status": health(True if snaps["speed"] is not None else None),
                                  "metrics": {"records": snaps["speed"]["total_records"] if snaps["speed"] else None,
                                              "added_last_commit": snaps["speed"]["added_records"] if snaps["speed"] else None},
                                  "last_update": snaps["speed"]["committed_at"] if snaps["speed"] else None, "detail": snaps["speed"]}
        tt = [x for x in m["trino"] if x.get("ok")]
        terr = sum(1 for x in m["trino"] if not x.get("ok"))
        p50 = sorted(x["ms"] for x in tt)[len(tt) // 2] if tt else None
        nodes["trino"] = {"label": "Trino Serving", "sub": "serving.v_*", "status": health(bool(tt) if m["trino"] else None),
                          "metrics": {"query_ms_p50": p50, "errors": terr, "queries": len(m["trino"])},
                          "last_update": m["trino"][-1]["at"] if m["trino"] else None,
                          "detail": {"recent": m["trino"][-8:]}}
        inf = m["inference"]
        li = inf[-1] if inf else None
        nodes["inference"] = {"label": "Suy luận ML", "sub": "DAG trafficsp_ml_inference",
                              "status": health(None if not li else li.get("status") != "FAILED"),
                              "metrics": {"last_status": li.get("status") if li else None, "seconds": li.get("seconds") if li else None,
                                          "failures_recent": sum(1 for x in inf[-12:] if x.get("status") == "FAILED")},
                              "last_update": li.get("finished_at") if li else None, "detail": {"recent": inf[-6:]}}
        nodes["dashboard"] = {"label": "Dashboard", "sub": "API + cache làm ấm", "status": health(core._warm.get("error") is None),
                              "metrics": {"warm_seconds": core._warm.get("seconds")}, "last_update": core._warm.get("at"),
                              "detail": core._warm}
        dag = m["dag"] if isinstance(m["dag"], dict) and "runs" in m["dag"] else None
        lr = dag["runs"][0] if dag and dag["runs"] else None
        nodes["bronze"] = {"label": "Bronze", "sub": r["bronze"] or "không có làn batch", "status": health(True if snaps["bronze"] is not None else None),
                           "metrics": {"records": snaps["bronze"]["total_records"] if snaps["bronze"] else None,
                                       "added_last_commit": snaps["bronze"]["added_records"] if snaps["bronze"] else None},
                           "last_update": snaps["bronze"]["committed_at"] if snaps["bronze"] else None, "detail": snaps["bronze"]}
        nodes["spark_batch"] = {"label": "Spark Batch (Airflow)", "sub": r["dag"] or "không có làn batch",
                                "status": health(None if not lr else lr["state"] in ("success", "running", "queued")),
                                "metrics": {"last_state": lr["state"] if lr else None, "duration_s": lr["duration_s"] if lr else None,
                                            "paused": dag["is_paused"] if dag else None},
                                "last_update": (lr["end"] or lr["start"]) if lr else None, "detail": dag}
        nodes["silver"] = {"label": "Silver", "sub": r["silver"] or "không có làn batch", "status": health(True if snaps["silver"] is not None else None),
                           "metrics": {"records": snaps["silver"]["total_records"] if snaps["silver"] else None},
                           "last_update": snaps["silver"]["committed_at"] if snaps["silver"] else None, "detail": snaps["silver"]}
        nodes["gold"] = {"label": "Gold Batch View", "sub": r["gold5"] or "không có làn batch", "status": health(True if snaps["gold"] is not None else None),
                         "metrics": {"records": snaps["gold"]["total_records"] if snaps["gold"] else None},
                         "last_update": snaps["gold"]["committed_at"] if snaps["gold"] else None, "detail": snaps["gold"]}
        hs = group_handoffs(control_rows(r)) if r["control"] else []
        h0 = hs[0] if hs else None
        nodes["handoff"] = {"label": "Lambda handoff", "sub": r["control"] or "không có làn batch",
                            "status": health(None if not h0 else h0["status"] != "FAILED"),
                            "metrics": {"last_status": h0["status"] if h0 else None, "batch_state": h0["batch_state"] if h0 else None,
                                        "duration_s": h0["duration_s"] if h0 else None},
                            "last_update": h0["finished_at"] if h0 else None, "detail": {"recent": hs[:5]}}
        edges = [["kafka", "speed"], ["speed", "iceberg_speed"], ["iceberg_speed", "trino"], ["trino", "inference"],
                 ["inference", "dashboard"], ["trino", "dashboard"], ["kafka", "bronze"], ["bronze", "spark_batch"],
                 ["spark_batch", "silver"], ["silver", "gold"], ["gold", "handoff"], ["handoff", "trino"]]
        return {"lane": lane, "nodes": nodes, "edges": edges, "generated_at": iso(now)}
    return core.cached(("ops_flow", lane), 15, run)


# ---------------------------------------------------------------- Grafana (nhúng vào trang Vận hành)
GRAFANA_URL = core.os.getenv("GRAFANA_URL", "http://grafana:3000")                    # backend gọi trong mạng Docker
GRAFANA_PUBLIC_URL = core.os.getenv("GRAFANA_PUBLIC_URL", "http://localhost:3001")    # trình duyệt mở (iframe)
PROMETHEUS_PUBLIC_URL = core.os.getenv("PROMETHEUS_PUBLIC_URL", "http://localhost:9090")


@router.get("/grafana")
def grafana(lane: str = "main"):
    """Danh sách dashboard Grafana để nhúng. Grafana phải bật xem ẩn danh (Viewer) và allow_embedding (docker-compose.yml)."""
    core.require_lane(lane)
    topic = lane_res(lane)["topic"]
    base = {"public_url": GRAFANA_PUBLIC_URL, "prometheus_url": PROMETHEUS_PUBLIC_URL, "topic": topic}
    try:
        found = core.http_json(f"{GRAFANA_URL}/api/search?type=dash-db", timeout=4)
    except Exception as e:  # noqa: BLE001 - lý do trả nguyên cho giao diện
        reason = "Grafana chưa bật xem ẩn danh" if "401" in str(e) else f"không kết nối được Grafana ({e})"
        return {**base, "available": False, "reason": reason, "dashboards": []}
    scope = {"traffic-kafka": f"lọc theo topic của làn: {topic}", "traffic-spark": "toàn cụm Spark, không tách theo làn",
             "traffic-system": "CPU, RAM của máy chạy Docker và từng container"}
    dashboards = [{"uid": d["uid"], "title": d["title"], "path": d["url"],
                   "vars": {"topic": topic} if d["uid"] == "traffic-kafka" else {},
                   "scope": scope.get(d["uid"], "")} for d in found]
    order = ["traffic-system", "traffic-kafka", "traffic-spark"]
    dashboards.sort(key=lambda d: (order.index(d["uid"]) if d["uid"] in order else len(order), d["title"]))
    return {**base, "available": bool(dashboards), "reason": None if dashboards else "Grafana chưa có dashboard nào",
            "dashboards": dashboards}
