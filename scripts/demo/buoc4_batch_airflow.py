"""BƯỚC 4 — Làn chậm (Batch Layer): bấm chạy DAG Airflow và theo dõi từng task.
Code chính: airflow/dags/trafficsp_lambda_batch.py, gọi lần lượt:
  jobs/batch/ingestion/kafka_to_bronze.py -> gold/plan_batch.py -> transformation/bronze_to_silver.py
  -> gold/batch_views.py (5 phút + 1 giờ) -> gold/dq_checks.py -> gold/lambda_handoff.py -> gold/verify_serving.py
Giao diện Airflow: http://localhost:8080 (xem song song cũng được).
"""
import json
import time

from _demo import run, say, title

AF = ["docker", "exec", "-u", "airflow", "traffic-airflow-scheduler", "airflow"]
DAG = "trafficsp_lambda_batch"
MEANING = {
    "kafka_to_bronze": "Lấy message mới từ Kafka, lưu NGUYÊN DẠNG vào bronze.traffic_raw / weather_raw",
    "plan": "Tính khoảng ngày cần xử lý (từ mốc T_batch hiện tại tới dữ liệu mới nhất)",
    "bronze_to_silver": "Làm sạch: khử trùng, giờ New York -> UTC, tách giờ DST lỗi sang quarantine",
    "batch_5min": "Tính lại CHÍNH XÁC bảng 5 phút -> gold.traffic_5min_batch",
    "batch_hourly": "Cộng lên bảng 1 giờ -> gold.traffic_hourly",
    "dq_checks": "Kiểm tra chất lượng: đủ dòng, không trùng, số liệu hợp lý (sai -> dừng)",
    "lambda_handoff": "Bàn giao: PREPARING -> VALIDATED -> PUBLISHED, dời mốc T_batch lên",
    "verify_serving": "Kiểm tra lại view trên Trino: không trùng, không sai nguồn",
}
ORDER = list(MEANING)


def af_json(args):
    out = run(AF + args + ["-o", "json"], show=False)
    # Airflow in kèm vài dòng cảnh báo (cũng chứa "["); JSON nằm ở dòng bắt đầu bằng "[{" hoặc "{"
    for line in out.splitlines():
        if line.startswith("[{") or line.startswith("{") or line == "[]":
            return json.loads(line)
    return None


title("BƯỚC 4: LÀN CHẬM — Airflow DAG trafficsp_lambda_batch")
active = [r for r in (af_json(["dags", "list-runs", DAG]) or []) if r.get("state") in ("running", "queued")]
if active:                       # DAG chỉ cho 1 lượt chạy cùng lúc (max_active_runs=1) -> theo dõi luôn lượt đang chạy
    run_id = active[0]["run_id"]
    say(f"Đang có một lượt chạy (theo lịch mỗi giờ hoặc bấm tay trước đó) -> theo dõi luôn lượt này")
else:
    say("Bấm chạy DAG (giống nút ▶ Trigger trên giao diện Airflow)")
    res = af_json(["dags", "trigger", DAG])
    r0 = res[0] if isinstance(res, list) else res
    run_id = r0.get("dag_run_id") or r0.get("run_id")
print(f"   run_id = {run_id}\n   (mở http://localhost:8080 -> Dags -> {DAG} để xem cùng lúc)\n", flush=True)

seen, t0 = {}, time.time()
while True:
    states = af_json(["tasks", "states-for-dag-run", DAG, run_id]) or []
    st = {s["task_id"]: (s.get("state") or "chưa chạy") for s in states}
    for task in ORDER:
        s = st.get(task, "chưa chạy")
        if s in ("up_for_retry", "queued", "scheduled") and seen.get(task) != s:
            print(f"  [{int(time.time() - t0):>4}s] …  {task:<17} {s}", flush=True)
        if seen.get(task) != s and s in ("running", "success", "failed", "upstream_failed", "skipped"):
            mark = {"running": "⏳", "success": "✅", "failed": "❌", "upstream_failed": "⛔", "skipped": "⏭"}[s]
            print(f"  [{int(time.time() - t0):>4}s] {mark} {task:<17} {s:<8} — {MEANING[task]}", flush=True)
        seen[task] = s
    done = all(seen.get(t) in ("success", "failed", "upstream_failed", "skipped") for t in ORDER)
    if done or time.time() - t0 > 1800:
        break
    time.sleep(8)

ok = all(seen.get(t) == "success" for t in ORDER)
say("DAG chạy xong: TẤT CẢ THÀNH CÔNG" if ok else "DAG kết thúc nhưng có task không thành công (xem log trên Airflow)")
print(f"Tổng thời gian: {int(time.time() - t0)} giây. Sang bước 5 để xem mốc T_batch đã dời thế nào.")
