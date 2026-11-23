"""Một lượt suy luận tự động (Airflow DAG trafficsp_ml_inference), điều khiển theo GIỜ SỰ KIỆN chứ không theo đồng hồ máy.

  spark-submit --master "local[2]" jobs/ml/inference_task.py --lane main

Quy trình:
  1. Chọn model theo models/ml/current.json, kiểm tra sha256 trong manifest (model không bị thay âm thầm).
  2. Đọc cửa sổ 5' hoàn tất mới nhất L trong view hợp nhất của làn (Trino). Gốc mới nhất có thể dự báo: T = L + 5' + A.
  3. Gốc đã dự báo gần nhất (bảng forecast, cùng model_version) -> các gốc còn thiếu (bước 5'), tối đa max_origins_per_run.
     Không có gốc mới -> SKIPPED (thoát 99). Dữ liệu phát lại đi nhanh hơn lịch Airflow -> lượt sau bù đủ, không bỏ sót gốc.
  4. Ít hơn min_valid_links đoạn có cửa sổ hợp lệ trong 60' trước L -> SKIPPED, không ghi dự báo rỗng.
  5. Bảng free-flow thiếu mốc as_of của model -> nạp từ models/ml/<version>/baseline_asof.parquet (đi kèm model).
  6. predict.run(...) ghi bằng MERGE theo (link_id, prediction_origin_utc, model_version): retry không sinh bản trùng.
Mỗi lượt (kể cả SKIPPED/FAILED) ghi một dòng JSON vào inference.log_file để dashboard và người vận hành theo dõi.
Mã thoát: 0 thành công, 99 bỏ qua (Airflow đánh dấu skipped), 1 lỗi.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
import time
import traceback
import uuid
from pathlib import Path

from common.trino_http import query
from ml import predict
from ml.mlconfig import load_config

SKIP = 99


def trino_one(sql: str):
    rows = query(sql, user="ml_inference")
    return rows[0][0] if rows and rows[0] else None


def ts(v) -> dt.datetime | None:
    return dt.datetime.strptime(str(v)[:19], "%Y-%m-%d %H:%M:%S") if v else None


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_model(models_dir: Path) -> tuple[Path, dict, dict]:
    version = json.loads((models_dir / "current.json").read_text())["model_version"]
    d = models_dir / version
    manifest = json.loads((d / "manifest.json").read_text())
    for name in ("model.json", "metadata.json"):
        got = sha256(d / name)
        if got != manifest["sha256"][name]:
            raise RuntimeError(f"{d / name}: sha256 {got[:12]}… khác manifest {manifest['sha256'][name][:12]}…")
    meta = json.loads((d / "metadata.json").read_text())
    if meta["model_version"] != version:
        raise RuntimeError(f"metadata.json là {meta['model_version']}, current.json chọn {version}")
    return d, meta, manifest


def select_origins(last_done: dt.datetime | None, latest_origin: dt.datetime, cut: dt.datetime | None,
                   availability_lag_min: int, cap: int) -> tuple[list[dt.datetime], int]:
    """Các gốc dự báo cần chạy lượt này (bước 5') và số gốc tồn đọng bị bỏ qua.
    - Đã có gốc trước: mọi gốc sau last_done tới latest_origin.
    - Làn mới có mốc cắt (phiên demo): từ gốc đầu tiên dùng được cửa sổ tại mốc cắt (cut + 5' + A).
    - Làn mới không có mốc cắt (hệ thống chính lần đầu): chỉ gốc mới nhất.
    Bù gốc CŨ NHẤT trước (để có số thực tế đối chiếu); tồn đọng > 3 lượt thì nhảy tới cap gốc mới nhất."""
    step = dt.timedelta(minutes=5)
    if last_done is not None:
        first = last_done + step
    elif cut is not None:
        first = cut + dt.timedelta(minutes=5 + availability_lag_min)
    else:
        first = latest_origin
    missing = []
    t = first
    while t <= latest_origin:
        missing.append(t)
        t += step
    if len(missing) > 3 * cap:
        return missing[-cap:], len(missing) - cap
    return missing[:cap], 0


def lane_cut(cfg: dict, lane: str) -> dt.datetime | None:
    """Mốc bắt đầu của phiên phát lại kiểm chứng (demo_lane.json); None với hệ thống chính."""
    if lane != "demo":
        return None
    f = Path(cfg.get("inference", {}).get("demo_lane_file", "/opt/lakehouse/logs/demo_lane.json"))
    if not f.exists():
        return None
    c = json.loads(f.read_text()).get("cut_utc")
    return dt.datetime.fromisoformat(c.replace("Z", "")) if c else None


def table_exists(fq: str) -> bool:
    _, schema, name = fq.split(".")
    return bool(trino_one(f"SELECT count(*) FROM lakehouse.information_schema.tables "
                          f"WHERE table_schema = '{schema}' AND table_name = '{name}'"))


def ensure_baseline(spark, cfg: dict, model_dir: Path, manifest: dict) -> str:
    table = cfg["data"]["baseline_table"]
    as_of = manifest["baseline_as_of_utc"][:19]
    if table_exists(table) and trino_one(f"SELECT count(*) FROM {table} WHERE as_of_utc = TIMESTAMP '{as_of} UTC'"):
        return "có sẵn"
    src = model_dir / manifest["baseline_file"]
    df = spark.read.parquet(f"file://{src}").filter(f"as_of_utc = TIMESTAMP '{as_of}'")
    df = df.selectExpr("CAST(link_id AS BIGINT) link_id", "free_flow_speed", "CAST(n_samples AS BIGINT) n_samples",
                       "as_of_utc", "history_start_utc", "computed_at")
    if df.count() == 0:
        raise RuntimeError(f"{src} không có free-flow as_of {as_of}")
    if spark.catalog.tableExists(table):
        df.writeTo(table).append()
    else:
        df.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
    return f"đã nạp {df.count()} đoạn từ {src.name}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lane", default="main")
    ap.add_argument("--max-origins", type=int)
    ap.add_argument("--dry-run", action="store_true", help="chỉ quyết định gốc dự báo, không chạy Spark/ghi")
    args = ap.parse_args()

    cfg = load_config()
    inf = cfg.get("inference", {})
    log_file = Path(inf.get("log_file", "/opt/lakehouse/logs/inference_runs.jsonl"))
    rec = {"run_id": f"inf-{dt.datetime.utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}", "lane": args.lane,
           "started_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "status": None, "reason": None}
    t0 = time.time()

    def finish(status: str, reason: str | None = None, code: int = 0, **extra) -> int:
        rec.update(status=status, reason=reason, seconds=round(time.time() - t0, 1),
                   finished_at=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), **extra)
        line = json.dumps(rec, ensure_ascii=False, default=str)
        print(line, flush=True)
        if not args.dry_run:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            with log_file.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        return code

    try:
        forecast_table = predict.set_lane(cfg, args.lane)
        model_dir, meta, manifest = resolve_model(Path(inf.get("models_dir", "/opt/airflow/models/ml")))
        rec["model_version"] = meta["model_version"]
        A = int(meta["availability_lag_minutes"])
        view = predict.UNIFIED_VIEW
        if not table_exists(view):
            return finish("SKIPPED", f"chưa có view {view} (làn {args.lane} chưa được tạo)", SKIP)

        L = ts(trino_one(f"SELECT CAST(max(window_start_utc) AS varchar) FROM {view} WHERE n_valid > 0"))
        if L is None:
            return finish("SKIPPED", f"{view} chưa có cửa sổ hợp lệ nào", SKIP)
        latest_origin = L + dt.timedelta(minutes=5 + A)
        last_done = None
        if table_exists(forecast_table):
            last_done = ts(trino_one(f"SELECT CAST(max(prediction_origin_utc) AS varchar) FROM {forecast_table} "
                                     f"WHERE model_version = '{meta['model_version']}'"))
        cap = args.max_origins or int(inf.get("max_origins_per_run", 12))
        origins, backlog = select_origins(last_done, latest_origin, lane_cut(cfg, args.lane), A, cap)
        if backlog:
            rec["skipped_backlog_origins"] = backlog
        rec.update(latest_complete_window_utc=L.isoformat(), last_origin_done_utc=last_done.isoformat() if last_done else None)
        if not origins:
            why = (f"đã dự báo tới gốc {last_done:%Y-%m-%d %H:%M} UTC; chưa có cửa sổ hoàn tất mới sau {L:%H:%M}" if last_done
                   else f"chưa có cửa sổ hoàn tất nào sau mốc bắt đầu phiên (cửa sổ mới nhất {L:%Y-%m-%d %H:%M} UTC)")
            return finish("SKIPPED", why, SKIP)

        # dữ liệu hợp lệ đủ dùng: số đoạn có ít nhất một cửa sổ hợp lệ trong 60' trước cửa sổ cuối của gốc đầu tiên
        L0 = origins[0] - dt.timedelta(minutes=5 + A)
        n_valid = int(trino_one(f"""SELECT count(DISTINCT link_id) FROM {view} WHERE n_valid > 0
            AND window_start_utc > TIMESTAMP '{L0 - dt.timedelta(minutes=60):%Y-%m-%d %H:%M:%S} UTC'
            AND window_start_utc <= TIMESTAMP '{L:%Y-%m-%d %H:%M:%S} UTC'""") or 0)
        rec["links_with_valid_data"] = n_valid
        if n_valid < int(inf.get("min_valid_links", 10)):
            return finish("SKIPPED", f"chỉ {n_valid} đoạn có dữ liệu hợp lệ trong 60 phút trước {L0:%H:%M} UTC", SKIP)
        if args.dry_run:
            return finish("DRY_RUN", None, 0, origins=[o.isoformat() for o in origins])

        from common.spark_session import build_spark
        spark = build_spark("ml_inference", {"spark.sql.shuffle.partitions": "8", "spark.ui.enabled": "false"})
        rec["baseline"] = ensure_baseline(spark, cfg, model_dir, manifest)
        out = predict.run(spark, cfg, str(model_dir), None, "serving", write=True, origins=origins, forecast_table=forecast_table)
        spark.stop()
        got = sorted(out["prediction_origin_utc"].astype(str).unique())
        return finish("SUCCESS", None, 0, forecast_table=forecast_table, n_origins=len(got),
                      origin_first_utc=got[0], origin_last_utc=got[-1], n_rows=int(len(out)),
                      n_predicted=int(out["predicted_speed"].notna().sum()))
    except SystemExit as e:          # predict.py báo thiếu dữ liệu/baseline bằng SystemExit(msg)
        return finish("FAILED", str(e), 1)
    except Exception as e:  # noqa: BLE001
        traceback.print_exc()
        return finish("FAILED", f"{type(e).__name__}: {e}", 1)


if __name__ == "__main__":
    sys.exit(main())
