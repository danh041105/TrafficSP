"""View serving.v_demo_* cho phiên phát lại kiểm chứng (có làn batch riêng, handoff thật).

Một phiên <run> có namespace riêng:
- speed_<run>: Speed Layer của phiên;
- bronze_<run>, silver_<run>, gold_<run>: Batch Layer của phiên, qua DAG trafficsp_lambda_batch_demo;
- bảng ml.traffic_forecast_15m_<run>.

View hợp nhất của phiên giữ đúng hợp đồng phục vụ của làn chính, với mốc cắt T_demo của phiên:

    window <  cut                 -> batch CHÍNH đã công bố (lịch sử 8 ngày cho feature ML)
    cut  <= window < T_demo       -> batch CỦA PHIÊN đã công bố (tag published của gold_<run>)
    window >= T_demo              -> speed CỦA PHIÊN

T_demo lấy từ dòng PUBLISHED mới nhất trong gold_<run>.lambda_control. Chưa có handoff nào thì T_demo = cut.
Các view còn lại (enriched, latest, forecast) sinh từ đúng SQL của làn chính (jobs/serving/*.sql),
chỉ thay tên view, namespace Speed và bảng dự báo.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

SERVING = Path(__file__).resolve().parent


def _statements(path: Path) -> list[str]:
    text = "\n".join(l for l in path.read_text(encoding="utf-8").splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


def views_sql(lane: dict) -> str:
    """lane: run_id, cut_utc (ISO), speed_namespace, gold_namespace, forecast_table."""
    cut = dt.datetime.fromisoformat(lane["cut_utc"].replace("Z", ""))
    # view trong catalog Iceberg cần cột timestamp(6): hằng số TIMESTAMP '...' (độ chính xác 0) phải ép kiểu
    cut_lit = f"CAST(TIMESTAMP '{cut:%Y-%m-%d %H:%M:%S} UTC' AS timestamp(6) with time zone)"
    speed, gold, fc = lane["speed_namespace"], lane["gold_namespace"], lane["forecast_table"]
    # batch_state của làn không nhỏ hơn mốc bắt đầu phiên: trước mốc đó đã có Batch đã công bố của hệ thống chính.
    # Handoff đầu tiên có thể công bố mốc < cut khi DAG chạy lúc phiên mới phát vài phút (safe_bound = max event - trễ cho phép).
    out = [f"""CREATE OR REPLACE VIEW lakehouse.serving.v_demo_batch_state SECURITY INVOKER AS
SELECT greatest(coalesce((SELECT batch_state FROM {gold}.lambda_control WHERE status = 'PUBLISHED'
                          ORDER BY created_at DESC LIMIT 1), {cut_lit}), {cut_lit}) AS batch_state, {cut_lit} AS cut_utc""",
           f"""CREATE OR REPLACE VIEW lakehouse.serving.v_demo_traffic_5min_unified SECURITY INVOKER AS
SELECT u.link_id, u.road_link_key, u.window_start_utc, u.window_start_utc + INTERVAL '5' MINUTE AS window_end_utc,
       u.date_key, u.hour_key,
       u.n_records, u.n_valid, u.n_zero, u.n_records - u.n_valid - u.n_zero AS n_invalid_other,
       u.sum_speed_mph, u.sum_speed_sq, u.sum_travel_time_sec,
       if(u.n_valid > 0, u.sum_speed_mph / u.n_valid) AS avg_speed_mph,
       if(u.n_valid = 0, 'NO_VALID_DATA', 'OK') AS quality_flag,
       u.layer
FROM (
    SELECT b.link_id, b.road_link_key, b.window_start_utc, b.date_key, b.hour_key, b.n_records, b.n_valid, b.n_zero,
           b.sum_speed_mph, b.sum_speed_sq, b.sum_travel_time_sec, 'batch' AS layer
    FROM lakehouse.gold.fact_traffic_5min FOR VERSION AS OF 'published' b
    WHERE b.window_start_utc < {cut_lit}
    UNION ALL
    SELECT b.link_id, b.road_link_key, b.window_start_utc, b.date_key, b.hour_key, b.n_records, b.n_valid, b.n_zero,
           b.sum_speed_mph, b.sum_speed_sq, b.sum_travel_time_sec, 'batch' AS layer
    FROM {gold}.fact_traffic_5min FOR VERSION AS OF 'published' b
    WHERE b.window_start_utc >= {cut_lit}
      AND b.window_start_utc < (SELECT batch_state FROM lakehouse.serving.v_demo_batch_state)
    UNION ALL
    SELECT s.link_id, s.road_link_key, s.window_start_utc, s.date_key, s.hour_key, s.n_records, s.n_valid, s.n_zero,
           s.sum_speed_mph, s.sum_speed_sq, s.sum_travel_time_sec, 'speed' AS layer
    FROM {speed}.traffic_5min_rt s
    WHERE s.window_start_utc >= (SELECT batch_state FROM lakehouse.serving.v_demo_batch_state)) u"""]
    want = ("v_traffic_5min_enriched", "v_traffic_latest", "v_traffic_forecast_15m ", "v_traffic_forecast_15m_latest")
    for st in _statements(SERVING / "trino_views.sql") + _statements(SERVING / "ml_views.sql"):
        head = st.split(" AS", 1)[0] + " "
        if not any(f"lakehouse.serving.{w}" in head for w in want):
            continue
        st = st.replace("lakehouse.speed.", f"{speed}.").replace("lakehouse.ml.traffic_forecast_15m", fc)
        for v in ("v_traffic_5min_unified", "v_traffic_5min_enriched", "v_traffic_latest", "v_traffic_forecast_15m"):
            st = st.replace(f"lakehouse.serving.{v}", f"lakehouse.serving.v_demo_{v[2:]}")
        out.append(st)
    if len(out) != 6:
        raise RuntimeError(f"chỉ sinh được {len(out)}/6 view demo; kiểm tra trino_views.sql / ml_views.sql")
    return ";\n".join(out) + ";\n"
