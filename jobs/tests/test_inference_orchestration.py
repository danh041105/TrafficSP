# Unit test cho phần điều phối mới: chọn gốc dự báo theo giờ sự kiện, bỏ qua lượt batch không tiến, view làn demo.
# Chạy trong container có pyspark (plan_batch import pyspark): cd /opt/airflow/jobs && python -m pytest tests -q
import datetime as dt
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs"))
sys.path.insert(0, str(ROOT / "scripts"))

from ml.inference_task import select_origins   # noqa: E402

T = dt.datetime(2025, 12, 16, 12, 0)
m = lambda k: dt.timedelta(minutes=k)  # noqa: E731


def test_continue_after_last_origin():
    o, backlog = select_origins(T, T + m(20), None, 10, 12)
    assert o == [T + m(5), T + m(10), T + m(15), T + m(20)] and backlog == 0


def test_nothing_new_returns_empty():
    assert select_origins(T + m(20), T + m(20), None, 10, 12) == ([], 0)
    assert select_origins(T + m(25), T + m(20), None, 10, 12) == ([], 0)   # gốc mới nhất lùi (không bao giờ ghi lùi)


def test_new_demo_lane_starts_at_cut_oldest_first():
    cut = T
    # 6 gốc thiếu (12:15..12:40) <= 3 x cap -> bù từ cũ nhất, mỗi lượt cap gốc
    o, backlog = select_origins(None, T + m(40), cut, 10, 4)
    assert o[0] == cut + m(15), "gốc đầu tiên dùng cửa sổ tại mốc cắt: cut + 5' + A"
    assert o == [cut + m(15), cut + m(20), cut + m(25), cut + m(30)] and backlog == 0


def test_new_main_lane_only_latest():
    assert select_origins(None, T, None, 10, 12) == ([T], 0)


def test_large_backlog_jumps_to_latest():
    o, backlog = select_origins(T, T + m(5 * 100), None, 10, 12)
    assert len(o) == 12 and o[-1] == T + m(500) and backlog == 100 - 12


def test_batch_can_advance():
    from batch.gold.plan_batch import batch_can_advance
    from common import lambda_control as lc
    t_old = dt.datetime(2026, 1, 1, 4, 45)
    assert not batch_can_advance(t_old, t_old)                       # dữ liệu hết: mốc an toàn = batch_state
    assert not batch_can_advance(t_old, t_old - m(5))
    assert batch_can_advance(t_old, t_old + m(5))                    # có dữ liệu mới đủ trễ
    assert batch_can_advance(lc.EPOCH, lc.EPOCH)                     # lượt đầu
    assert batch_can_advance(t_old, t_old, forced=True)              # --from-day (backfill)


def test_demo_views_point_to_session_resources_only():
    from serving.demo_views import views_sql
    sql = views_sql({"run_id": "x", "cut_utc": "2025-12-16T12:00:00Z", "speed_namespace": "lakehouse.speed_demox",
                     "gold_namespace": "lakehouse.gold_demox", "forecast_table": "lakehouse.ml.traffic_forecast_15m_demox"})
    assert sql.count("CREATE OR REPLACE VIEW lakehouse.serving.v_demo_") == 6
    assert "lakehouse.speed." not in sql, "không đọc bảng Speed của hệ thống chính"
    assert "v_batch_state " not in sql.replace("v_demo_batch_state", ""), "mốc cắt là của phiên, không phải batch_state chính"
    assert "TIMESTAMP '2025-12-16 12:00:00 UTC' AS timestamp(6) with time zone" in sql
    assert "lakehouse.gold_demox.lambda_control" in sql and "lakehouse.gold_demox.fact_traffic_5min FOR VERSION AS OF 'published'" in sql
    assert "lakehouse.ml.traffic_forecast_15m_demox" in sql and "lakehouse.ml.traffic_forecast_15m f" not in sql
