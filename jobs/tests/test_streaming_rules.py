# Unit test (không cần Kafka; quality_rules cần pyspark) cho quy tắc dùng chung Batch/Stream và lịch phát GĐ2.
# Chạy: cd jobs && python -m pytest tests/test_streaming_rules.py -q
import datetime as dt
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs"))
sys.path.insert(0, str(ROOT / "scripts"))
if "confluent_kafka" not in sys.modules:                       # producer import ở mức module; test không gửi Kafka
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:
        sys.modules["confluent_kafka"] = types.SimpleNamespace(Producer=object)
for _m in ("pyarrow", "pyarrow.compute", "pyarrow.parquet"):  # replay_source đọc file bằng pyarrow; test thay nguồn đọc
    try:
        __import__(_m)
    except ImportError:
        sys.modules[_m] = types.ModuleType(_m)

from common import quality_rules as qr          # noqa: E402
from common import time_rules as tr             # noqa: E402


def test_dst_hours_2025_from_tzdata():
    h = tr.dst_local_hours(2025)
    assert h["ambiguous"] == [("2025-11-02", 1)]
    assert h["nonexistent"] == [("2025-03-09", 2)]


@pytest.mark.parametrize("local,expected", [
    ("2025-01-15T08:00:00", "2025-01-15T13:00:00+00:00"),   # EST
    ("2025-07-15T08:00:00", "2025-07-15T12:00:00+00:00"),   # EDT
    ("2025-03-09T01:58:00", "2025-03-09T06:58:00+00:00"),   # ngay trước khi tiến giờ
    ("2025-03-09T03:03:00", "2025-03-09T07:03:00+00:00"),   # ngay sau khi tiến giờ
    ("2025-11-02T00:58:00", "2025-11-02T04:58:00+00:00"),   # trước giờ lặp (EDT)
    ("2025-11-02T02:03:00", "2025-11-02T07:03:00+00:00"),   # sau giờ lặp (EST)
    ("2025-12-31T23:58:00", "2026-01-01T04:58:00+00:00"),
])
def test_local_to_utc(local, expected):
    assert tr.local_to_utc(tr.parse_local(local)).isoformat() == expected


@pytest.mark.parametrize("local", ["2025-11-02T01:03:04", "2025-11-02T01:58:59", "2025-03-09T02:30:00"])
def test_ambiguous_or_nonexistent_has_no_utc(local):
    assert tr.local_to_utc(tr.parse_local(local)) is None
    assert tr.classify_local(tr.parse_local(local)) in ("dst_ambiguous", "dst_nonexistent")


@pytest.mark.parametrize("speed,tt,reason", [
    (45.0, 60, None), (100.0, 30, None), (100.1, 30, "speed_gt_max"), (0.0, 0, "speed_zero"),
    (0.0, 12, "speed_zero_tt_positive"), (20.0, 0, "travel_time_nonpositive"), (20.0, -5, "travel_time_nonpositive"),
    (-1.0, 10, "speed_negative"), (None, 10, "null_value"),
])
def test_invalid_reason(speed, tt, reason):
    assert qr.invalid_reason(speed, tt) == reason


@pytest.mark.parametrize("speed,ff,n,state", [
    (20.0, 50.0, 1, qr.STATE_CONGESTED), (25.0, 50.0, 1, qr.STATE_SLOW), (39.9, 50.0, 1, qr.STATE_SLOW),
    (40.0, 50.0, 1, qr.STATE_FREE_FLOW), (40.0, None, 1, qr.STATE_NO_BASELINE), (None, 50.0, 0, qr.STATE_NO_VALID_DATA),
])
def test_traffic_state(speed, ff, n, state):
    assert qr.traffic_state(speed, ff, n) == state


# ------------------------------------------------------------ producer GĐ2 (scripts/realtime_replay.py, API hiện tại)
# Ba test dưới đây thay cho bản cũ viết theo API producer trước khi tách module (traffic_pacing_utc, weather_pacing_utc,
# realtime_schedule, Injector). Hành vi được kiểm tra giữ nguyên ý nghĩa: thứ tự phát qua DST, mốc phát thời tiết,
# lịch phát đúng thứ tự trong cửa sổ và không sửa payload. Producer mới không còn tiêm lỗi (ooo/dup/skew) nên phần đó bỏ.
import realtime_replay as rr   # noqa: E402

UTC = dt.timezone.utc


def test_traffic_release_preserves_dst_order():
    # 00:58 EDT -> 04:58Z ; 01:03 (giờ lặp, lấy lần đầu fold=0, EDT) -> 05:03Z ; 02:03 EST -> 07:03Z
    t = [rr.traffic_release(s) for s in ("2025-11-02T00:58:00", "2025-11-02T01:03:00", "2025-11-02T02:03:00")]
    assert t == sorted(t)
    assert [dt.datetime.fromtimestamp(x, UTC).strftime("%H:%M") for x in t] == ["04:58", "05:03", "07:03"]


def test_weather_release_at_end_of_accumulation():
    # bản ghi mốc T mô tả (T-1h, T] nên chỉ quan sát được từ T; delay cộng thêm phút
    assert rr.weather_release("2025-10-01T05:00", 0) == dt.datetime(2025, 10, 1, 5, tzinfo=UTC).timestamp()
    assert rr.weather_release("2025-10-01T05:00", 30) - rr.weather_release("2025-10-01T05:00", 0) == 1800


def test_schedule_sorted_within_window_payload_unchanged(monkeypatch):
    traffic = [{"link_id": 1, "data_as_of": s} for s in
               ("2025-10-01T00:55:00", "2025-10-01T01:00:00", "2025-10-01T01:02:30", "2025-10-01T01:59:00", "2025-10-01T02:01:00")]
    weather = [{"cell_id": "c1", "time": t} for t in ("2025-10-01T04:00", "2025-10-01T05:00", "2025-10-01T06:00")]
    monkeypatch.setattr(rr, "iter_traffic", lambda phase, run_id: iter(traffic))
    monkeypatch.setattr(rr, "read_weather", lambda phase, run_id: weather)
    args = types.SimpleNamespace(start="2025-10-01T05:00", end="2025-10-01T06:00", weather_delay_min=0)
    out = list(rr.schedule(args, "t"))
    rel = [x[0] for x in out]
    assert rel == sorted(rel), "phải phát theo thứ tự release"
    lo, hi = rr.parse_utc(args.start), rr.parse_utc(args.end)
    assert all(lo <= r < hi for r in rel), "chỉ phát trong [start, end)"
    sent = [x[4] for x in out if x[2] == rr.config.TOPIC_TRAFFIC]
    # 01:00..01:59 EDT = 05:00..05:59Z nằm trong cửa sổ; 00:55 và 02:01 EDT nằm ngoài
    assert [m["data_as_of"] for m in sent] == ["2025-10-01T01:00:00", "2025-10-01T01:02:30", "2025-10-01T01:59:00"]
    assert all(m is traffic[i] for m, i in zip(sent, (1, 2, 3))), "payload gốc không bị sửa"
    assert [x[4]["time"] for x in out if x[2] == rr.config.TOPIC_WEATHER] == ["2025-10-01T05:00"]
