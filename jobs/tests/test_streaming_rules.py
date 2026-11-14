# Unit test thuần Python (không cần Spark/Kafka) cho quy tắc dùng chung Batch/Stream và lịch phát GĐ2.
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


# ------------------------------------------------------------ producer GĐ2
import replay_producer as rp   # noqa: E402


def _args(**kw):
    a = rp.parse_args(["--phase", "gd2", "--count-only"])
    for k, v in kw.items():
        setattr(a, k, v)
    return a


def test_pacing_preserves_dst_order():
    # 00:58 EDT -> 04:58Z ; 01:03 (lần đầu, fold=0) -> 05:03Z ; 02:03 EST -> 07:03Z
    t = [rp.traffic_pacing_utc(s) for s in ("2025-11-02T00:58:00", "2025-11-02T01:03:00", "2025-11-02T02:03:00")]
    assert t == sorted(t)
    assert dt.datetime.fromtimestamp(t[1], dt.timezone.utc).hour == 5


def test_weather_release_at_end_of_accumulation():
    # bản ghi mốc T phát lúc T (+delay): mưa của (T-1h, T] chỉ biết được từ T
    assert rp.weather_pacing_utc("2025-10-01T05:00", 0) == dt.datetime(2025, 10, 1, 5, tzinfo=dt.timezone.utc).timestamp()
    assert rp.weather_pacing_utc("2025-10-01T05:00", 30) - rp.weather_pacing_utc("2025-10-01T05:00", 0) == 1800


def test_schedule_window_sorted_and_injections_reproducible():
    args = _args(start="2025-11-02T04:30", end="2025-11-02T05:30", ooo_fraction=0.05, dup_fraction=0.02,
                 skew_links="4616340", skew_copies=1, seed=7)
    s1 = list(rp.realtime_schedule(args, "t", rp.Injector(args)))
    s2 = list(rp.realtime_schedule(args, "t", rp.Injector(args)))
    rel = [x[0] for x in s1]
    assert rel == sorted(rel), "phải phát theo thứ tự release"
    assert [(x[0], x[3], x[5]) for x in s1] == [(x[0], x[3], x[5]) for x in s2], "cùng seed -> cùng lịch"
    kinds = {x[5] for x in s1}
    assert {"ooo", "dup", "skew"} <= kinds
    # payload gốc không bị sửa: data_as_of vẫn là chuỗi giờ New York nguyên bản
    traffic = [x for x in s1 if x[2] == "traffic_raw"]
    assert all(len(x[4]["data_as_of"]) == 19 and x[4]["data_as_of"].startswith("2025-11-02T") for x in traffic)
    # bản đến muộn có release > thời điểm gốc
    for r, _, topic, _, msg, kind in s1:
        if kind == "ooo":
            assert r > rp.traffic_pacing_utc(msg["data_as_of"])
