# Unit test thuần Python cho giao thức Lambda handoff (common/lambda_control.py) - không cần Spark.
# Chạy: python -m pytest jobs/tests/test_lambda_control.py -q
import datetime as dt
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jobs"))

from common import lambda_control as lc   # noqa: E402

T = dt.datetime


def row(hid, status, t_batch, minute, s5=None, sh=None):
    return {"handoff_id": hid, "status": status, "t_batch": t_batch, "created_at": T(2026, 11, 13, 8, minute),
            "snapshot_5min": s5, "snapshot_hourly": sh}


def test_floor5():
    assert lc.floor5(T(2025, 12, 2, 8, 7, 59, 123)) == T(2025, 12, 2, 8, 5)
    assert lc.floor5(T(2025, 12, 2, 8, 0)) == T(2025, 12, 2, 8, 0)


def test_current_t_batch_only_published_and_max():
    rows = [row("a", "PUBLISHED", T(2025, 12, 2, 8), 1), row("b", "VALIDATED", T(2025, 12, 2, 9), 2),
            row("c", "PUBLISHED", T(2025, 12, 2, 7), 3), row("d", "FAILED", T(2025, 12, 2, 10), 4)]
    assert lc.current_t_batch(rows) == T(2025, 12, 2, 8)
    assert lc.current_t_batch([]) == lc.EPOCH


def test_current_t_batch_accepts_aware_timestamps():
    aware = T(2025, 12, 2, 8, tzinfo=dt.timezone.utc)
    assert lc.current_t_batch([row("a", "PUBLISHED", aware, 1)]) == T(2025, 12, 2, 8)


def test_pending_handoffs_uses_last_status():
    rows = [row("a", "PREPARING", T(2025, 12, 2, 8), 1), row("a", "VALIDATED", T(2025, 12, 2, 8), 2),
            row("a", "PUBLISHED", T(2025, 12, 2, 8), 3),
            row("b", "PREPARING", T(2025, 12, 2, 9), 4),
            row("c", "PREPARING", T(2025, 12, 2, 9), 5), row("c", "VALIDATED", T(2025, 12, 2, 9), 6, 11, 22)]
    pend = {p["handoff_id"]: p["status"] for p in lc.pending_handoffs(rows)}
    assert pend == {"b": "PREPARING", "c": "VALIDATED"}


@pytest.mark.parametrize("status,tags,t_pub,expected", [
    ("PREPARING", (11, 22), T(2025, 12, 2, 8), "FAILED"),                 # chưa kiểm chứng -> bỏ
    ("VALIDATED", (11, 22), T(2025, 12, 2, 8), "PUBLISHED"),              # tag đã dời đúng -> hoàn tất
    ("VALIDATED", (99, 22), T(2025, 12, 2, 8), "FAILED"),                 # tag 5 phút chưa dời
    ("VALIDATED", (11, 99), T(2025, 12, 2, 8), "FAILED"),                 # tag 1 giờ chưa dời
    ("VALIDATED", (11, 22), T(2025, 12, 2, 9), "FAILED"),                 # đã có T_batch >= -> không lùi / không lặp
])
def test_recovery_action(status, tags, t_pub, expected):
    p = row("x", status, T(2025, 12, 2, 9), 1, 11, 22)
    assert lc.recovery_action(p, tags[0], tags[1], t_pub) == expected


def test_choose_t_new_auto_and_explicit():
    t, safe = lc.choose_t_new("auto", T(2025, 12, 2, 10, 58, 11), 10)
    assert t == safe == T(2025, 12, 2, 10, 45)
    t, safe = lc.choose_t_new("2025-12-02T08:03:00", T(2025, 12, 2, 10, 58), 10)
    assert t == T(2025, 12, 2, 8, 0) and safe == T(2025, 12, 2, 10, 45)
    with pytest.raises(ValueError):
        lc.choose_t_new("auto", None, 10)


@pytest.mark.parametrize("t_old,t_new,correction,ok", [
    (T(2025, 12, 2, 8), T(2025, 12, 2, 9), False, True),
    (T(2025, 12, 2, 9), T(2025, 12, 2, 8), False, False),    # giảm
    (T(2025, 12, 2, 9), T(2025, 12, 2, 8), True, False),     # giảm, kể cả correction
    (T(2025, 12, 2, 9), T(2025, 12, 2, 9), False, False),    # không đổi
    (T(2025, 12, 2, 9), T(2025, 12, 2, 9), True, True),      # correction: công bố lại cùng mốc
    (T(2025, 12, 2, 9), T(2025, 12, 2, 11), False, False),   # vượt mốc an toàn
])
def test_validate_move(t_old, t_new, correction, ok):
    reason = lc.validate_move(t_old, t_new, T(2025, 12, 2, 10, 45), correction)
    assert (reason is None) == ok
