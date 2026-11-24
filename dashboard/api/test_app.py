# Unit test backend dashboard (chạy trong container dashboard): cd /app/api && python -m pytest test_app.py -q
import datetime as dt
import threading
import time

import app


def test_wkt_to_coords_real_linestring():
    assert app.wkt_to_coords("LINESTRING(-73.99 40.75, -74.00 40.76)") == [[-73.99, 40.75], [-74.0, 40.76]]
    assert app.wkt_to_coords(None) is None
    assert app.wkt_to_coords("POINT(-73.9 40.7)") is None           # không vẽ hình học giả khi thiếu dữ liệu
    assert app.wkt_to_coords("LINESTRING(-73.99 40.75)") is None   # 1 điểm không thành đường


def test_literal_timestamp_is_utc_constant():
    t = dt.datetime(2026, 1, 1, 4, 58, 9, tzinfo=dt.timezone.utc)
    assert app.lit(t) == "TIMESTAMP '2026-01-01 04:58:09 UTC'"
    assert app.parse_ts("2026-01-01T04:58:09.000000Z") == t


def test_forecast_status_rules():
    sql = app.status_case("2026-01-01T04:45:00Z")
    assert "NO_FORECAST" in sql and "EVALUATED" in sql and "MISSING_ACTUAL" in sql
    # PENDING so với mốc số thực tế đã có, không so với giờ máy. Literal không kèm múi giờ (cùng kiểu với bộ lọc
    # prediction_origin_utc); phiên Trino của backend chạy ở UTC nên giá trị đúng bằng mốc UTC.
    assert "> TIMESTAMP '2026-01-01 04:45:00' THEN 'PENDING'" in sql
    # nguồn đã ngừng: đích sau mốc số thực tế không bao giờ có số -> BEYOND_DATA, không phải PENDING
    ended = app.status_case("2026-01-01T04:45:00Z", ended=True)
    assert "THEN 'BEYOND_DATA'" in ended and "'PENDING'" not in ended


def test_cache_single_flight():
    calls = []

    def slow():
        calls.append(1)
        time.sleep(0.3)
        return 42
    out = []
    ts = [threading.Thread(target=lambda: out.append(app.cached(("t", "sf"), 60, slow))) for _ in range(5)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert out == [42] * 5 and len(calls) == 1, "5 yêu cầu đồng thời chỉ chạy 1 lần"


def test_clean_line_drops_corrupt_points():
    # điểm ngoài khung NYC (chữ số bị cắt) và một "gai" giữa đường bị bỏ; đường thưa điểm nhưng thẳng hàng giữ nguyên
    line = [[-73.9990, 40.7600], [-74.91, 40.7621], [-73.9993, 40.7634], [-73.9995, 40.7650]]
    out, dropped = app.clean_line(line)
    assert dropped == 1 and [-74.91, 40.7621] not in out
    spike = [[-73.9300, 40.7990], [-73.9299, 40.7995], [-73.9299, 40.8705], [-73.9297, 40.7999]]
    out, dropped = app.clean_line(spike)
    assert dropped == 1 and [-73.9299, 40.8705] not in out
    sparse = [[-74.1933, 40.5902], [-74.1905, 40.5775], [-74.1900, 40.5700], [-74.1895, 40.5650]]
    assert app.clean_line(sparse) == (sparse, 0)
    long_end = [[-74.19332, 40.5902], [-74.19046, 40.57748], [-74.1903, 40.5770], [-74.1901, 40.5765]]
    assert app.clean_line(long_end) == (long_end, 0)               # điểm cuối thật cách 1,4 km: giữ
