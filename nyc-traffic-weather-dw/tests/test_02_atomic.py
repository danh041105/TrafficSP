"""(1)(2)(3)(4)(5) atomic facts: khop nguon tung gia tri, khong trung grain, PK/FK, DST co can cu."""
import datetime as dt
from zoneinfo import ZoneInfo

from conftest import one


def test_traffic_reading_values_identical_to_raw(con):
    # moi dong nguon co dung 1 dong fact voi speed/travel_time/timestamp giu nguyen
    mism = one(con, """SELECT count(*) FROM raw_nyc_dot r
        FULL JOIN (SELECT f.*, d.link_id FROM fact_traffic_reading f JOIN dim_road_link d USING (road_key)) f
          ON f.link_id = r.link_id AND f.reading_ts_local = CAST(r.data_as_of AS TIMESTAMP)
        WHERE f.link_id IS NULL OR r.link_id IS NULL OR f.speed_mph IS DISTINCT FROM r.speed OR f.travel_time_s IS DISTINCT FROM r.travel_time""")
    assert mism == 0


def test_traffic_reading_grain_unique(con):
    assert one(con, "SELECT count(*) - count(DISTINCT (road_key, reading_ts_local)) FROM fact_traffic_reading") == 0


def test_weather_grain_unique(con):
    assert one(con, "SELECT count(*) - count(DISTINCT (weather_loc_key, interval_start_utc)) FROM fact_weather_hourly") == 0


def test_dimension_keys_unique_and_unknown_present(con):
    for d, k, bk in [("dim_date", "date_key", "date_key"), ("dim_time", "time_key", "time_key"), ("dim_road_link", "road_key", "link_id"),
                     ("dim_weather_location", "weather_loc_key", "weather_location_id"), ("dim_weather_condition", "condition_key", "condition_key")]:
        assert one(con, f"SELECT count(*) - count(DISTINCT {k}) FROM {d}") == 0, d
        assert one(con, f"SELECT count(*) - count(DISTINCT {bk}) FROM {d}") == 0, d
        assert one(con, f"SELECT count(*) FROM {d} WHERE {k} = -1") == 1, d


def test_foreign_keys_resolve(con):
    pairs = [("fact_traffic_reading", "road_key", "dim_road_link"), ("fact_traffic_reading", "date_key", "dim_date"), ("fact_traffic_reading", "time_key", "dim_time"),
             ("fact_weather_hourly", "weather_loc_key", "dim_weather_location"), ("fact_weather_hourly", "condition_key", "dim_weather_condition"),
             ("fact_weather_hourly", "date_key", "dim_date"), ("fact_traffic_hourly", "road_key", "dim_road_link"),
             ("fact_traffic_weather_hourly", "weather_loc_key", "dim_weather_location"), ("fact_traffic_weather_hourly", "condition_key", "dim_weather_condition")]
    for f, k, d in pairs:
        assert one(con, f"SELECT count(*) FROM {f} ANTI JOIN {d} USING ({k})") == 0, (f, k)
        assert one(con, f"SELECT count(*) FROM {f} WHERE {k} = -1") == 0, (f, k, "unknown member duoc dung")


def test_dst_handling_matches_tzdata(con):
    ny = ZoneInfo("America/New_York")
    # giờ không tồn tại: 0 dòng; giờ kép: UTC NULL + cờ; mọi dòng khác: UTC = tzdata
    assert one(con, "SELECT count(*) FROM fact_traffic_reading WHERE reading_ts_local >= '2025-03-09 02:00' AND reading_ts_local < '2025-03-09 03:00'") == 0
    assert one(con, "SELECT count(*) FROM fact_traffic_reading WHERE is_dst_ambiguous AND reading_ts_utc IS NOT NULL") == 0
    assert one(con, "SELECT count(*) FROM fact_traffic_reading WHERE NOT is_dst_ambiguous AND reading_ts_utc IS NULL") == 0
    assert one(con, "SELECT count(*) FROM fact_traffic_reading WHERE is_dst_ambiguous") == one(con, """SELECT count(*) FROM fact_traffic_reading
        WHERE reading_ts_local >= '2025-11-02 01:00' AND reading_ts_local < '2025-11-02 02:00'""")
    sample = con.execute("""SELECT reading_ts_local, reading_ts_utc
                            FROM (SELECT reading_ts_local, reading_ts_utc FROM fact_traffic_reading WHERE reading_ts_utc IS NOT NULL)
                            USING SAMPLE reservoir(5000 ROWS) REPEATABLE (42)""").fetchall()
    # kem cac moc quanh 2 lan doi gio
    sample += con.execute("""SELECT reading_ts_local, reading_ts_utc FROM fact_traffic_reading WHERE reading_ts_utc IS NOT NULL
                             AND (reading_ts_local BETWEEN '2025-03-09 00:00' AND '2025-03-09 04:00'
                               OR reading_ts_local BETWEEN '2025-11-01 23:00' AND '2025-11-02 03:00')""").fetchall()
    for loc, utc in sample:
        exp = loc.replace(tzinfo=ny).astimezone(dt.timezone.utc)
        assert exp == utc.astimezone(dt.timezone.utc), (loc, utc)


def test_speed_zero_kept_not_dropped(con):
    assert one(con, "SELECT count(*) FROM fact_traffic_reading WHERE speed_mph = 0") == one(con, "SELECT count(*) FROM raw_nyc_dot WHERE speed = 0")
    assert one(con, "SELECT count(*) FROM fact_traffic_reading WHERE speed_mph = 0 AND is_valid") == 0
