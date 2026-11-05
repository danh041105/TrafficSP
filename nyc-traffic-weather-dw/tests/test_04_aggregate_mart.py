"""(8) aggregate khop atomic, (9) so dong truoc/sau JOIN, (10) measure tong hop dung, khong AVG(AVG)."""
import math

from conftest import one


def test_hourly_matches_atomic_per_link_and_month(con):
    bad = one(con, """
        WITH a AS (SELECT road_key, strftime(date_trunc('hour', reading_ts_utc), '%Y-%m') m, count(*) n, count(*) FILTER (WHERE is_valid) v,
                          sum(speed_mph) FILTER (WHERE is_valid) s, sum(travel_time_s) FILTER (WHERE is_valid) tt
                   FROM fact_traffic_reading WHERE reading_ts_utc IS NOT NULL GROUP BY 1, 2),
             h AS (SELECT road_key, month_utc m, sum(n_records) n, sum(n_valid) v, sum(sum_speed) s, sum(sum_travel_time) tt FROM fact_traffic_hourly GROUP BY 1, 2)
        SELECT count(*) FROM a FULL JOIN h USING (road_key, m)
        WHERE coalesce(a.n, 0) <> coalesce(h.n, 0) OR coalesce(a.v, 0) <> coalesce(h.v, 0)
           OR abs(coalesce(a.s, 0) - coalesce(h.s, 0)) > 1e-9 * greatest(1, abs(coalesce(a.s, 0))) OR coalesce(a.tt, 0) <> coalesce(h.tt, 0)""")
    assert bad == 0   # thang link khong co ban ghi: atomic khong co dong, hourly co dong n=0 -> so sanh sau coalesce


def test_unassigned_records_are_only_dst_ambiguous(con):
    total = one(con, "SELECT count(*) FROM fact_traffic_reading")
    assigned = one(con, "SELECT sum(n_records) FROM fact_traffic_hourly")
    amb = one(con, "SELECT count(*) FROM fact_traffic_reading WHERE is_dst_ambiguous")
    assert total - assigned == amb == 2468


def test_no_record_vs_zero_speed_distinguished(con):
    assert one(con, "SELECT count(*) FROM fact_traffic_hourly WHERE n_records = 0 AND (n_zero > 0 OR n_valid > 0)") == 0
    assert one(con, "SELECT count(*) FROM fact_traffic_hourly WHERE n_records = 0") > 0
    assert one(con, "SELECT count(*) FROM fact_traffic_hourly WHERE n_records > 0 AND n_valid = 0 AND n_zero > 0") > 0


def test_rollup_from_hourly_equals_atomic(con):
    # trung binh theo borough tu fact theo gio (SUM/SUM) phai bang tinh truc tiep tu atomic
    h = dict(con.execute("""SELECT r.borough, sum(f.sum_speed) / sum(f.n_valid) FROM fact_traffic_hourly f JOIN dim_road_link r USING (road_key) GROUP BY 1""").fetchall())
    a = dict(con.execute("""SELECT r.borough, avg(f.speed_mph) FROM fact_traffic_reading f JOIN dim_road_link r USING (road_key)
                            WHERE f.is_valid AND f.reading_ts_utc IS NOT NULL GROUP BY 1""").fetchall())
    assert h.keys() == a.keys()
    for b in h:
        assert math.isclose(h[b], a[b], rel_tol=1e-9), b
    # va KHAC voi AVG(AVG) (chung minh quy tac D-19 co y nghia)
    naive = dict(con.execute("""SELECT r.borough, avg(f.sum_speed / f.n_valid) FROM fact_traffic_hourly f JOIN dim_road_link r USING (road_key)
                                WHERE f.n_valid > 0 GROUP BY 1""").fetchall())
    assert any(abs(naive[b] - h[b]) > 1e-6 for b in h)


def test_std_from_components_matches_atomic(con):
    lid = one(con, "SELECT min(road_key) FROM dim_road_link WHERE road_key > 0")
    s = con.execute(f"""SELECT sqrt(sum(sum_speed_sq)/sum(n_valid) - power(sum(sum_speed)/sum(n_valid), 2)) FROM fact_traffic_hourly WHERE road_key = {lid}""").fetchall()[0][0]
    a = con.execute(f"""SELECT stddev_pop(speed_mph) FROM fact_traffic_reading WHERE road_key = {lid} AND is_valid AND reading_ts_utc IS NOT NULL""").fetchall()[0][0]
    assert math.isclose(s, a, rel_tol=1e-6)


def test_join_preserves_rows_and_grain(con):
    assert one(con, "SELECT count(*) FROM fact_traffic_weather_hourly") == one(con, "SELECT count(*) FROM fact_traffic_hourly")
    assert one(con, "SELECT count(*) - count(DISTINCT (road_key, hour_utc)) FROM fact_traffic_weather_hourly") == 0
    assert one(con, "SELECT count(*) FROM fact_traffic_weather_hourly WHERE temperature_c IS NULL OR precipitation_mm IS NULL") == 0


def test_mart_weather_equals_weather_fact(con):
    assert one(con, """SELECT count(*) FROM fact_traffic_weather_hourly m JOIN fact_weather_hourly w
                       ON w.weather_loc_key = m.weather_loc_key AND w.interval_start_utc = m.hour_utc
                       WHERE m.precipitation_mm IS DISTINCT FROM w.precipitation_mm OR m.temperature_c IS DISTINCT FROM w.temperature_c
                          OR m.condition_key IS DISTINCT FROM w.condition_key""") == 0
    # moi dong mart dung o thoi tiet cua link (khong lech mapping)
    assert one(con, """SELECT count(*) FROM fact_traffic_weather_hourly m JOIN dim_road_link r USING (road_key)
                       WHERE m.weather_loc_key <> r.weather_loc_key""") == 0
