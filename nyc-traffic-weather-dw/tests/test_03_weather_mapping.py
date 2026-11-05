"""(6) can gio thoi tiet dung khoang, (7) spatial mapping co khoang cach + phien ban."""
from conftest import one


def test_instant_variables_taken_at_interval_start(con):
    # temperature/humidity/wind/gust cua khoang [H,H+1) = gia tri API tai H
    assert one(con, """SELECT count(*) FROM fact_weather_hourly f JOIN dim_weather_location l USING (weather_loc_key)
        JOIN stg_weather_hourly s ON s.weather_location_id = l.weather_location_id AND s.time_utc = f.interval_start_utc
        WHERE f.temperature_c IS DISTINCT FROM s.temperature_2m OR f.relative_humidity_pct IS DISTINCT FROM s.relative_humidity_2m
           OR f.wind_speed_kmh IS DISTINCT FROM s.wind_speed_10m OR f.wind_gust_kmh IS DISTINCT FROM s.wind_gusts_10m""") == 0


def test_accumulated_variables_taken_at_interval_end(con):
    # precipitation/snowfall/weather_code cua khoang [H,H+1) = gia tri API tai H+1 ("sum of the preceding hour")
    assert one(con, """SELECT count(*) FROM fact_weather_hourly f JOIN dim_weather_location l USING (weather_loc_key)
        JOIN dim_weather_condition c USING (condition_key)
        JOIN stg_weather_hourly s ON s.weather_location_id = l.weather_location_id AND s.time_utc = f.interval_start_utc + INTERVAL 1 HOUR
        WHERE f.precipitation_mm IS DISTINCT FROM s.precipitation OR f.snowfall_cm IS DISTINCT FROM s.snowfall
           OR c.weather_code IS DISTINCT FROM s.weather_code""") == 0


def test_weather_covers_local_year_exactly(con):
    r = con.execute("""SELECT min(timezone('America/New_York', interval_start_utc)), max(timezone('America/New_York', interval_start_utc)),
                              count(*) FROM fact_weather_hourly""").fetchall()[0]
    assert str(r[0]) == "2025-01-01 00:00:00" and str(r[1]) == "2025-12-31 23:00:00" and r[2] == 16 * 8760


def test_every_link_mapped_with_distance_and_version(con):
    assert one(con, "SELECT count(*) FROM dim_road_link WHERE road_key > 0") == 124
    assert one(con, """SELECT count(*) FROM dim_road_link WHERE road_key > 0 AND (weather_loc_key < 1 OR mapping_distance_km IS NULL
                       OR mapping_version IS NULL)""") == 0
    assert one(con, "SELECT max(mapping_distance_km) FROM dim_road_link") < 7.0
    assert one(con, "SELECT count(*) FROM road_weather_mapping WHERE mapping_method IS NULL OR mapping_version IS NULL") == 0
