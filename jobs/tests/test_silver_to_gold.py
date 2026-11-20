"""Small Spark SQL tests; default uses TEMP VIEW fixtures only.

Run with spark-submit and PYTHONPATH pointing to jobs, as the existing tests do.
--integration also exercises real Iceberg operations in a verified temporary
local warehouse, without writing production lakehouse tables.
"""

import argparse
import shutil
import tempfile
from datetime import date
from math import isclose
from pathlib import Path

from common.spark_session import build_spark
from batch.transformation.silver_to_gold import (
    GoldLoader, congestion_sql, month_ranges, road_metrics_sql, traffic_reading_sql, weather_bands_sql,
)


def test_month_boundaries():
    assert list(month_ranges(date(2024, 2, 28), date(2024, 3, 2))) == [
        (date(2024, 2, 28), date(2024, 2, 29)), (date(2024, 3, 1), date(2024, 3, 2))]
    assert list(month_ranges(date(2025, 12, 31), date(2026, 1, 1))) == [
        (date(2025, 12, 31), date(2025, 12, 31)), (date(2026, 1, 1), date(2026, 1, 1))]


def test_bands(spark):
    spark.sql("""CREATE OR REPLACE TEMP VIEW test_gold_band_input AS
      SELECT * FROM VALUES
        (1,0D,0D,0D,-1D,0),
        (2,2.49D,0.49D,8.04672D,0D,1),
        (3,2.5D,0.5D,24.14016D,10D,2),
        (4,7.5D,1D,30D,25D,95),
        (5,CAST(NULL AS DOUBLE),CAST(NULL AS DOUBLE),CAST(NULL AS DOUBLE),CAST(NULL AS DOUBLE),999),
        (6,-1D,-1D,-1D,CAST('NaN' AS DOUBLE),45),
        (7,CAST('NaN' AS DOUBLE),CAST('Infinity' AS DOUBLE),CAST('Infinity' AS DOUBLE),5D,56),
        (8,0D,0D,0D,5D,66),
        (9,0D,0D,0D,5D,77)
      AS t(id,precipitation,snowfall,wind_speed_10m,temperature_2m,weather_code)""")
    rows = {r.id: (r.precip_band,r.snow_band,r.wind_band,r.temp_band,r.weather_group)
            for r in spark.sql(weather_bands_sql("test_gold_band_input")).collect()}
    assert rows[1] == ("dry","none","low","freezing","clear"), rows
    assert rows[2] == ("light","light","moderate","cold","clear"), rows
    assert rows[3] == ("moderate","moderate","high","mild","cloudy"), rows
    assert rows[4] == ("heavy","heavy","high","hot","thunderstorm"), rows
    assert rows[5] == ("unknown",)*5, rows
    assert rows[6] == ("unknown","unknown","unknown","unknown","fog"), rows
    assert rows[7] == ("unknown","unknown","unknown","cold","drizzle"), rows
    assert rows[8][-1] == "rain" and rows[9][-1] == "snow", rows


def test_exact_calibration(spark):
    spark.sql("""CREATE OR REPLACE TEMP VIEW test_gold_calibration_traffic AS
      SELECT CAST(link_id AS BIGINT) AS link_id,CAST(ts AS TIMESTAMP) AS traffic_utc,
        DATE '2025-01-02' AS date,status,speed,travel_time,'transcom1' AS transcom_id
      FROM VALUES
        (1,'2025-01-02 05:05:00',0,10D,100L),
        (1,'2025-01-02 06:05:00',0,20D,100L),
        (1,'2025-01-02 07:05:00',0,40D,100L),
        (1,'2025-01-02 08:05:00',0,80D,100L),
        (1,'2025-01-02 17:05:00',CAST(NULL AS INT),30D,100L),
        (1,'2025-01-02 09:05:00',-101,1000D,1000L)
      AS t(link_id,ts,status,speed,travel_time)""")
    spark.sql("""CREATE OR REPLACE TEMP VIEW test_gold_calibration_links AS
      SELECT * FROM VALUES (1L,'road1','borough','40.0,-74.0 40.1,-74.1','g1'),
        (2L,'road2','borough','40.0,-74.0 40.1,-74.1','g1')
      AS t(link_id,link_name,borough,link_points,cell_id)""")
    spark.sql("""CREATE OR REPLACE TEMP VIEW test_gold_calibration_weather AS
      SELECT 'g1' AS cell_id,CAST(ts AS TIMESTAMP) AS weather_utc,precipitation,snowfall
      FROM VALUES ('2025-01-02 05:00:00',0D,0D),('2025-01-02 06:00:00',0D,0D),
        ('2025-01-02 07:00:00',0D,1D),('2025-01-02 08:00:00',1D,0D),
        ('2025-01-02 09:00:00',0D,0D),('2025-01-02 17:00:00',0D,0D)
      AS t(ts,precipitation,snowfall)""")
    rows = {r.link_id: r for r in spark.sql(road_metrics_sql(
        "test_gold_calibration_traffic","test_gold_calibration_links","test_gold_calibration_weather",
        date(2025,1,1),date(2025,9,30))).collect()}
    assert isclose(rows['1'].length_m,1341.12,rel_tol=1e-12), rows
    # Exact continuous P85 of [10,20] is 18.5; approx/discrete 20 would fail.
    assert isclose(rows['1'].free_flow_speed_mph,18.5,rel_tol=1e-12), rows
    assert rows['2'].length_m is None and rows['2'].free_flow_speed_mph is None, rows
    assert rows['1'].owner is None, rows
    assert rows['1'].geometry_wkt == 'LINESTRING(-74.0 40.0, -74.1 40.1)', rows


def test_traffic_scd_timezone(spark):
    spark.sql("""CREATE OR REPLACE TEMP VIEW test_gold_readings AS
      SELECT 1L AS link_id,CAST(ts AS TIMESTAMP) AS traffic_utc,CAST(d AS DATE) AS date,
        status,30D AS speed,100L AS travel_time
      FROM VALUES
        ('2025-02-01 03:05:00','2025-01-31',0),
        ('2025-02-01 05:05:00','2025-02-01',CAST(NULL AS INT)),
        ('2025-03-09 06:55:00','2025-03-09',0),
        ('2025-03-09 07:05:00','2025-03-09',0),
        ('2025-02-01 06:05:00','2025-02-01',-101)
      AS t(ts,d,status)""")
    spark.sql("""CREATE OR REPLACE TEMP VIEW test_gold_road_versions AS
      SELECT * FROM VALUES (10,'1',DATE '2025-01-01',DATE '2025-02-01'),
        (20,'1',DATE '2025-02-01',CAST(NULL AS DATE))
      AS t(road_link_key,link_id,valid_from,valid_to)""")
    rows = spark.sql(traffic_reading_sql("test_gold_readings","test_gold_road_versions",
        date(2025,1,1),date(2025,9,30))).orderBy("reading_ts_utc").collect()
    assert [(r.road_link_key,r.date_key,r.hour_key,r.status) for r in rows] == [
        (10,20250131,22,'0'),(20,20250201,0,None),(20,20250309,1,'0'),(20,20250309,3,'0')], rows


def test_common_congestion_and_precip_alignment(spark):
    spark.sql("""CREATE OR REPLACE TEMP VIEW test_gold_congestion_traffic AS
      SELECT road_link_key,CAST(ts AS TIMESTAMP) AS reading_ts_utc,20250102 AS date_key,
        status,speed_mph,travel_time_sec
      FROM VALUES
        (1,'2025-01-02 05:05:00','0',10D,100D),
        (1,'2025-01-02 05:10:00','0',0D,200D),
        (1,'2025-01-02 05:15:00',CAST(NULL AS STRING),20D,400D),
        (1,'2025-01-02 05:20:00','-101',20D,999D),
        (1,'2025-01-02 05:25:00','0',20D,0D),
        (1,'2025-01-02 05:30:00','0',CAST('NaN' AS DOUBLE),999D),
        (2,'2025-01-02 05:05:00','0',20D,100D),
        (3,'2025-01-02 05:05:00','0',20D,100D)
      AS t(road_link_key,ts,status,speed_mph,travel_time_sec)""")
    spark.sql("""CREATE OR REPLACE TEMP VIEW test_gold_congestion_roads AS
      SELECT * FROM VALUES (1,1000D,50D,'g1'),(2,CAST(NULL AS DOUBLE),50D,'g1'),(3,1000D,0D,'g1')
      AS t(road_link_key,length_m,free_flow_speed_mph,nearest_grid_key)""")
    spark.sql("""CREATE OR REPLACE TEMP VIEW test_gold_congestion_weather AS
      SELECT 'g1' AS grid_key,CAST(ts AS TIMESTAMP) AS hour_start_utc,7 AS condition_key,precipitation_mm
      FROM VALUES ('2025-01-02 04:00:00',99D),('2025-01-02 05:00:00',1.7D),('2025-01-02 06:00:00',88D)
      AS t(ts,precipitation_mm)""")
    rows = spark.sql(congestion_sql("test_gold_congestion_traffic","test_gold_congestion_roads",
        "test_gold_congestion_weather",date(2025,1,2),date(2025,1,2))).collect()
    assert len(rows)==1, rows
    r=rows[0]
    assert (r.road_link_key,r.date_key,r.hour_key,r.grid_key,r.condition_key)==(1,20250102,0,'g1',7), rows
    assert isclose(r.sum_distance_m,3000) and isclose(r.sum_travel_time_sec,700), rows
    assert isclose(r.sum_free_flow_time_sec,3000/(50*0.44704),rel_tol=1e-12), rows
    assert isclose(r.precip_prev_1h_mm,1.7), rows


def test_iceberg_scd2(spark):
    """Real MERGE/replacement operations, only in the isolated temporary catalog."""
    catalog = 'gold_scd_fixture'
    silver, gold = f'{catalog}.silver', f'{catalog}.gold'
    spark.sql(f'CREATE NAMESPACE {silver}')
    ddl = (Path(__file__).resolve().parents[1] / 'ddl' / 'gold.sql').read_text(encoding='utf-8')
    ddl = '\n'.join(line for line in ddl.splitlines() if not line.lstrip().startswith('--'))
    for statement in ddl.replace('lakehouse.gold',gold).split(';'):
        if statement.strip():
            spark.sql(statement.strip())
    spark.sql(f"""CREATE TABLE {silver}.traffic_cleaned USING ICEBERG AS
      SELECT 1L AS link_id,CAST(ts AS TIMESTAMP) AS traffic_utc,CAST(d AS DATE) AS date,
        0 AS status,speed,100L AS travel_time,'transcom1' AS transcom_id
      FROM VALUES ('2025-01-15 05:05:00','2025-01-15',10D),
        ('2025-02-15 05:05:00','2025-02-15',20D) AS t(ts,d,speed)""")
    spark.sql(f"""CREATE TABLE {silver}.link_info USING ICEBERG AS
      SELECT * FROM VALUES (1L,'road1','borough','40.0,-74.0 40.1,-74.1','g1'),
        (2L,'road2','borough','40.0,-74.0 40.1,-74.1','g1')
      AS t(link_id,link_name,borough,link_points,cell_id)""")
    spark.sql(f"""CREATE TABLE {silver}.weather_cleaned USING ICEBERG AS
      SELECT 'g1' AS cell_id,CAST(ts AS TIMESTAMP) AS weather_utc,40D AS latitude,-74D AS longitude,
        10D AS temperature_2m,50 AS relative_humidity_2m,0D AS precipitation,0D AS snowfall,
        10D AS wind_speed_10m,0 AS weather_code
      FROM VALUES ('2025-01-15 05:00:00'),('2025-02-15 05:00:00') AS t(ts)""")
    def loader(effective=None,start=date(2025,1,1),end=date(2025,2,28)):
        return GoldLoader(spark,start,end,date(2025,1,1),date(2025,2,28),effective,silver,gold)
    job=loader()
    job.load_dimensions()
    initial={r.link_id:r.road_link_key for r in spark.sql(f'SELECT * FROM {gold}.dim_road_link').collect()}
    conditions=spark.sql(f'SELECT * FROM {gold}.dim_weather_condition ORDER BY condition_key').collect()
    job.load_dimensions()
    assert {r.link_id:r.road_link_key for r in spark.sql(f'SELECT * FROM {gold}.dim_road_link').collect()}==initial
    assert spark.sql(f'SELECT * FROM {gold}.dim_weather_condition ORDER BY condition_key').collect()==conditions
    job.load_traffic()
    job.load_weather()
    job.load_congestion()
    job.validate()
    spark.sql(f"UPDATE {silver}.link_info SET link_name='renamed road1' WHERE link_id=1")
    try:
        job.load_dimensions()
    except ValueError as error:
        assert '--effective-date' in str(error), error
    else:
        raise AssertionError('Changed road silently rewrote current SCD2 version')
    try:
        loader(date(2025,2,1),start=date(2025,2,16)).load_dimensions()
    except ValueError as error:
        assert 'expand --start-date/--end-date' in str(error), error
    else:
        raise AssertionError('SCD2 change invalidated facts outside the requested reload scope')
    changed=loader(date(2025,2,1))
    changed.load_dimensions()
    roads=spark.sql(f'SELECT * FROM {gold}.dim_road_link ORDER BY road_link_key').collect()
    assert len(roads)==3, roads
    old=next(r for r in roads if r.road_link_key==initial['1'])
    new=next(r for r in roads if r.link_id=='1' and r.valid_to is None)
    assert old.valid_from==date(2025,1,1) and old.valid_to==date(2025,2,1), roads
    assert new.road_link_key>max(initial.values()) and new.valid_from==date(2025,2,1), roads
    assert next(r for r in roads if r.link_id=='2').road_link_key==initial['2'], roads
    changed.load_traffic()
    changed.load_congestion()
    changed.validate()
    readings=spark.sql(f'SELECT date_key,road_link_key FROM {gold}.fact_traffic_reading ORDER BY date_key').collect()
    assert [(r.date_key,r.road_link_key) for r in readings]==[(20250115,initial['1']),(20250215,new.road_link_key)], readings
    changed.load_dimensions()
    changed.load_traffic()
    changed.load_congestion()
    assert spark.sql(f'SELECT * FROM {gold}.dim_road_link ORDER BY road_link_key').collect()==roads
    assert spark.sql(f'SELECT * FROM {gold}.dim_weather_condition ORDER BY condition_key').collect()==conditions
    assert spark.sql(f'SELECT date_key,road_link_key FROM {gold}.fact_traffic_reading ORDER BY date_key').collect()==readings
    # A source correction removes February facts, preserving January outside the rerun scope.
    spark.sql(f"UPDATE {silver}.traffic_cleaned SET status=-101 WHERE date=DATE '2025-02-15'")
    february=loader(start=date(2025,2,1))
    february.load_traffic()
    february.load_congestion()
    february.validate()
    assert spark.table(f'{gold}.fact_traffic_reading').count()==1
    assert spark.table(f'{gold}.fact_congestion_hourly').count()==1
    assert spark.sql(f'SELECT road_link_key FROM {gold}.fact_traffic_reading').first().road_link_key==initial['1']
    # New condition tuples allocate a new key without changing any old tuple/key.
    spark.sql(f"UPDATE {silver}.weather_cleaned SET weather_code=65 WHERE weather_utc=TIMESTAMP '2025-02-15 05:00:00'")
    february._load_conditions()
    expanded=spark.sql(f'SELECT * FROM {gold}.dim_weather_condition ORDER BY condition_key').collect()
    assert len(expanded)==len(conditions)+1 and expanded[0]==conditions[0], expanded
    february._load_conditions()
    assert spark.sql(f'SELECT * FROM {gold}.dim_weather_condition ORDER BY condition_key').collect()==expanded
    february.load_weather()
    assert spark.sql(f"SELECT condition_key FROM {gold}.fact_weather_hourly WHERE hour_start_utc=TIMESTAMP '2025-02-15 05:00:00'").first().condition_key==expanded[-1].condition_key
    february.validate()
    print('test_iceberg_scd2: PASS (initial/rerun keys, effective-date guard, SCD2, stale-key/status healing)',flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--integration',action='store_true',help='Also test real Iceberg operations in a temporary local warehouse')
    args=parser.parse_args()
    test_month_boundaries()
    warehouse=None
    extra={}
    if args.integration:
        warehouse=Path(tempfile.mkdtemp(prefix='traffic-gold-scd-test-')).resolve()
        extra={
            'spark.sql.catalog.gold_scd_fixture':'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.gold_scd_fixture.type':'hadoop',
            'spark.sql.catalog.gold_scd_fixture.warehouse':warehouse.as_uri(),
        }
    spark=build_spark("test_silver_to_gold",extra)
    try:
        for test in (test_bands,test_exact_calibration,test_traffic_scd_timezone,
                     test_common_congestion_and_precip_alignment):
            test(spark)
            print(f"{test.__name__}: PASS",flush=True)
        if args.integration:
            test_iceberg_scd2(spark)
    finally:
        spark.stop()
        if warehouse:
            root=Path(tempfile.gettempdir()).resolve()
            if warehouse.parent!=root or not warehouse.name.startswith('traffic-gold-scd-test-'):
                raise RuntimeError(f'Refusing cleanup outside the verified temporary fixture: {warehouse}')
            shutil.rmtree(warehouse)
    print("SILVER_TO_GOLD_TEMP_VIEW_TESTS: PASS",flush=True)


if __name__=='__main__':
    main()
