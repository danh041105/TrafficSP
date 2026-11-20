"""Load Gold with Spark SQL; run one writer at a time.

Traffic scope is inclusive New York dates, processed one month at a time.
Weather includes every current Silver hour, including the boundary buffer.
Road calibration uses explicit, fixed baseline dates independently of fact scope.
The initial road snapshot is assumed valid from 2025-01-01; subsequent SCD2
changes require --effective-date and never rewrite an existing version's key.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from common.spark_session import build_spark


TIMEZONE = "America/New_York"
BAND_COLUMNS = ["precip_band", "snow_band", "wind_band", "temp_band", "weather_group"]
ROAD_COLUMNS = ["link_id", "transcom_id", "link_name", "borough", "owner", "geometry_wkt",
                "length_m", "free_flow_speed_mph", "nearest_grid_key"]
STEPS = ["dimensions", "traffic", "weather", "congestion", "validate"]


def month_ranges(start: date, end: date):
    """Inclusive dates, split at month boundaries without expanding scope."""
    current = start
    while current <= end:
        following = date(current.year + (current.month == 12), current.month % 12 + 1, 1)
        stop = min(end, following - timedelta(days=1))
        yield current, stop
        current = stop + timedelta(days=1)


def finite_sql(column: str) -> str:
    return f"({column} IS NOT NULL AND NOT isnan({column}) AND abs({column}) < CAST('Infinity' AS DOUBLE))"


def kept_sql(alias: str = "t") -> str:
    return f"({alias}.status IS NULL OR {alias}.status <> -101)"


def date_scope(start: date, end: date, column: str = "t.date") -> str:
    return f"{column} BETWEEN DATE '{start}' AND DATE '{end}'"


def weather_bands_sql(source: str) -> str:
    """Band boundaries and WMO mapping; NULL/invalid measurements are unknown."""
    p, s, w, t = "precipitation", "snowfall", "wind_speed_10m / 1.609344", "temperature_2m"
    return f"""
      SELECT w.*,
        CASE WHEN NOT {finite_sql(p)} OR {p}<0 THEN 'unknown'
             WHEN {p}=0 THEN 'dry' WHEN {p}<2.5 THEN 'light'
             WHEN {p}<7.5 THEN 'moderate' ELSE 'heavy' END AS precip_band,
        CASE WHEN NOT {finite_sql(s)} OR {s}<0 THEN 'unknown'
             WHEN {s}=0 THEN 'none' WHEN {s}<0.5 THEN 'light'
             WHEN {s}<1 THEN 'moderate' ELSE 'heavy' END AS snow_band,
        CASE WHEN NOT {finite_sql(w)} OR {w}<0 THEN 'unknown'
             WHEN {w}<5 THEN 'low' WHEN {w}<15 THEN 'moderate' ELSE 'high' END AS wind_band,
        CASE WHEN NOT {finite_sql(t)} THEN 'unknown'
             WHEN {t}<0 THEN 'freezing' WHEN {t}<10 THEN 'cold'
             WHEN {t}<25 THEN 'mild' ELSE 'hot' END AS temp_band,
        CASE WHEN weather_code IN (0,1) THEN 'clear'
             WHEN weather_code IN (2,3) THEN 'cloudy'
             WHEN weather_code IN (45,48) THEN 'fog'
             WHEN weather_code IN (51,53,55,56,57) THEN 'drizzle'
             WHEN weather_code IN (61,63,65,66,67,80,81,82) THEN 'rain'
             WHEN weather_code IN (71,73,75,77,85,86) THEN 'snow'
             WHEN weather_code IN (95,96,99) THEN 'thunderstorm'
             ELSE 'unknown' END AS weather_group
      FROM {source} w
    """


def road_metrics_sql(traffic: str, links: str, weather: str, start: date, end: date) -> str:
    """Exact median implied length and exact dry/snow-free night P85 speed."""
    return f"""
      WITH retained AS (
        SELECT * FROM {traffic} t WHERE {kept_sql()} AND {date_scope(start, end)}
      ), lengths AS (
        SELECT link_id,
          percentile_cont(0.5) WITHIN GROUP (ORDER BY speed * 0.44704 * travel_time) AS length_m
        FROM retained WHERE speed>0 AND travel_time>0
          AND {finite_sql('speed')} AND {finite_sql('travel_time')}
        GROUP BY link_id
      ), free_flow AS (
        SELECT t.link_id,
          percentile_cont(0.85) WITHIN GROUP (ORDER BY t.speed) AS free_flow_speed_mph
        FROM retained t JOIN {links} l ON t.link_id=l.link_id
        JOIN {weather} w ON l.cell_id=w.cell_id AND date_trunc('hour',t.traffic_utc)=w.weather_utc
        WHERE hour(from_utc_timestamp(t.traffic_utc,'{TIMEZONE}')) BETWEEN 0 AND 5
          AND w.precipitation=0 AND w.snowfall=0 AND t.speed>0 AND {finite_sql('t.speed')}
        GROUP BY t.link_id
      ), transcom AS (
        SELECT link_id,min(transcom_id) AS transcom_id FROM retained GROUP BY link_id
      )
      SELECT CAST(l.link_id AS STRING) AS link_id, x.transcom_id, l.link_name, l.borough,
        CAST(NULL AS STRING) AS owner,
        concat('LINESTRING(',concat_ws(', ',transform(split(trim(l.link_points),' +'),
          p -> concat(split(p,',')[1],' ',split(p,',')[0]))),')') AS geometry_wkt,
        m.length_m, f.free_flow_speed_mph, l.cell_id AS nearest_grid_key
      FROM {links} l LEFT JOIN lengths m ON l.link_id=m.link_id
        LEFT JOIN free_flow f ON l.link_id=f.link_id LEFT JOIN transcom x ON l.link_id=x.link_id
    """


def congestion_sql(traffic: str, roads: str, weather: str, start: date, end: date) -> str:
    """A common eligible reading set for all three sums, weather joined once."""
    lower, upper = int(start.strftime("%Y%m%d")), int(end.strftime("%Y%m%d"))
    return f"""
      WITH totals AS (
        SELECT t.road_link_key,date_trunc('hour',t.reading_ts_utc) AS hour_start_utc,
          r.nearest_grid_key AS grid_key,
          sum(r.length_m) AS sum_distance_m,
          sum(t.travel_time_sec) AS sum_travel_time_sec,
          sum(r.length_m/(r.free_flow_speed_mph*0.44704)) AS sum_free_flow_time_sec
        FROM {traffic} t JOIN {roads} r ON t.road_link_key=r.road_link_key
        WHERE t.date_key BETWEEN {lower} AND {upper}
          AND (t.status IS NULL OR t.status <> '-101')
          AND t.speed_mph>=0 AND t.travel_time_sec>0 AND r.length_m>0 AND r.free_flow_speed_mph>0
          AND {finite_sql('t.speed_mph')} AND {finite_sql('t.travel_time_sec')}
          AND {finite_sql('r.length_m')} AND {finite_sql('r.free_flow_speed_mph')}
        GROUP BY t.road_link_key,date_trunc('hour',t.reading_ts_utc),r.nearest_grid_key
      )
      SELECT a.road_link_key,a.hour_start_utc,
        CAST(date_format(from_utc_timestamp(a.hour_start_utc,'{TIMEZONE}'),'yyyyMMdd') AS INT) AS date_key,
        hour(from_utc_timestamp(a.hour_start_utc,'{TIMEZONE}')) AS hour_key,
        a.grid_key,w.condition_key,a.sum_distance_m,a.sum_travel_time_sec,a.sum_free_flow_time_sec,
        w.precipitation_mm AS precip_prev_1h_mm
      FROM totals a LEFT JOIN {weather} w
        ON a.grid_key=w.grid_key AND a.hour_start_utc=w.hour_start_utc
    """


def traffic_reading_sql(traffic: str, roads: str, start: date, end: date) -> str:
    return f"""SELECT r.road_link_key,t.traffic_utc AS reading_ts_utc,
      CAST(date_format(from_utc_timestamp(t.traffic_utc,'{TIMEZONE}'),'yyyyMMdd') AS INT) AS date_key,
      hour(from_utc_timestamp(t.traffic_utc,'{TIMEZONE}')) AS hour_key,
      CAST(t.status AS STRING) AS status,t.speed AS speed_mph,CAST(t.travel_time AS DOUBLE) AS travel_time_sec
      FROM {traffic} t LEFT JOIN {roads} r
      ON CAST(t.link_id AS STRING)=r.link_id AND t.date>=r.valid_from
        AND (r.valid_to IS NULL OR t.date<r.valid_to)
      WHERE {kept_sql()} AND {date_scope(start,end)}"""


class GoldLoader:
    def __init__(self, spark, start: date, end: date, baseline_start: date, baseline_end: date,
                 effective_date: date | None = None, silver: str = "lakehouse.silver",
                 gold: str = "lakehouse.gold"):
        self.spark, self.start, self.end = spark, start, end
        self.baseline_start, self.baseline_end = baseline_start, baseline_end
        self.effective_date, self.silver, self.gold = effective_date, silver, gold
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    def sql(self, query: str):
        return self.spark.sql(query)

    def require_empty(self, query: str, message: str):
        if self.sql(query).limit(1).count():
            raise ValueError(message)

    def unique(self, table: str, columns: list[str], where: str = "TRUE"):
        keys = ",".join(columns)
        self.require_empty(f"SELECT {keys} FROM {table} WHERE {where} GROUP BY {keys} HAVING count(*)>1",
                           f"Duplicate logical key: {table}({keys})")
        self.require_empty(f"SELECT 1 FROM {table} WHERE {where} AND (" +
                           " OR ".join(f"{c} IS NULL" for c in columns) + ")",
                           f"NULL logical key: {table}({keys})")

    def merge(self, table: str, source: str, keys: list[str], update: bool = True):
        on = " AND ".join(f"t.{c}=s.{c}" for c in keys)
        matched = "WHEN MATCHED THEN UPDATE SET *" if update else ""
        self.sql(f"MERGE INTO {self.gold}.{table} t USING ({source}) s ON {on} "
                 f"{matched} WHEN NOT MATCHED THEN INSERT *")

    def replace_rows(self, table: str, source: str, keys: list[str], scope: str):
        """Replace one bounded fact slice, including removed source keys.

        The validated source is materialized before DELETE. DELETE+append are
        two commits, so a failure between them requires rerunning this slice;
        a rerun restores it from Silver without reallocating dimension keys.
        """
        staged = self.sql(source).cache()
        staged.createOrReplaceTempView("gold_fact_stage")
        try:
            count = staged.count()
            self.unique("gold_fact_stage", keys)
            print(f"GOLD_REPLACE_BEGIN {table}: {scope}; staged_rows={count}; rerun slice after interruption", flush=True)
            self.sql(f"DELETE FROM {self.gold}.{table} WHERE {scope}")
            if count:
                staged.writeTo(f"{self.gold}.{table}").append()
            print(f"GOLD_REPLACE_DONE {table}: rows={count}", flush=True)
        finally:
            staged.unpersist()

    def load_dimensions(self):
        self.unique(f"{self.silver}.link_info", ["link_id"])
        self.unique(f"{self.silver}.weather_cleaned", ["cell_id", "weather_utc"])
        self.require_empty(f"""SELECT cell_id FROM {self.silver}.weather_cleaned GROUP BY cell_id
          HAVING count(DISTINCT struct(latitude,longitude))<>1""", "Grid has conflicting coordinates")
        self.require_empty(f"""SELECT 1 FROM {self.silver}.weather_cleaned
          WHERE cell_id IS NULL OR weather_utc IS NULL OR latitude IS NULL OR longitude IS NULL
            OR minute(weather_utc)<>0 OR second(weather_utc)<>0""", "Invalid grid/weather hour")
        self.require_empty(f"""SELECT link_id FROM {self.silver}.traffic_cleaned t
          WHERE {kept_sql()} AND {date_scope(self.baseline_start,self.baseline_end)}
          GROUP BY link_id HAVING count(DISTINCT transcom_id)>1""", "Conflicting retained transcom_id")

        calendar = f"""
          WITH bounds AS (
            SELECT min(d) AS lo,max(d) AS hi FROM (
              SELECT min(date) AS d FROM {self.silver}.traffic_cleaned
              UNION ALL SELECT max(date) FROM {self.silver}.traffic_cleaned
              UNION ALL SELECT min(to_date(from_utc_timestamp(weather_utc,'{TIMEZONE}')))
                FROM {self.silver}.weather_cleaned
              UNION ALL SELECT max(to_date(from_utc_timestamp(weather_utc,'{TIMEZONE}')))
                FROM {self.silver}.weather_cleaned
            )
          ), days AS (SELECT explode(sequence(lo,hi,INTERVAL 1 DAY)) AS d FROM bounds)
          SELECT CAST(date_format(d,'yyyyMMdd') AS INT) AS date_key,d AS full_date,
            year(d) AS year,month(d) AS month,date_format(d,'EEEE') AS day_of_week,
            dayofweek(d) IN (1,7) AS is_weekend,CAST(NULL AS BOOLEAN) AS is_holiday,
            CASE WHEN month(d) IN (12,1,2) THEN 'winter' WHEN month(d) IN (3,4,5) THEN 'spring'
                 WHEN month(d) IN (6,7,8) THEN 'summer' ELSE 'autumn' END AS season FROM days
        """
        self.merge("dim_date", calendar, ["date_key"], update=False)
        self.merge("dim_hour", """SELECT CAST(id AS INT) AS hour_key,CAST(id AS INT) AS hour_24,
          CASE WHEN id<6 THEN 'night' WHEN id<10 THEN 'morning' WHEN id<16 THEN 'daytime'
               WHEN id<20 THEN 'evening' ELSE 'late_evening' END AS time_band FROM range(24)""",
                   ["hour_key"], update=False)
        self.merge("dim_weather_grid", f"""SELECT cell_id AS grid_key,min(latitude) AS latitude,
          min(longitude) AS longitude,CAST(NULL AS DOUBLE) AS elevation_m
          FROM {self.silver}.weather_cleaned GROUP BY cell_id""", ["grid_key"])
        self._load_conditions()
        self._load_roads()
        print("GOLD_DIMENSIONS: loaded calendar/hour/grid/condition/road", flush=True)

    def _load_conditions(self):
        columns = ",".join(BAND_COLUMNS)
        comparison = " AND ".join(f"c.{c}=d.{c}" for c in BAND_COLUMNS)
        source = f"""WITH candidates AS (SELECT DISTINCT {columns} FROM (
          {weather_bands_sql(f'{self.silver}.weather_cleaned')})), new_rows AS (
          SELECT c.* FROM candidates c LEFT ANTI JOIN {self.gold}.dim_weather_condition d ON {comparison}
        ) SELECT CAST((SELECT coalesce(max(condition_key),0) FROM {self.gold}.dim_weather_condition)
          + row_number() OVER (ORDER BY {columns}) AS INT) AS condition_key,{columns} FROM new_rows"""
        self.merge("dim_weather_condition", source, BAND_COLUMNS, update=False)

    def _load_roads(self):
        candidates = self.sql(road_metrics_sql(f"{self.silver}.traffic_cleaned", f"{self.silver}.link_info",
                             f"{self.silver}.weather_cleaned", self.baseline_start, self.baseline_end)).cache()
        candidates.createOrReplaceTempView("gold_road_candidates")
        candidates.count()  # Materialize calibration exactly once, before any MERGE.
        try:
            self.unique(f"{self.gold}.dim_road_link", ["road_link_key"])
            self.unique(f"{self.gold}.dim_road_link", ["link_id"], "valid_to IS NULL")
            self.require_empty(f"""SELECT link_id FROM {self.gold}.dim_road_link GROUP BY link_id
              HAVING count_if(valid_to IS NULL)<>1""", "Each existing road needs exactly one open version")
            equal = " AND ".join(f"c.{c} <=> d.{c}" for c in ROAD_COLUMNS if c != "link_id")
            changed = f"""SELECT c.*,d.road_link_key AS old_key,d.valid_from AS old_from
              FROM gold_road_candidates c LEFT JOIN {self.gold}.dim_road_link d
                ON c.link_id=d.link_id AND d.valid_to IS NULL
              WHERE d.road_link_key IS NULL OR NOT ({equal})"""
            self.sql(changed).createOrReplaceTempView("gold_road_changes")
            if self.effective_date is None:
                self.require_empty("SELECT 1 FROM gold_road_changes WHERE old_key IS NOT NULL",
                                   "Road attributes/calibration changed: supply --effective-date for a new SCD2 version")
            effective = self.effective_date or date(2025, 1, 1)
            self.require_empty(f"SELECT 1 FROM gold_road_changes WHERE old_from>=DATE '{effective}'",
                               "SCD2 effective date must be later than the current version start")
            lower, upper = self.start.strftime('%Y%m%d'), self.end.strftime('%Y%m%d')
            for fact in ('fact_traffic_reading', 'fact_congestion_hourly'):
                self.require_empty(f"""SELECT 1 FROM {self.gold}.{fact} f JOIN gold_road_changes c
                  ON f.road_link_key=c.old_key WHERE f.date_key>={effective.strftime('%Y%m%d')}
                    AND NOT(f.date_key BETWEEN {lower} AND {upper})""",
                                   "SCD2 change affects existing facts outside the requested reload dates; expand --start-date/--end-date")
            stage = self.sql(f"""SELECT c.*,
              CAST((SELECT coalesce(max(road_link_key),0) FROM {self.gold}.dim_road_link)
                + row_number() OVER (ORDER BY link_id) AS INT) AS road_link_key,
              DATE '{effective}' AS valid_from,CAST(NULL AS DATE) AS valid_to FROM gold_road_changes c""").cache()
            stage.createOrReplaceTempView("gold_road_stage")
            try:
                count = stage.count()
                if count:
                    names = ["road_link_key", *ROAD_COLUMNS, "valid_from", "valid_to"]
                    cols, values = ",".join(names), ",".join(f"s.{c}" for c in names)
                    self.sql(f"""MERGE INTO {self.gold}.dim_road_link t USING (
                      SELECT old_key AS match_key,* FROM gold_road_stage WHERE old_key IS NOT NULL
                      UNION ALL SELECT CAST(NULL AS INT) AS match_key,* FROM gold_road_stage
                    ) s ON t.road_link_key=s.match_key
                    WHEN MATCHED AND t.valid_to IS NULL THEN UPDATE SET t.valid_to=s.valid_from
                    WHEN NOT MATCHED AND s.match_key IS NULL THEN INSERT ({cols}) VALUES ({values})""")
                print(f"GOLD_ROAD_NEW_VERSIONS: {count}", flush=True)
            finally:
                stage.unpersist()
        finally:
            candidates.unpersist()

    def traffic_source(self, start: date, end: date):
        return traffic_reading_sql(f"{self.silver}.traffic_cleaned", f"{self.gold}.dim_road_link", start, end)

    def load_traffic(self):
        for start, end in month_ranges(self.start, self.end):
            self.unique(f"{self.silver}.traffic_cleaned", ["link_id", "traffic_utc"],
                        f"{kept_sql('traffic_cleaned')} AND {date_scope(start,end,'date')}")
            source = self.traffic_source(start, end)
            self.require_empty(f"SELECT 1 FROM ({source}) WHERE road_link_key IS NULL",
                               "Traffic has no effective road dimension version")
            scope = f"date_key BETWEEN {start.strftime('%Y%m%d')} AND {end.strftime('%Y%m%d')}"
            self.replace_rows("fact_traffic_reading", source, ["road_link_key", "reading_ts_utc"], scope)
            print(f"GOLD_TRAFFIC: {start}..{end}", flush=True)

    def load_weather(self):
        bounds = self.sql(f"SELECT min(weather_utc) AS lo,max(weather_utc) AS hi FROM {self.silver}.weather_cleaned").first()
        if bounds.lo is None:
            return
        self.sql(weather_bands_sql(f"{self.silver}.weather_cleaned")).createOrReplaceTempView("gold_weather_banded")
        on = " AND ".join(f"w.{c}=c.{c}" for c in BAND_COLUMNS)
        for start, end in month_ranges(bounds.lo.date(), bounds.hi.date()):
            source = f"""SELECT w.cell_id AS grid_key,w.weather_utc AS hour_start_utc,
              CAST(date_format(from_utc_timestamp(w.weather_utc,'{TIMEZONE}'),'yyyyMMdd') AS INT) AS date_key,
              hour(from_utc_timestamp(w.weather_utc,'{TIMEZONE}')) AS hour_key,c.condition_key,w.weather_code,
              w.precipitation AS precipitation_mm,CAST(NULL AS DOUBLE) AS rain_mm,w.snowfall AS snowfall_cm,
              w.temperature_2m AS temperature_c,w.wind_speed_10m/1.609344 AS wind_speed_mph,
              CAST(w.relative_humidity_2m AS DOUBLE) AS relative_humidity_pct,CAST(NULL AS DOUBLE) AS cloud_cover_pct
              FROM gold_weather_banded w LEFT JOIN {self.gold}.dim_weather_condition c ON {on}
              WHERE to_date(w.weather_utc) BETWEEN DATE '{start}' AND DATE '{end}'"""
            self.require_empty(f"SELECT 1 FROM ({source}) WHERE condition_key IS NULL", "Unresolved weather condition")
            scope = (f"hour_start_utc>=TIMESTAMP '{start} 00:00:00' AND "
                     f"hour_start_utc<TIMESTAMP '{end+timedelta(days=1)} 00:00:00'")
            self.replace_rows("fact_weather_hourly", source, ["grid_key", "hour_start_utc"], scope)
            print(f"GOLD_WEATHER: {start}..{end}", flush=True)

    def load_congestion(self):
        for start, end in month_ranges(self.start, self.end):
            source = congestion_sql(f"{self.gold}.fact_traffic_reading", f"{self.gold}.dim_road_link",
                                    f"{self.gold}.fact_weather_hourly", start, end)
            self.require_empty(f"SELECT 1 FROM ({source}) WHERE condition_key IS NULL OR grid_key IS NULL",
                               "Eligible traffic hour has no weather/condition/grid")
            scope = f"date_key BETWEEN {start.strftime('%Y%m%d')} AND {end.strftime('%Y%m%d')}"
            self.replace_rows("fact_congestion_hourly", source, ["road_link_key", "hour_start_utc"], scope)
            print(f"GOLD_CONGESTION: {start}..{end}", flush=True)

    def validate(self):
        dimension_keys = {"dim_date": "date_key", "dim_hour": "hour_key", "dim_weather_grid": "grid_key",
                          "dim_weather_condition": "condition_key", "dim_road_link": "road_link_key"}
        for table, key in dimension_keys.items():
            self.unique(f"{self.gold}.{table}", [key])
        self.unique(f"{self.gold}.dim_road_link", ["link_id"], "valid_to IS NULL")
        self.unique(f"{self.gold}.dim_weather_condition", BAND_COLUMNS)
        self.require_empty(f"""SELECT 1 FROM {self.gold}.dim_road_link
          WHERE valid_to IS NOT NULL AND valid_to<=valid_from""", "Invalid road validity interval")
        self.require_empty(f"""SELECT 1 FROM (SELECT valid_to,
          lead(valid_from) OVER (PARTITION BY link_id ORDER BY valid_from) AS next_start
          FROM {self.gold}.dim_road_link) WHERE next_start IS NOT NULL AND (valid_to IS NULL OR valid_to>next_start)""",
                           "Overlapping road SCD2 intervals")
        for table in ("fact_traffic_reading", "fact_weather_hourly", "fact_congestion_hourly"):
            timestamp = "reading_ts_utc" if table == "fact_traffic_reading" else "hour_start_utc"
            self.require_empty(f"""SELECT 1 FROM {self.gold}.{table}
              WHERE date_key<>CAST(date_format(from_utc_timestamp({timestamp},'{TIMEZONE}'),'yyyyMMdd') AS INT)
                OR hour_key<>hour(from_utc_timestamp({timestamp},'{TIMEZONE}'))""", f"Timezone key mismatch: {table}")
            for key, dimension in [("date_key", "dim_date"), ("hour_key", "dim_hour")]:
                self._foreign_key(table, key, dimension, key)
            if table != "fact_weather_hourly":
                self._foreign_key(table, "road_link_key", "dim_road_link", "road_link_key")
            if table != "fact_traffic_reading":
                self._foreign_key(table, "grid_key", "dim_weather_grid", "grid_key")
                self._foreign_key(table, "condition_key", "dim_weather_condition", "condition_key")
        self._foreign_key("dim_road_link", "nearest_grid_key", "dim_weather_grid", "grid_key", nullable=True)
        self.unique(f"{self.gold}.fact_weather_hourly", ["grid_key", "hour_start_utc"])
        weather_counts = self.sql(f"""SELECT (SELECT count(*) FROM {self.silver}.weather_cleaned) AS expected,
          (SELECT count(*) FROM {self.gold}.fact_weather_hourly) AS actual""").first()
        if weather_counts.expected != weather_counts.actual:
            raise ValueError(f"Weather count mismatch: {weather_counts}")
        self.require_empty(f"""SELECT 1 FROM {self.silver}.weather_cleaned s LEFT ANTI JOIN
          {self.gold}.fact_weather_hourly g ON s.cell_id=g.grid_key AND s.weather_utc=g.hour_start_utc""",
                           "Silver weather key absent from Gold")
        self.require_empty(f"""SELECT 1 FROM {self.gold}.fact_weather_hourly g LEFT ANTI JOIN
          {self.silver}.weather_cleaned s ON s.cell_id=g.grid_key AND s.weather_utc=g.hour_start_utc""",
                           "Gold weather key absent from Silver")
        self.require_empty(f"SELECT 1 FROM {self.gold}.fact_traffic_reading WHERE status='-101'", "Excluded status in Gold")
        for start, end in month_ranges(self.start, self.end):
            lower, upper = int(start.strftime("%Y%m%d")), int(end.strftime("%Y%m%d"))
            scope = f"date_key BETWEEN {lower} AND {upper}"
            for table in ("fact_traffic_reading", "fact_congestion_hourly"):
                timestamp = "reading_ts_utc" if table == "fact_traffic_reading" else "hour_start_utc"
                self.unique(f"{self.gold}.{table}", ["road_link_key", timestamp], scope)
            counts = self.sql(f"""SELECT (SELECT count(*) FROM {self.silver}.traffic_cleaned t
              WHERE {kept_sql()} AND {date_scope(start,end)}) AS expected,
              (SELECT count(*) FROM {self.gold}.fact_traffic_reading WHERE {scope}) AS actual""").first()
            if counts.expected != counts.actual:
                raise ValueError(f"Traffic count mismatch {start}..{end}: {counts}")
            self.require_empty(f"""SELECT 1 FROM {self.gold}.fact_traffic_reading t
              JOIN {self.gold}.dim_road_link r ON t.road_link_key=r.road_link_key
              WHERE t.{scope} AND (to_date(from_utc_timestamp(t.reading_ts_utc,'{TIMEZONE}'))<r.valid_from
                OR (r.valid_to IS NOT NULL AND to_date(from_utc_timestamp(t.reading_ts_utc,'{TIMEZONE}'))>=r.valid_to))""",
                               "Traffic references wrong road SCD2 interval")
            expected = congestion_sql(f"{self.gold}.fact_traffic_reading", f"{self.gold}.dim_road_link",
                                      f"{self.gold}.fact_weather_hourly", start, end)
            measures = ["sum_distance_m", "sum_travel_time_sec", "sum_free_flow_time_sec", "precip_prev_1h_mm"]
            mismatch = " OR ".join(f"NOT (e.{c} <=> a.{c}) AND (e.{c} IS NULL OR a.{c} IS NULL OR "
                                      f"abs(e.{c}-a.{c})>1e-8*greatest(1D,abs(e.{c})))" for c in measures)
            self.require_empty(f"""SELECT 1 FROM ({expected}) e FULL OUTER JOIN
              (SELECT * FROM {self.gold}.fact_congestion_hourly WHERE {scope}) a
              ON e.road_link_key=a.road_link_key AND e.hour_start_utc=a.hour_start_utc
              WHERE e.road_link_key IS NULL OR a.road_link_key IS NULL
                OR NOT(e.grid_key<=>a.grid_key) OR NOT(e.condition_key<=>a.condition_key) OR {mismatch}""",
                               "Congestion grain/measure mismatch")
            print(f"GOLD_VALIDATE_MONTH: {start}..{end} expected_traffic={counts.expected} PASS", flush=True)
        for table in dimension_keys:
            print(f"GOLD_COUNT {table}: {self.sql(f'SELECT count(*) AS n FROM {self.gold}.{table}').first().n}", flush=True)
        print("GOLD_VALIDATE: logical PK/FK/SCD/counts/timezone/congestion PASS", flush=True)

    def _foreign_key(self, table: str, column: str, dimension: str, target_column: str, nullable: bool = False):
        predicate = f"WHERE t.{column} IS NOT NULL" if nullable else ""
        self.require_empty(f"SELECT 1 FROM {self.gold}.{table} t LEFT ANTI JOIN {self.gold}.{dimension} d "
                           f"ON t.{column}=d.{target_column} {predicate}",
                           f"Foreign key violation: {table}.{column} -> {dimension}.{target_column}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--baseline-start-date", type=date.fromisoformat)
    parser.add_argument("--baseline-end-date", type=date.fromisoformat)
    parser.add_argument("--effective-date", type=date.fromisoformat)
    parser.add_argument("--steps", default=",".join(STEPS))
    parser.add_argument("--skip-validation", action="store_true")
    args = parser.parse_args()
    steps = args.steps.split(",")
    if any(s not in STEPS for s in steps):
        parser.error(f"steps must be a comma-separated subset of {STEPS}")
    baseline_start = args.baseline_start_date or args.start_date
    baseline_end = args.baseline_end_date or args.end_date
    if args.end_date < args.start_date or baseline_end < baseline_start:
        parser.error("end date must be >= start date")
    spark = build_spark("silver_to_gold")
    try:
        loader = GoldLoader(spark, args.start_date, args.end_date, baseline_start, baseline_end, args.effective_date)
        for step in STEPS:
            if step not in steps or (step == "validate" and args.skip_validation):
                continue
            getattr(loader, "validate" if step == "validate" else f"load_{step}")()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
