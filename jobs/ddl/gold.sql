-- Spark SQL only. Spark owns writes, while Trino queries the shared catalog.
-- CREATE IF NOT EXISTS preserves existing tables and data on reruns.
-- Logical PK/FK and SCD rules are documented, not enforced by Iceberg DDL.
-- Weather grid keys equal the STRING cell_id already assigned in Silver.
-- Road version keys require incremental allocation by a future load job.
-- An INT column is not an automatic identity. Reruns retain existing version keys.
-- No data is inserted and no existing table schema is migrated by this init.

SET spark.sql.session.timeZone=UTC;

CREATE NAMESPACE IF NOT EXISTS lakehouse.gold;

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date (
    date_key INT NOT NULL COMMENT 'Logical PK, YYYYMMDD in the chosen business timezone',
    full_date DATE NOT NULL,
    year INT,
    month INT,
    day_of_week STRING,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    season STRING,
    is_cbd_tolling BOOLEAN
)
USING ICEBERG
COMMENT 'SCD 0 calendar dimension. Existing date attributes are immutable'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_hour (
    hour_key INT NOT NULL COMMENT 'Logical PK, stable key for an hour of day',
    hour_24 INT NOT NULL COMMENT 'Hour of day from 0 to 23',
    time_band STRING
)
USING ICEBERG
COMMENT 'Hour-of-day lookup. Business timezone must match dim_date keys'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_weather_grid (
    grid_key STRING NOT NULL COMMENT 'Logical PK, exactly silver.weather_cleaned.cell_id',
    latitude DOUBLE,
    longitude DOUBLE,
    elevation_m DOUBLE
)
USING ICEBERG
COMMENT 'SCD 1 weather grid. Update attributes in place while retaining grid_key'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_weather_condition (
    condition_key INT NOT NULL COMMENT 'Logical PK for one distinct set of condition bands',
    precip_band STRING,
    snow_band STRING,
    wind_band STRING,
    temp_band STRING,
    weather_group STRING
)
USING ICEBERG
COMMENT 'Weather condition band lookup. Classification thresholds require a business contract'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_road_link (
    road_link_key INT NOT NULL COMMENT 'Logical PK, incrementally allocated surrogate key for each new SCD 2 version',
    link_id STRING NOT NULL COMMENT 'Source road link business key, shared across versions',
    transcom_id STRING,
    link_name STRING,
    borough STRING,
    owner STRING,
    geometry_wkt STRING COMMENT 'WKT geometry with longitude latitude coordinate order',
    length_m DOUBLE,
    free_flow_speed_mph DOUBLE,
    nearest_grid_key STRING COMMENT 'Logical FK to dim_weather_grid.grid_key, directly from silver.link_info.cell_id',
    valid_from DATE NOT NULL COMMENT 'Inclusive start date of this version in the business timezone',
    valid_to DATE COMMENT 'Exclusive end date, NULL denotes the current open version'
)
USING ICEBERG
COMMENT 'SCD 2 road link. Versions of a link must not overlap in [valid_from, valid_to)'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_traffic_reading (
    road_link_key INT NOT NULL COMMENT 'Logical PK part and FK to dim_road_link.road_link_key',
    reading_ts_utc TIMESTAMP NOT NULL COMMENT 'Logical PK part, reading instant in UTC',
    date_key INT NOT NULL COMMENT 'Logical FK to dim_date.date_key in the business timezone',
    hour_key INT NOT NULL COMMENT 'Logical FK to dim_hour.hour_key in the business timezone',
    status STRING,
    speed_mph DOUBLE,
    travel_time_sec DOUBLE
)
USING ICEBERG
PARTITIONED BY (months(reading_ts_utc))
COMMENT 'Grain is one road link version and one UTC traffic reading instant'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_weather_hourly (
    grid_key STRING NOT NULL COMMENT 'Logical PK part and FK to dim_weather_grid.grid_key, equals source cell_id',
    hour_start_utc TIMESTAMP NOT NULL COMMENT 'Logical PK part, UTC start of represented weather hour',
    date_key INT NOT NULL COMMENT 'Logical FK to dim_date.date_key in the business timezone',
    hour_key INT NOT NULL COMMENT 'Logical FK to dim_hour.hour_key in the business timezone',
    condition_key INT NOT NULL COMMENT 'Logical FK to dim_weather_condition.condition_key',
    weather_code INT,
    precipitation_mm DOUBLE,
    rain_mm DOUBLE,
    snowfall_cm DOUBLE,
    temperature_c DOUBLE,
    wind_speed_mph DOUBLE,
    relative_humidity_pct DOUBLE,
    cloud_cover_pct DOUBLE
)
USING ICEBERG
PARTITIONED BY (months(hour_start_utc))
COMMENT 'Grain is one weather grid and one UTC hour. Align source accumulation intervals explicitly'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');

CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_congestion_hourly (
    road_link_key INT NOT NULL COMMENT 'Logical PK part and FK to dim_road_link.road_link_key',
    hour_start_utc TIMESTAMP NOT NULL COMMENT 'Logical PK part, inclusive start of the UTC traffic hour',
    date_key INT NOT NULL COMMENT 'Logical FK to dim_date.date_key in the business timezone',
    hour_key INT NOT NULL COMMENT 'Logical FK to dim_hour.hour_key in the business timezone',
    grid_key STRING NOT NULL COMMENT 'Logical FK to dim_weather_grid.grid_key, equals mapped Silver cell_id',
    condition_key INT NOT NULL COMMENT 'Logical FK to dim_weather_condition.condition_key',
    sum_distance_m DOUBLE,
    sum_travel_time_sec DOUBLE,
    sum_free_flow_time_sec DOUBLE,
    precip_prev_1h_mm DOUBLE
)
USING ICEBERG
PARTITIONED BY (months(hour_start_utc))
COMMENT 'Grain is one road link version and one UTC traffic hour. Measures require explicit valid-reading rules'
TBLPROPERTIES ('format-version' = '2', 'write.format.default' = 'parquet');
