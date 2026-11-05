-- Tao view tren cac file Parquet cua DW (dung cho kiem dinh va OLAP). Tham so: {dw}, {mart}
CREATE OR REPLACE VIEW dim_date              AS SELECT * FROM read_parquet('{dw}/dimensions/dim_date.parquet');
CREATE OR REPLACE VIEW dim_time              AS SELECT * FROM read_parquet('{dw}/dimensions/dim_time.parquet');
CREATE OR REPLACE VIEW dim_road_link         AS SELECT * FROM read_parquet('{dw}/dimensions/dim_road_link.parquet');
CREATE OR REPLACE VIEW dim_weather_location  AS SELECT * FROM read_parquet('{dw}/dimensions/dim_weather_location.parquet');
CREATE OR REPLACE VIEW dim_weather_condition AS SELECT * FROM read_parquet('{dw}/dimensions/dim_weather_condition.parquet');
CREATE OR REPLACE VIEW fact_traffic_reading  AS SELECT * FROM read_parquet('{dw}/facts/fact_traffic_reading/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW fact_weather_hourly   AS SELECT * FROM read_parquet('{dw}/facts/fact_weather_hourly.parquet');
CREATE OR REPLACE VIEW fact_traffic_hourly   AS SELECT * FROM read_parquet('{dw}/facts/fact_traffic_hourly/*/*.parquet', hive_partitioning = true);
CREATE OR REPLACE VIEW fact_traffic_weather_hourly AS SELECT * FROM read_parquet('{mart}/fact_traffic_weather_hourly/*/*.parquet', hive_partitioning = true);
