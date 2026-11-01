#!/bin/bash
# Chỉ chạy 1 lần khi volume postgres_data còn trống
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER airflow WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;

    CREATE USER iceberg WITH PASSWORD '${ICEBERG_DB_PASSWORD}';
    CREATE DATABASE iceberg_catalog OWNER iceberg;
EOSQL