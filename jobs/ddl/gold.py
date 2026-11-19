"""Initialize the empty Gold Iceberg tables using Spark SQL.

The schema lives in gold.sql, beside this entrypoint. This job creates missing
objects by default. --recreate explicitly drops/purges these eight tables first.
It does not seed dimensions or load Silver data; silver_to_gold.py owns loading
and logical PK/FK/SCD checks.
"""

import argparse
from pathlib import Path

from common.spark_session import build_spark


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recreate", action="store_true",
                        help="Explicitly drop/purge the eight Gold tables before creating their current schema")
    args = parser.parse_args()
    # The checked-in DDL has full-line comments and no semicolons in literals.
    ddl_path = Path(__file__).with_suffix(".sql")
    sql = "\n".join(
        line for line in ddl_path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )

    spark = build_spark("ddl_gold")
    try:
        if args.recreate:
            for table in ("fact_congestion_hourly", "fact_weather_hourly", "fact_traffic_reading",
                          "dim_road_link", "dim_weather_condition", "dim_weather_grid", "dim_hour", "dim_date"):
                spark.sql(f"DROP TABLE IF EXISTS lakehouse.gold.{table} PURGE")
        for statement in sql.split(";"):
            if statement.strip():
                spark.sql(statement.strip())
        spark.sql("SHOW TABLES IN lakehouse.gold").show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
