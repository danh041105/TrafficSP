"""Validate the rebuilt Jan-Sep 2025 snapshot and four hourly dashboard SELECTs."""
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from profile_gold_mapping import query

EXPECTED_QUERIES = {
    "H01_peak_hour_profile", "H02_matched_weather_by_hour",
    "H03_weather_when_congested", "H04_congested_road_hours",
}


def main():
    root = Path(__file__).resolve().parent
    results = {"task": "TASK-20261120-002", "started_utc": datetime.now(timezone.utc).isoformat(), "queries": {}}
    output_path = root / "dashboard_hourly_query_results.json"
    results["schema"] = query("""SELECT table_name,column_name,data_type
      FROM lakehouse.information_schema.columns
      WHERE table_schema='gold' AND column_name IN ('is_cbd_tolling','grid_key','nearest_grid_key')
      ORDER BY table_name,column_name""")
    if len(results["schema"]["rows"]) != 4 or any(row[1] == "is_cbd_tolling" or row[2] != "varchar"
                                                       for row in results["schema"]["rows"]):
        raise ValueError("Gold schema has not been recreated with STRING grid keys and without CBD")
    tables = ["dim_date", "dim_hour", "dim_road_link", "dim_weather_grid", "dim_weather_condition",
              "fact_traffic_reading", "fact_weather_hourly", "fact_congestion_hourly"]
    results["gold_counts"] = query(" UNION ALL ".join(
        f"SELECT '{table}' AS table_name,count(*) AS n FROM lakehouse.gold.{table}" for table in tables))
    results["bronze_replay_counts"] = query("""SELECT 'traffic' AS source,run_id,count(*) AS n
      FROM lakehouse.bronze.traffic_raw GROUP BY run_id UNION ALL
      SELECT 'weather' AS source,run_id,count(*) AS n FROM lakehouse.bronze.weather_raw GROUP BY run_id""")
    results["silver_quality"] = query("""SELECT count(*) AS n,count(DISTINCT link_id) AS links,
      count_if(status=-101) AS excluded_status,count_if(speed<=0 OR travel_time<=0) AS nonpositive_metrics,
      count_if(is_weekend IS NULL OR season IS NULL) AS missing_enrichment
      FROM lakehouse.silver.traffic_cleaned""")
    counts = dict(results["gold_counts"]["rows"])
    silver = results["silver_quality"]["rows"][0]
    if counts["fact_traffic_reading"] != silver[0] or any(silver[2:]):
        raise ValueError(f"Silver/Gold count or traffic quality mismatch: {silver}, {counts}")
    bronze = {(row[0], row[1]): row[2] for row in results["bronze_replay_counts"]["rows"]}
    if bronze != {("traffic", "rebuild_20261119_001"): 9480364,
                  ("weather", "rebuild_20261119_001"): 105296}:
        raise ValueError(f"Unexpected nine-month replay counts: {bronze}")
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    sql_path = root.parent / "jobs" / "batch" / "dashboard" / "dashboard_queries.sql"
    for block in sql_path.read_text(encoding="utf-8").split(";"):
        lines = block.strip().splitlines()
        if not lines:
            continue
        label = next((line[3:].strip() for line in lines if re.match(r"^-- H\d{2}_\w+$", line)), "query")
        sql = "\n".join(line for line in lines if not line.lstrip().startswith("--")).strip()
        if not sql:
            continue
        if label not in EXPECTED_QUERIES or label in results["queries"]:
            raise ValueError(f"Missing, unexpected or duplicate query label: {label}")
        started = time.monotonic()
        try:
            result = query(sql)
            result["elapsed_seconds"] = round(time.monotonic() - started, 2)
            results["queries"][label] = result
            print(label, "PASS", len(result["rows"]), "rows", flush=True)
        except Exception as error:
            results["queries"][label] = {"error": str(error)}
            print(label, "FAIL", str(error)[:250], flush=True)
        output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    if set(results["queries"]) != EXPECTED_QUERIES or any("error" in value for value in results["queries"].values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
