"""Benchmark truy vấn Serving Layer trên Trino: thời gian phản hồi, dữ liệu quét, số file đọc, và kiểm tra partition pruning.
Mỗi truy vấn chạy 1 lần làm nóng + N lần đo. Chạy trong container có mạng tới Trino:
  docker exec -e PYTHONPATH=/opt/spark/jobs -e TRINO_URL=http://trino:8080 traffic-spark-master \
      python3 /opt/spark/jobs/tests/benchmark/trino_queries.py --runs 5 --day 2025-12-04
"""
import argparse
import json
import statistics

from common.trino_http import query_with_stats


def queries(day: str, link: int) -> dict:
    d0, d1 = f"TIMESTAMP '{day} 00:00 UTC'", f"TIMESTAMP '{day} 00:00 UTC' + INTERVAL '1' DAY"
    return {
        "latest_all_links": "SELECT traffic_state, count(*) FROM lakehouse.serving.v_traffic_latest GROUP BY 1",
        "lambda_status": "SELECT * FROM lakehouse.serving.v_lambda_status",
        "unified_1link_1day": f"SELECT * FROM lakehouse.serving.v_traffic_5min_unified WHERE link_id = {link} "
                              f"AND window_start_utc >= {d0} AND window_start_utc < {d1}",
        "unified_all_1day": f"SELECT layer, count(*), sum(n_records) FROM lakehouse.serving.v_traffic_5min_unified "
                            f"WHERE window_start_utc >= {d0} AND window_start_utc < {d1} GROUP BY 1",
        "enriched_borough_1day": f"SELECT borough, traffic_state, weather_status, count(*), avg(avg_speed) "
                                 f"FROM lakehouse.serving.v_traffic_5min_enriched "
                                 f"WHERE window_start_utc >= {d0} AND window_start_utc < {d1} GROUP BY 1, 2, 3",
        "hourly_1month_olap": "SELECT link_id, sum(sum_speed) / nullif(sum(n_valid), 0) AS avg_speed, sum(n_records) "
                              "FROM lakehouse.serving.v_traffic_hourly_unified "
                              "WHERE hour_utc >= TIMESTAMP '2025-09-01 00:00 UTC' AND hour_utc < TIMESTAMP '2025-10-01 00:00 UTC' "
                              "GROUP BY link_id",
        "speed_raw_1day_pruned": f"SELECT count(*) FROM lakehouse.speed.traffic_5min_rt "
                                 f"WHERE window_start_utc >= {d0} AND window_start_utc < {d1}",
        "speed_raw_full_scan": "SELECT count(*), sum(n_records) FROM lakehouse.speed.traffic_5min_rt",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--day", default="2025-12-04")
    ap.add_argument("--link", type=int, default=4616340)
    a = ap.parse_args()
    out = {}
    for name, sql in queries(a.day, a.link).items():
        query_with_stats(sql)                                   # làm nóng (metadata cache, JIT)
        ms, stats = [], {}
        for _ in range(a.runs):
            rows, stats = query_with_stats(sql)
            ms.append(stats.get("elapsedTimeMillis"))
        out[name] = {"rows_out": len(rows), "elapsed_ms_median": statistics.median(ms), "elapsed_ms_max": max(ms),
                     "physical_input_mb": round((stats.get("physicalInputBytes") or 0) / 2 ** 20, 2),
                     "processed_rows": stats.get("processedRows"), "splits": stats.get("totalSplits")}
        print(name, json.dumps(out[name]))
    print(json.dumps(out))


if __name__ == "__main__":
    main()
