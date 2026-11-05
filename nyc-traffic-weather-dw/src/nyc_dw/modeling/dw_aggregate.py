"""Phase 5 - fact_traffic_hourly (luoi day du) + mart fact_traffic_weather_hourly + file DuckDB chua view."""
from nyc_dw import config
from nyc_dw.transformation.staging import dst_hours
from nyc_dw.utils.common import DATA, DW, MART, REPORTS, atomic_output, batch_id, duck, logger, sql_file, write_json

log = logger("phase5_aggregate")
fs = lambda p: str(p).replace("\\", "/")


def views(con):
    con.execute(sql_file("views/create_views.sql", dw=DW, mart=MART))


def run() -> bool:
    bid = batch_id("phase5"); log.info("batch %s", bid)
    rules = config.load("quality_rules")["traffic"]; dh = dst_hours(2025)
    con = duck()
    for v in ["dim_date", "dim_time", "dim_road_link", "dim_weather_location", "dim_weather_condition"]:
        con.execute(f"CREATE VIEW {v} AS SELECT * FROM read_parquet('{fs(DW / 'dimensions' / (v + '.parquet'))}')")
    con.execute(f"CREATE VIEW fact_traffic_reading AS SELECT * FROM read_parquet('{fs(DW)}/facts/fact_traffic_reading/*/*.parquet', hive_partitioning = true)")
    con.execute(f"CREATE VIEW fact_weather_hourly AS SELECT * FROM read_parquet('{fs(DW / 'facts' / 'fact_weather_hourly.parquet')}')")

    con.execute(sql_file("facts/fact_traffic_hourly.sql", expected_per_hour=rules["expected_readings_per_hour"],
                         outage_share=rules["network_outage_min_share_links_missing"], amb_date=dh["amb_date"], amb_hour=dh["amb_hour"], batch_id=bid))
    con.execute(sql_file("marts/fact_traffic_weather_hourly.sql", batch_id=bid))
    for tbl, root in [("fact_traffic_hourly", DW / "facts"), ("fact_traffic_weather_hourly", MART)]:
        root.mkdir(parents=True, exist_ok=True)
        with atomic_output(root / tbl) as tmp:
            con.execute(f"COPY (SELECT * FROM {tbl} ORDER BY month_utc, road_key, hour_utc) TO '{fs(tmp)}' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (month_utc))")

    q = lambda s: con.execute(s).fetchall()[0][0]
    a = con.execute("""SELECT count(*) FILTER (WHERE reading_ts_utc IS NOT NULL), count(*) FILTER (WHERE reading_ts_utc IS NULL),
                              count(*) FILTER (WHERE is_valid AND reading_ts_utc IS NOT NULL),
                              sum(speed_mph) FILTER (WHERE is_valid AND reading_ts_utc IS NOT NULL),
                              sum(travel_time_s) FILTER (WHERE is_valid AND reading_ts_utc IS NOT NULL)
                       FROM fact_traffic_reading""").fetchall()[0]
    h = con.execute("SELECT count(*), sum(n_records), sum(n_valid), sum(sum_speed), sum(sum_travel_time) FROM fact_traffic_hourly").fetchall()[0]
    exp_grid = q("""SELECT sum(datediff('hour', mn, mx) + 1) FROM (SELECT road_key, min(date_trunc('hour', reading_ts_utc)) mn, max(date_trunc('hour', reading_ts_utc)) mx
                    FROM fact_traffic_reading WHERE reading_ts_utc IS NOT NULL GROUP BY 1)""")
    j = con.execute("""SELECT count(*), count(DISTINCT (road_key, hour_utc)), count(*) FILTER (WHERE temperature_c IS NULL OR precipitation_mm IS NULL),
                              count(*) FILTER (WHERE condition_key = -1 OR weather_loc_key = -1) FROM fact_traffic_weather_hourly""").fetchall()[0]
    C = {
        "grid_rows_equal_expected": h[0] == exp_grid,
        "hourly_pk_unique": q("SELECT count(DISTINCT (road_key, hour_utc)) FROM fact_traffic_hourly") == h[0],
        "sum_n_records_equal_atomic_assigned": h[1] == a[0],
        "sum_n_valid_equal_atomic": h[2] == a[2],
        "sum_speed_equal_atomic": abs(h[3] - a[3]) < 1e-6 * a[3],
        "sum_travel_time_equal_atomic": h[4] == a[4],
        "counts_partition_n_records": q("SELECT count(*) FROM fact_traffic_hourly WHERE n_valid + n_zero + n_invalid_other <> n_records") == 0,
        "dst_unassigned_equal_2468": a[1] == 2468,
        "dst_ambiguous_hours_flagged": q("SELECT count(DISTINCT hour_utc) FROM fact_traffic_hourly WHERE is_dst_ambiguous_hour") == 2,
        "join_rows_equal_hourly": j[0] == h[0],
        "join_pk_unique": j[1] == j[0],
        "join_all_weather_matched": j[2] == 0 and j[3] == 0,
        "fk_date_time_valid": q("SELECT count(*) FROM fact_traffic_hourly ANTI JOIN dim_date USING (date_key)") == 0 and
                              q("SELECT count(*) FROM fact_traffic_hourly ANTI JOIN dim_time USING (time_key)") == 0,
    }
    R = {"batch_id": bid, "checks": C,
         "atomic": {"assigned": a[0], "dst_unassigned": a[1], "valid_assigned": a[2]},
         "hourly": {"rows": h[0], "expected_grid": exp_grid, "sum_n_records": h[1], "sum_n_valid": h[2]},
         "hourly_profile": con.sql("""SELECT count(*) FILTER (WHERE n_records = 0) empty_hours, count(*) FILTER (WHERE n_records > 0 AND n_valid = 0) zero_only_hours,
                                         count(*) FILTER (WHERE n_records > 12) overfilled_hours, count(*) FILTER (WHERE n_valid >= 6) hours_cov_ge_6,
                                         count(*) FILTER (WHERE has_stuck) stuck_hours, count(DISTINCT hour_utc) FILTER (WHERE is_network_outage) outage_hours
                                  FROM fact_traffic_hourly""").df().iloc[0].astype(int).to_dict(),
         "outage_hours_by_day": con.sql("""SELECT strftime(timezone('America/New_York', hour_utc), '%Y-%m-%d') d, count(DISTINCT hour_utc) h
                                         FROM fact_traffic_hourly WHERE is_network_outage GROUP BY 1 ORDER BY 1""").df().values.tolist(),
         "mart_rows": j[0],
         "partitions": sorted(p.name for p in (MART / "fact_traffic_weather_hourly").iterdir())}

    # File DuckDB chi chua VIEW tren Parquet (khong nhan ban du lieu)
    dbp = DATA / "nyc_dw.duckdb"
    with atomic_output(dbp) as tmp:
        c2 = duck(str(tmp)); views(c2); c2.close()
    R["duckdb_file"] = fs(dbp.relative_to(DATA.parent))
    write_json(R, REPORTS / "phase5_aggregate.json")
    n = lambda x: f"{int(x):,}".replace(",", " ")
    md = ["# Phase 5 – Aggregate fact & mart tích hợp", "", f"Batch `{bid}`. Chi tiết: `reports/phase5_aggregate.json`.", "",
          "| Tiêu chí | Kết quả |", "|---|---|"] + [f"| {k} | {'ĐẠT' if v else '**KHÔNG ĐẠT**'} |" for k, v in C.items()]
    hp = R["hourly_profile"]
    md += ["", f"- `fact_traffic_hourly`: {n(h[0])} dòng (lưới kỳ vọng {n(exp_grid)}); Σ n_records = {n(h[1])} = bản ghi atomic có giờ UTC ({n(a[0])}); {n(a[1])} bản ghi giờ DST kép không gán giờ.",
           f"- Giờ không có bản ghi: {n(hp['empty_hours'])} · có bản ghi nhưng không có số đo hợp lệ: {n(hp['zero_only_hours'])} · > 12 bản ghi (ghi bù): {n(hp['overfilled_hours'])} · có ≥ 6 lần đo hợp lệ: {n(hp['hours_cov_ge_6'])} · có tốc độ đứng yên: {n(hp['stuck_hours'])}.",
           f"- Giờ sự cố toàn mạng (≥ 90% link không có bản ghi, không tính 2 giờ DST kép): {hp['outage_hours']} giờ, theo ngày: {R['outage_hours_by_day']}.",
           f"- `fact_traffic_weather_hourly`: {n(j[0])} dòng, phân vùng {len(R['partitions'])} tháng; 100% dòng có thời tiết.",
           f"- File DuckDB chỉ chứa view: `{R['duckdb_file']}`.",
           "", "Ghi chú: thêm cột `is_dst_ambiguous_hour` vào hai fact theo giờ để 2 giờ UTC của giờ DST kép (05Z, 06Z ngày 02/11) không bị hiểu nhầm là sự cố."]
    (REPORTS / "phase5_aggregate_report.md").write_text("\n".join(md), encoding="utf-8")
    ok = all(C.values()); log.info("PHASE 5 %s %s", "OK" if ok else "CO LOI", {k: v for k, v in C.items() if not v})
    return ok
