"""Phase 4 - Dimensions (surrogate key on dinh) + atomic facts."""
import datetime as dt

import pandas as pd

from nyc_dw import config
from nyc_dw.modeling.reference import CATEGORY_VI, DAY_VI, MONTH_VI, WMO
from nyc_dw.transformation.staging import dst_hours
from nyc_dw.utils.common import DW, REPORTS, STG, atomic_output, batch_id, duck, logger, write_json

log = logger("phase4_dw_atomic")
DIMS, FACTS, REG = DW / "dimensions", DW / "facts", DW / "dimensions" / "_key_registry"
UNKNOWN = -1
fs = lambda p: str(p).replace("\\", "/")


def assign_keys(dim: str, bks: list[str]) -> dict:
    """Cap surrogate key on dinh: key cu giu nguyen, BK moi nhan max+1."""
    REG.mkdir(parents=True, exist_ok=True); path = REG / f"{dim}.parquet"
    reg = pd.read_parquet(path) if path.exists() else pd.DataFrame({"bk": pd.Series(dtype=str), "key": pd.Series(dtype="int64")})
    known = dict(zip(reg.bk, reg.key)); nxt = int(reg.key.max()) + 1 if len(reg) else 1
    new = []
    for b in sorted(set(bks)):
        if b not in known:
            known[b] = nxt; new.append((b, nxt)); nxt += 1
    if new:
        reg = pd.concat([reg, pd.DataFrame(new, columns=["bk", "key"])], ignore_index=True)
        with atomic_output(path) as tmp:
            reg.to_parquet(tmp, index=False)
    return known


def _write(con, sql_or_name: str, target, partition: str | None = None):
    with atomic_output(target) as tmp:
        part = f", PARTITION_BY ({partition})" if partition else ""
        con.execute(f"COPY ({sql_or_name}) TO '{fs(tmp)}' (FORMAT parquet, COMPRESSION zstd{part})")


def run() -> bool:
    bid = batch_id("phase4"); log.info("batch %s", bid)
    cal = config.load("calendar"); DIMS.mkdir(parents=True, exist_ok=True); FACTS.mkdir(parents=True, exist_ok=True)
    con = duck(); R = {"batch_id": bid}
    con.execute(f"CREATE VIEW stg_traffic AS SELECT * FROM read_parquet('{fs(STG)}/stg_traffic_reading/*/*.parquet', hive_partitioning = true)")
    for t in ["stg_road_link", "road_weather_mapping", "stg_weather_interval"]:
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{fs(STG / (t + '.parquet'))}')")

    # ---------- dim_date ----------
    dh = dst_hours(2025); hol = {str(k): v for k, v in cal["holidays_2025"].items()}
    rows = []
    for i in range(365):
        d = dt.date(2025, 1, 1) + dt.timedelta(days=i); iso = d.isocalendar()
        h = hol.get(d.isoformat())
        rows.append({"date_key": d.year * 10000 + d.month * 100 + d.day, "full_date": d, "day_of_week": iso.weekday,
                     "day_name_vi": DAY_VI[iso.weekday - 1], "is_weekend": iso.weekday >= 6, "is_holiday": h is not None,
                     "holiday_name": h, "day_type": "Holiday" if h else ("Weekend" if iso.weekday >= 6 else "Weekday"),
                     "iso_week": iso.week, "month": d.month, "month_name_vi": MONTH_VI[d.month - 1], "quarter": (d.month - 1) // 3 + 1,
                     "year": d.year, "is_dst_transition_day": d.isoformat() in (dh["amb_date"], dh["nonexist_date"])})
    rows.append({"date_key": UNKNOWN, "full_date": None, "day_of_week": None, "day_name_vi": "Không xác định", "is_weekend": None,
                 "is_holiday": None, "holiday_name": None, "day_type": "Unknown", "iso_week": None, "month": None,
                 "month_name_vi": None, "quarter": None, "year": None, "is_dst_transition_day": None})
    con.register("_dd", pd.DataFrame(rows)); _write(con, "SELECT * FROM _dd ORDER BY date_key", DIMS / "dim_date.parquet")

    # ---------- dim_time ----------
    rows = [{"time_key": h, "hour_label": f"{h:02d}:00",
             "day_part": next(p["name"] for p in cal["day_parts"] if p["from"] <= h <= p["to"])} for h in range(24)]
    rows.append({"time_key": UNKNOWN, "hour_label": None, "day_part": "Không xác định"})
    con.register("_dt", pd.DataFrame(rows)); _write(con, "SELECT time_key::TINYINT time_key, hour_label, day_part FROM _dt ORDER BY time_key", DIMS / "dim_time.parquet")

    # ---------- dim_weather_condition ----------
    ck = assign_keys("dim_weather_condition", [str(c[0]) for c in WMO])
    rows = [{"condition_key": ck[str(c)], "weather_code": c, "description_en": en, "description_vi": vi, "category": cat,
             "category_vi": CATEGORY_VI[cat], "precip_type": pt, "severity_rank": sev} for c, en, vi, cat, pt, sev in WMO]
    rows.append({"condition_key": UNKNOWN, "weather_code": None, "description_en": "Unknown", "description_vi": "Không xác định",
                 "category": "Unknown", "category_vi": "Không xác định", "precip_type": None, "severity_rank": None})
    con.register("_dc", pd.DataFrame(rows))
    _write(con, "SELECT condition_key::SMALLINT condition_key, weather_code::SMALLINT weather_code, description_en, description_vi, category, category_vi, precip_type, severity_rank::TINYINT severity_rank FROM _dc ORDER BY condition_key", DIMS / "dim_weather_condition.parquet")

    # ---------- dim_weather_location ----------
    wl = con.sql("""SELECT w.weather_location_id, any_value(w.model) model, any_value(h.grid_lat) grid_lat, any_value(h.grid_lon) grid_lon, any_value(h.elevation) elevation_m
                    FROM stg_weather_interval w JOIN read_parquet('""" + fs(STG / "stg_weather_hourly.parquet") + """') h USING (weather_location_id)
                    GROUP BY 1""").df()
    wk = assign_keys("dim_weather_location", wl.weather_location_id.tolist()); wl["weather_loc_key"] = wl.weather_location_id.map(wk)
    stat = con.sql("""SELECT m.weather_location_id, count(*) n_links_mapped, mode(l.borough) primary_borough
                      FROM road_weather_mapping m JOIN stg_road_link l USING (link_id) GROUP BY 1""").df()
    wl = wl.merge(stat, on="weather_location_id", how="left")
    wl = pd.concat([wl, pd.DataFrame([{"weather_loc_key": UNKNOWN, "weather_location_id": "UNKNOWN"}])], ignore_index=True)
    con.register("_wl", wl)
    _write(con, "SELECT weather_loc_key::INTEGER weather_loc_key, weather_location_id, model, grid_lat, grid_lon, elevation_m, n_links_mapped::SMALLINT n_links_mapped, primary_borough FROM _wl ORDER BY weather_loc_key", DIMS / "dim_weather_location.parquet")
    con.execute(f"CREATE VIEW dim_weather_location AS SELECT * FROM read_parquet('{fs(DIMS / 'dim_weather_location.parquet')}')")

    # ---------- dim_road_link ----------
    ff = cal["free_flow"]
    free = con.sql(f"""
        SELECT t.link_id, quantile_cont(t.speed_mph, {ff['percentile']}) AS free_flow_speed_mph, count(*) AS n_free_flow_readings
        FROM stg_traffic t
        JOIN road_weather_mapping m USING (link_id)
        JOIN stg_weather_interval w ON w.weather_location_id = m.weather_location_id
                                   AND w.interval_start_utc = date_trunc('hour', t.reading_ts_utc)
        WHERE t.invalid_reason IS NULL AND t.reading_ts_utc IS NOT NULL
          AND hour(timezone('America/New_York', t.reading_ts_utc)) IN ({','.join(map(str, ff['local_hours']))})
          AND w.precipitation_mm < {ff['dry']['precipitation_mm_lt']} AND w.snowfall_cm = {ff['dry']['snowfall_cm_eq']}
        GROUP BY 1""").df()
    L = con.sql("SELECT * FROM stg_road_link").df().merge(con.sql("SELECT * FROM road_weather_mapping").df(), on="link_id", suffixes=("", "_m"))
    L = L.merge(free, on="link_id", how="left")
    L.loc[L.n_free_flow_readings.fillna(0) < ff["min_readings"], "free_flow_speed_mph"] = None
    rk = assign_keys("dim_road_link", [f"{x}|1" for x in L.link_id]); L["road_key"] = [rk[f"{x}|1"] for x in L.link_id]
    L["weather_loc_key"] = L.weather_location_id.map(wk).fillna(UNKNOWN).astype(int)
    rules = config.load("quality_rules")
    L["is_low_quality"] = L.valid_share < rules["traffic"]["link_low_quality_valid_share"]
    con.register("_rl", L)
    _write(con, f"""
        SELECT road_key::INTEGER road_key, link_id, 1::SMALLINT version, source_internal_id, link_name, borough, 'New York City' city, owner,
               geometry_wkt, n_points_raw::SMALLINT n_points_raw, n_points_valid::SMALLINT n_points_valid, rep_lat, rep_lon, length_km_valid,
               shares_geometry_with, weather_loc_key::INTEGER weather_loc_key, distance_km mapping_distance_km, is_far mapping_is_far,
               cells_touched::TINYINT mapping_cells_touched, mapping_version, first_seen_utc, last_seen_utc, valid_share valid_share_2025,
               is_low_quality, free_flow_speed_mph, n_free_flow_readings::INTEGER n_free_flow_readings,
               first_seen_utc row_effective_from, TIMESTAMPTZ '9999-12-31 00:00:00+00' row_effective_to, TRUE is_current, '{bid}' load_batch_id
        FROM _rl
        UNION ALL
        SELECT -1, 'UNKNOWN', 1, NULL, 'Không xác định', 'Unknown', 'New York City', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, -1, NULL, NULL, NULL,
               NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, TIMESTAMPTZ '9999-12-31 00:00:00+00', TRUE, '{bid}'
        ORDER BY road_key""", DIMS / "dim_road_link.parquet")
    for d in ["dim_date", "dim_time", "dim_road_link", "dim_weather_condition"]:
        con.execute(f"CREATE VIEW {d} AS SELECT * FROM read_parquet('{fs(DIMS / (d + '.parquet'))}')")

    # ---------- fact_traffic_reading ----------
    _write(con, f"""
        SELECT coalesce(r.road_key, {UNKNOWN})::INTEGER road_key,
               (year(s.reading_ts_local) * 10000 + month(s.reading_ts_local) * 100 + day(s.reading_ts_local))::INTEGER date_key,
               hour(s.reading_ts_local)::TINYINT time_key,
               s.reading_ts_local, s.reading_ts_utc, s.utc_offset_min, s.speed_mph, s.travel_time_s::INTEGER travel_time_s,
               (s.invalid_reason IS NULL) is_valid, s.invalid_reason, s.is_dst_ambiguous, s.is_stuck, s.source_file,
               '{bid}' load_batch_id, s.source_month
        FROM stg_traffic s LEFT JOIN dim_road_link r ON r.link_id = s.link_id AND r.is_current
        ORDER BY s.source_month, road_key, s.reading_ts_local""", FACTS / "fact_traffic_reading", partition="source_month")
    con.execute(f"CREATE VIEW fact_traffic_reading AS SELECT * FROM read_parquet('{fs(FACTS)}/fact_traffic_reading/*/*.parquet', hive_partitioning = true)")

    # ---------- fact_weather_hourly ----------
    _write(con, f"""
        SELECT coalesce(l.weather_loc_key, {UNKNOWN})::INTEGER weather_loc_key, w.interval_start_utc,
               strftime(timezone('America/New_York', w.interval_start_utc), '%Y%m%d')::INTEGER date_key,
               hour(timezone('America/New_York', w.interval_start_utc))::TINYINT time_key,
               coalesce(c.condition_key, {UNKNOWN})::SMALLINT condition_key,
               w.temperature_c, w.relative_humidity_pct, w.wind_speed_kmh, w.wind_gust_kmh, w.precipitation_mm, w.snowfall_cm,
               w.model, w.source_file, '{bid}' load_batch_id
        FROM stg_weather_interval w
        LEFT JOIN dim_weather_location l ON l.weather_location_id = w.weather_location_id
        LEFT JOIN dim_weather_condition c ON c.weather_code = w.weather_code
        ORDER BY 1, 2""", FACTS / "fact_weather_hourly.parquet")
    con.execute(f"CREATE VIEW fact_weather_hourly AS SELECT * FROM read_parquet('{fs(FACTS / 'fact_weather_hourly.parquet')}')")

    # ---------- kiem tra ----------
    q = lambda s: con.execute(s).fetchall()[0][0]
    exp = config.sources()["nyc_dot"]["expected"]["rows_2025"]
    C = {
        "dim_date_365_plus_unknown": q("SELECT count(*) FROM dim_date") == 366 and q("SELECT count(DISTINCT date_key) FROM dim_date") == 366,
        "dim_date_11_holidays": q("SELECT count(*) FROM dim_date WHERE is_holiday") == 11,
        "dim_time_24_plus_unknown": q("SELECT count(DISTINCT time_key) FROM dim_time") == 25,
        "dim_road_link_124_plus_unknown_bk_unique": q("SELECT count(*) FROM dim_road_link") == 125 and q("SELECT count(DISTINCT (link_id, version)) FROM dim_road_link") == 125,
        "dim_road_link_pk_unique": q("SELECT count(DISTINCT road_key) FROM dim_road_link") == 125,
        "dim_road_link_all_mapped": q("SELECT count(*) FROM dim_road_link WHERE road_key > 0 AND weather_loc_key = -1") == 0,
        "dim_weather_location_16_plus_unknown": q("SELECT count(DISTINCT weather_loc_key) FROM dim_weather_location") == 17,
        "dim_weather_condition_28_plus_unknown": q("SELECT count(DISTINCT condition_key) FROM dim_weather_condition") == 29,
        "fact_traffic_reading_rows_equal_source": q("SELECT count(*) FROM fact_traffic_reading") == exp,
        "fact_traffic_reading_pk_unique": q("SELECT count(DISTINCT (road_key, reading_ts_local)) FROM fact_traffic_reading") == exp,
        "fact_traffic_reading_no_unknown_road": q("SELECT count(*) FROM fact_traffic_reading WHERE road_key = -1") == 0,
        "fact_traffic_reading_fk_date_time": q("SELECT count(*) FROM fact_traffic_reading f ANTI JOIN dim_date d USING (date_key)") == 0
                                            and q("SELECT count(*) FROM fact_traffic_reading f ANTI JOIN dim_time t USING (time_key)") == 0,
        "fact_weather_hourly_rows_16x8760": q("SELECT count(*) FROM fact_weather_hourly") == 140160,
        "fact_weather_hourly_pk_unique": q("SELECT count(DISTINCT (weather_loc_key, interval_start_utc)) FROM fact_weather_hourly") == 140160,
        "fact_weather_no_unknown_fk": q("SELECT count(*) FROM fact_weather_hourly WHERE weather_loc_key = -1 OR condition_key = -1") == 0,
        "fact_weather_fk_date_time": q("SELECT count(*) FROM fact_weather_hourly ANTI JOIN dim_date USING (date_key)") == 0,
    }
    R["checks"] = C
    R["dim_road_link"] = con.sql("""SELECT count(*) FILTER (road_key > 0) links, count(*) FILTER (is_low_quality) low_quality,
                                           count(*) FILTER (free_flow_speed_mph IS NULL AND road_key > 0) no_free_flow,
                                           round(min(free_flow_speed_mph),1) ff_min, round(median(free_flow_speed_mph),1) ff_med, round(max(free_flow_speed_mph),1) ff_max
                                    FROM dim_road_link""").df().iloc[0].astype(str).to_dict()
    R["fact_traffic_reading"] = con.sql("""SELECT count(*) n, sum(is_valid::INT) valid, sum(is_dst_ambiguous::INT) amb, sum(is_stuck::INT) stuck FROM fact_traffic_reading""").df().iloc[0].astype(int).to_dict()
    R["fact_weather_hourly_by_category"] = dict(con.sql("""SELECT c.category, count(*) FROM fact_weather_hourly f JOIN dim_weather_condition c USING (condition_key) GROUP BY 1 ORDER BY 2 DESC""").fetchall())
    R["date_key_check_local_vs_utc"] = con.sql("""SELECT interval_start_utc, date_key, time_key FROM fact_weather_hourly
                                                  WHERE weather_loc_key = 1 AND interval_start_utc IN ('2025-01-01 05:00:00+00','2025-03-09 06:00:00+00','2025-03-09 07:00:00+00',
                                                  '2025-11-02 05:00:00+00','2025-11-02 06:00:00+00','2026-01-01 04:00:00+00') ORDER BY 1""").df().astype(str).values.tolist()
    write_json(R, REPORTS / "phase4_dw_atomic.json")
    n = lambda x: f"{int(x):,}".replace(",", " ")
    md = [f"# Phase 4 – Dimensions & atomic facts", "", f"Batch `{bid}`. Chi tiết: `reports/phase4_dw_atomic.json`.", "", "| Tiêu chí | Kết quả |", "|---|---|"]
    md += [f"| {k} | {'ĐẠT' if v else '**KHÔNG ĐẠT**'} |" for k, v in C.items()]
    md += ["", f"- `dim_road_link`: {R['dim_road_link']}", f"- `fact_traffic_reading`: {R['fact_traffic_reading']}",
           f"- `fact_weather_hourly` theo nhóm điều kiện (số ô-giờ): {R['fact_weather_hourly_by_category']}",
           "- Kiểm tra date_key/time_key theo giờ New York (ô key 1) – chú ý 02/11 có 2 giờ UTC cùng time_key = 1:", "",
           "| interval_start_utc | date_key | time_key |", "|---|---|---|"] + [f"| {a} | {b} | {c} |" for a, b, c in R["date_key_check_local_vs_utc"]]
    md += ["", "Ghi chú triển khai: `fact_traffic_reading` phân vùng theo tháng của `reading_ts_local` (tháng file nguồn), vì `reading_ts_utc` để trống ở giờ DST kép (làm rõ D-16)."]
    (REPORTS / "phase4_dw_atomic_report.md").write_text("\n".join(md), encoding="utf-8")
    ok = all(C.values()); log.info("PHASE 4 %s %s", "OK" if ok else "CO LOI", {k: v for k, v in C.items() if not v})
    return ok
