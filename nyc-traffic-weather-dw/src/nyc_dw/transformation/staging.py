"""Phase 3 - Staging: traffic (giu toan bo + co), road link (lam sach hinh hoc), mapping link -> o, weather (khoang gio UTC)."""
import datetime as dt, json, urllib.parse, urllib.request
from collections import Counter
from zoneinfo import ZoneInfo

import pandas as pd

from nyc_dw import config
from nyc_dw.transformation.geometry import clean_points, haversine_km
from nyc_dw.utils.common import RAW, REPORTS, STG, atomic_output, batch_id, duck, logger, sql_file, write_json

log = logger("phase3_staging")
NY = ZoneInfo("America/New_York")


def dst_hours(year: int) -> dict:
    """Tim gio dia phuong mo ho (lui gio) va khong ton tai (tien gio) tu tzdata."""
    amb = nonex = None
    d = dt.datetime(year, 1, 1)
    while d.year == year:
        a0, a1 = d.replace(fold=0).utcoffset() if False else d.replace(tzinfo=NY, fold=0).utcoffset(), d.replace(tzinfo=NY, fold=1).utcoffset()
        if a0 != a1:
            back = d.replace(tzinfo=NY, fold=0).astimezone(dt.timezone.utc).astimezone(NY).replace(tzinfo=None)
            if back != d and nonex is None:
                nonex = d
            elif back == d and amb is None:
                amb = d
        d += dt.timedelta(hours=1)
    return {"amb_date": amb.date().isoformat(), "amb_hour": amb.hour, "nonexist_date": nonex.date().isoformat(), "nonexist_hour": nonex.hour}


def _api_cells(points: list[tuple[float, float]], model: str) -> list | None:
    """1 request nho (1 ngay) de biet o Open-Meteo tu chon cho moi diem dai dien (cell_selection mac dinh = land)."""
    q = {"latitude": ",".join(f"{a:.5f}" for a, _ in points), "longitude": ",".join(f"{b:.5f}" for _, b in points),
         "start_date": "2025-07-01", "end_date": "2025-07-01", "hourly": "temperature_2m", "timezone": "GMT", "models": model}
    url = "https://archive-api.open-meteo.com/v1/archive?" + urllib.parse.urlencode(q, safe=",")
    try:
        b = json.loads(urllib.request.urlopen(url, timeout=120).read())
        return [(x["latitude"], x["longitude"]) for x in (b if isinstance(b, list) else [b])]
    except Exception as e:
        log.warning("Khong goi duoc API de doi chieu o: %s", e); return None


def run() -> bool:
    bid = batch_id("phase3"); log.info("batch %s", bid)
    rules = config.load("quality_rules"); src = config.sources(); model = src["open_meteo"]["model"]
    STG.mkdir(parents=True, exist_ok=True)
    con = duck()
    R = {"batch_id": bid}

    # ---------- 1. Traffic ----------
    dh = dst_hours(2025); R["dst_hours"] = dh; log.info("DST %s", dh)
    con.execute(sql_file("staging/stg_traffic_reading.sql", raw_glob=RAW / "nyc_dot" / "2025-*.parquet", batch_id=bid,
                         speed_max=rules["traffic"]["speed_valid"]["max_inclusive"], stuck_min=rules["traffic"]["stuck_min_consecutive"], **dh))
    with atomic_output(STG / "stg_traffic_reading") as tmp:
        con.execute(f"COPY (SELECT * FROM stg_traffic_reading ORDER BY source_month, link_id, reading_ts_local) TO '{str(tmp).replace(chr(92), '/')}' "
                    "(FORMAT parquet, COMPRESSION zstd, PARTITION_BY (source_month))")
    t = con.sql("""SELECT count(*) n, count(DISTINCT (link_id, reading_ts_local)) n_bk,
                          sum(is_dst_ambiguous::INT) amb, sum(is_dst_nonexistent::INT) nonex,
                          sum((reading_ts_utc IS NULL)::INT) utc_null, sum((invalid_reason IS NULL)::INT) valid,
                          sum(is_stuck::INT) stuck_rows, count(DISTINCT link_id) links,
                          min(utc_offset_min) off_min, max(utc_offset_min) off_max
                   FROM stg_traffic_reading""").df().iloc[0].to_dict()
    R["traffic"] = {k: int(v) for k, v in t.items()}
    R["traffic_invalid_reason"] = dict(con.sql("SELECT coalesce(invalid_reason,'VALID'), count(*) FROM stg_traffic_reading GROUP BY 1 ORDER BY 2 DESC").fetchall())
    R["traffic_offset_by_month"] = {m: [a, b] for m, a, b in con.sql(
        "SELECT source_month, min(utc_offset_min), max(utc_offset_min) FROM stg_traffic_reading GROUP BY 1 ORDER BY 1").fetchall()}
    # kiem tra doi gio: 2025-03-09 01:58 (EST) -> 06:58Z ; 03:03 (EDT) -> 07:03Z
    R["dst_spot_check"] = con.sql("""SELECT reading_ts_local, reading_ts_utc, utc_offset_min FROM stg_traffic_reading
                                    WHERE link_id = (SELECT min(link_id) FROM stg_traffic_reading)
                                      AND reading_ts_local BETWEEN '2025-03-09 01:50' AND '2025-03-09 03:10' ORDER BY 1""").df().astype(str).values.tolist()
    log.info("traffic %s", R["traffic"])

    # ---------- 2. Road link ----------
    attrs = con.sql(f"""SELECT link_id, id, link_points, link_name, borough, owner, count(*) n, min(data_as_of) mn, max(data_as_of) mx
                        FROM read_parquet('{str(RAW / 'nyc_dot' / '2025-*.parquet').replace(chr(92), '/')}')
                        GROUP BY ALL""").df()
    vs = con.sql("SELECT link_id, avg((invalid_reason IS NULL)::DOUBLE) valid_share, min(reading_ts_utc) fs, max(reading_ts_utc) ls FROM stg_traffic_reading GROUP BY 1").df()
    rows = []
    for lid, g in attrs.groupby("link_id"):
        g = g.sort_values("n", ascending=False)
        names = Counter(); [names.update({nm: int(c)}) for nm, c in g.groupby("link_name").n.sum().items()]
        clean_names = [nm for nm, _ in names.most_common() if "\\" not in nm]
        geo_variants = g.link_points.unique(); bor = g.borough.unique(); own = g.owner.unique(); ids = g.id.unique()
        cp = clean_points(geo_variants[0], rules["geometry"]["bbox"], rules["geometry"]["max_point_deviation_km"])
        rows.append({"link_id": lid, "source_internal_id": ids[0], "n_source_internal_id": len(ids),
                     "link_name": clean_names[0] if clean_names else names.most_common(1)[0][0],
                     "link_name_variants": json.dumps(list(names), ensure_ascii=False), "n_link_name_corrupted": sum("\\" in nm for nm in names),
                     "borough": bor[0], "n_borough": len(bor), "owner": own[0], "n_owner": len(own),
                     "link_points_raw": geo_variants[0], "n_geometry_versions": len(geo_variants),
                     **{k: v for k, v in cp.items() if k not in ("start", "end", "bad_tokens")},
                     "bad_tokens": json.dumps(cp["bad_tokens"]), "start_lat": cp["start"][0], "start_lon": cp["start"][1],
                     "end_lat": cp["end"][0], "end_lon": cp["end"][1],
                     "first_seen_local": g.mn.min(), "last_seen_local": g.mx.max(), "n_readings": int(g.n.sum())})
    L = pd.DataFrame(rows).merge(vs.rename(columns={"fs": "first_seen_utc", "ls": "last_seen_utc"}), on="link_id")
    dup = L.groupby("link_points_raw").link_id.apply(list)
    L["shares_geometry_with"] = L.link_points_raw.map(lambda p: ",".join(x for x in dup[p]) if len(dup[p]) > 1 else None)
    L["shares_geometry_with"] = [None if s is None else ",".join(x for x in s.split(",") if x != lid) for s, lid in zip(L.shares_geometry_with, L.link_id)]
    L["load_batch_id"] = bid

    # ---------- 3. Mapping link -> o ----------
    cells = []
    for f in sorted((RAW / "open_meteo" / model).glob("ifs_*.json")):
        b = json.loads(f.read_text(encoding="utf-8")); cells.append({"weather_location_id": f.stem, "grid_lat": b["latitude"], "grid_lon": b["longitude"], "elevation": b.get("elevation")})
    C = pd.DataFrame(cells)
    def nearest(la, lo):
        d = [(haversine_km(la, lo, c.grid_lat, c.grid_lon), c.weather_location_id) for c in C.itertuples()]
        return min(d)
    mp = []
    api = _api_cells(list(zip(L.rep_lat, L.rep_lon)), model)
    cell_by_coord = {(round(c.grid_lat, 4), round(c.grid_lon, 4)): c.weather_location_id for c in C.itertuples()}
    for i, r in enumerate(L.itertuples()):
        dist, cid = nearest(r.rep_lat, r.rep_lon)
        touched = {cid, nearest(r.start_lat, r.start_lon)[1], nearest(r.end_lat, r.end_lon)[1]}
        api_cid = cell_by_coord.get((round(api[i][0], 4), round(api[i][1], 4)), f"OUTSIDE_SET:{api[i][0]},{api[i][1]}") if api else None
        mp.append({"link_id": r.link_id, "weather_location_id": cid, "distance_km": round(dist, 3), "cells_touched": len(touched),
                   "api_weather_location_id": api_cid, "api_cell_agrees": (api_cid == cid) if api else None,
                   "is_far": dist > rules["mapping"]["far_flag_km"], "mapping_method": "nearest_land_cell_haversine_median_point",
                   "mapping_version": "v1", "load_batch_id": bid})
    M = pd.DataFrame(mp)
    with atomic_output(STG / "stg_road_link.parquet") as tmp:
        L.to_parquet(tmp, index=False, compression="zstd")
    with atomic_output(STG / "road_weather_mapping.parquet") as tmp:
        M.to_parquet(tmp, index=False, compression="zstd")
    R["road_link"] = {"links": len(L), "with_rep_point": int(L.rep_lat.notna().sum()), "links_with_bad_tokens": int((L.n_tokens_bad > 0).sum()),
                      "bad_tokens_total": int(L.n_tokens_bad.sum()), "points_far_dropped": int(L.n_points_far_dropped.sum()),
                      "links_far_dropped": int((L.n_points_far_dropped > 0).sum()), "geometry_versions_max": int(L.n_geometry_versions.max()),
                      "borough_versions_max": int(L.n_borough.max()), "links_name_corrupted": int((L.n_link_name_corrupted > 0).sum()),
                      "links_sharing_geometry": L.loc[L.shares_geometry_with.notna(), ["link_id", "shares_geometry_with", "link_name"]].values.tolist(),
                      "length_km_valid": L.length_km_valid.describe().round(3).to_dict(),
                      "longest": L.nlargest(5, "length_km_valid")[["link_id", "link_name", "length_km_valid"]].values.tolist()}
    R["mapping"] = {"links_mapped": int(M.weather_location_id.notna().sum()), "cells_used": int(M.weather_location_id.nunique()),
                    "distance_km": M.distance_km.describe().round(2).to_dict(), "far": int(M.is_far.sum()),
                    "far_links": M.loc[M.is_far, ["link_id", "distance_km"]].values.tolist(),
                    "cells_touched": {int(k): int(v) for k, v in M.cells_touched.value_counts().sort_index().items()},
                    "api_checked": api is not None, "api_agree": int(M.api_cell_agrees.sum()) if api else None,
                    "api_disagree": M.loc[M.api_cell_agrees == False, ["link_id", "weather_location_id", "api_weather_location_id", "distance_km"]].values.tolist() if api else None,
                    "api_outside_set": int(M.api_weather_location_id.astype(str).str.startswith("OUTSIDE").sum()) if api else None}
    log.info("road_link %s", {k: R["road_link"][k] for k in ("links", "links_with_bad_tokens", "points_far_dropped")}); log.info("mapping %s", R["mapping"]["cells_touched"])

    # ---------- 4. Weather ----------
    frames = []
    dl = {m["weather_location_id"]: m["downloaded_utc"] for m in json.loads((RAW / "open_meteo" / model / "_manifest.json").read_text(encoding="utf-8"))}
    for f in sorted((RAW / "open_meteo" / model).glob("ifs_*.json")):
        b = json.loads(f.read_text(encoding="utf-8")); h = pd.DataFrame(b["hourly"])
        h["time_utc"] = pd.to_datetime(h.pop("time")).dt.tz_localize("UTC")
        h["weather_location_id"] = f.stem; h["model"] = model; h["grid_lat"] = b["latitude"]; h["grid_lon"] = b["longitude"]
        h["elevation"] = b.get("elevation"); h["source_file"] = f.name; h["api_downloaded_utc"] = dl[f.stem]
        frames.append(h)
    con.register("_wx_long", pd.concat(frames, ignore_index=True))
    s_utc = dt.datetime(2025, 1, 1, tzinfo=NY).astimezone(dt.timezone.utc); e_utc = dt.datetime(2026, 1, 1, tzinfo=NY).astimezone(dt.timezone.utc)
    con.execute(sql_file("staging/stg_weather.sql", start_utc=s_utc.strftime("%Y-%m-%d %H:%M:%S+00"), end_utc_exclusive=e_utc.strftime("%Y-%m-%d %H:%M:%S+00"), batch_id=bid))
    for name in ("stg_weather_hourly", "stg_weather_interval"):
        with atomic_output(STG / f"{name}.parquet") as tmp:
            con.execute(f"COPY (SELECT * FROM {name} ORDER BY 1, 2) TO '{str(tmp).replace(chr(92), '/')}' (FORMAT parquet, COMPRESSION zstd)")
    w = con.sql("""SELECT count(*) n, count(DISTINCT weather_location_id) cells, count(DISTINCT (weather_location_id, interval_start_utc)) n_key,
                          min(interval_start_utc) s, max(interval_start_utc) e, sum((NOT has_next_hour)::INT) no_next,
                          sum((temperature_c IS NULL OR precipitation_mm IS NULL OR snowfall_cm IS NULL OR weather_code IS NULL
                               OR wind_speed_kmh IS NULL OR wind_gust_kmh IS NULL OR relative_humidity_pct IS NULL)::INT) nulls
                   FROM stg_weather_interval""").df().iloc[0].to_dict()
    R["weather_interval"] = {k: (str(v) if k in ("s", "e") else int(v)) for k, v in w.items()}
    R["weather_hourly_rows"] = con.execute("SELECT count(*) FROM stg_weather_hourly").fetchall()[0][0]
    # doi chieu voi staging cu (D:/DE_dataset_sizing) - phai khop tung gia tri
    old = src["open_meteo"]["staging_parquet"].replace("\\", "/")
    R["weather_vs_previous_staging_mismatch"] = con.execute(f"""SELECT count(*) FROM stg_weather_hourly a FULL JOIN read_parquet('{old}') b
        USING (weather_location_id, time_utc)
        WHERE a.precipitation IS DISTINCT FROM b.precipitation OR a.temperature_2m IS DISTINCT FROM b.temperature_2m
           OR a.weather_code IS DISTINCT FROM b.weather_code OR a.wind_gusts_10m IS DISTINCT FROM b.wind_gusts_10m""").fetchall()[0][0]
    log.info("weather %s", R["weather_interval"])

    # ---------- 5. Tieu chi ----------
    exp = src["nyc_dot"]["expected"]["rows_2025"]
    checks = {
        "traffic_rows_equal_raw": R["traffic"]["n"] == exp,
        "traffic_business_key_unique": R["traffic"]["n_bk"] == R["traffic"]["n"],
        "dst_ambiguous_flagged_2468": R["traffic"]["amb"] == 2468,
        "dst_nonexistent_zero": R["traffic"]["nonex"] == 0,
        "utc_null_only_dst": R["traffic"]["utc_null"] == R["traffic"]["amb"] + R["traffic"]["nonex"],
        "utc_offsets_est_edt_only": R["traffic"]["off_min"] == -300 and R["traffic"]["off_max"] == -240,
        "all_124_links_rep_point": R["road_link"]["links"] == 124 and R["road_link"]["with_rep_point"] == 124,
        "all_links_mapped": R["mapping"]["links_mapped"] == 124,
        "weather_intervals_16x8760": R["weather_interval"]["n"] == 16 * 8760 and R["weather_interval"]["n_key"] == 16 * 8760,
        "weather_no_nulls": R["weather_interval"]["nulls"] == 0 and R["weather_interval"]["no_next"] == 0,
        "weather_equals_previous_staging": R["weather_vs_previous_staging_mismatch"] == 0,
    }
    R["checks"] = checks
    write_json(R, REPORTS / "phase3_staging.json")
    _report(R)
    ok = all(checks.values()); log.info("PHASE 3 %s %s", "OK" if ok else "CO LOI", {k: v for k, v in checks.items() if not v})
    return ok


def _report(R):
    n = lambda x: f"{x:,}".replace(",", " ")
    t, rl, mp, w = R["traffic"], R["road_link"], R["mapping"], R["weather_interval"]
    L = [f"# Phase 3 – Staging", "", f"Batch `{R['batch_id']}`. Chi tiết: `reports/phase3_staging.json`.", "",
         "## Tiêu chí", "", "| Tiêu chí | Kết quả |", "|---|---|"] + [f"| {k} | {'ĐẠT' if v else '**KHÔNG ĐẠT**'} |" for k, v in R["checks"].items()]
    L += ["", "## Traffic (`data/staging/stg_traffic_reading/`, phân vùng theo tháng nguồn)", "",
          f"- {n(t['n'])} dòng, {t['links']} link; khóa (`link_id`, `reading_ts_local`) duy nhất: {n(t['n_bk'])}.",
          f"- Mốc DST tính từ tzdata: giờ kép {R['dst_hours']['amb_date']} {R['dst_hours']['amb_hour']:02d}:xx → {t['amb']} bản ghi (`reading_ts_utc` NULL); giờ không tồn tại {R['dst_hours']['nonexist_date']} {R['dst_hours']['nonexist_hour']:02d}:xx → {t['nonex']} bản ghi.",
          f"- `utc_offset_min` chỉ nhận {t['off_min']} (EST) và {t['off_max']} (EDT). Theo tháng: {R['traffic_offset_by_month']}.",
          f"- Phân loại: {R['traffic_invalid_reason']}.", f"- Bản ghi thuộc chuỗi tốc độ đứng yên (≥ 12 lần): {n(t['stuck_rows'])}.",
          "- Kiểm tra đổi giờ mùa xuân (1 link):", "", "| local | UTC | offset |", "|---|---|---|"] + [f"| {a} | {b} | {c} |" for a, b, c in R["dst_spot_check"]]
    L += ["", "## Road link (`stg_road_link.parquet`)", "",
          f"- {rl['links']} link, tất cả có điểm đại diện. {rl['links_with_bad_tokens']} link có token lỗi ({rl['bad_tokens_total']} token); {rl['points_far_dropped']} điểm bị loại vì cách trung vị > 3 km ({rl['links_far_dropped']} link).",
          f"- Mỗi link: tối đa {rl['geometry_versions_max']} phiên bản hình học, {rl['borough_versions_max']} borough; {rl['links_name_corrupted']} link có tên lỗi (đã chọn tên sạch phổ biến nhất).",
          f"- Link dùng chung hình học: {rl['links_sharing_geometry']}.",
          f"- Chiều dài sau làm sạch (km): {rl['length_km_valid']}. Dài nhất: {rl['longest']}."]
    L += ["", "## Mapping link → ô (`road_weather_mapping.parquet`)", "",
          f"- {mp['links_mapped']}/124 link được gán, dùng {mp['cells_used']}/16 ô. Khoảng cách (km): {mp['distance_km']}.",
          f"- Xa hơn 6,7 km: {mp['far']} {mp['far_links']}. Số ô chạm (đại diện/đầu/cuối): {mp['cells_touched']}.",
          f"- Đối chiếu với ô Open-Meteo tự chọn (cell_selection=land): kiểm tra = {mp['api_checked']}, trùng {mp['api_agree']}/124, ô ngoài tập 16: {mp['api_outside_set']}.",
          f"- Khác nhau: {mp['api_disagree']}"]
    L += ["", "## Weather (`stg_weather_hourly.parquet`, `stg_weather_interval.parquet`)", "",
          f"- Theo giờ API: {n(R['weather_hourly_rows'])} dòng; khớp từng giá trị với staging trước đó (lệch: {R['weather_vs_previous_staging_mismatch']}).",
          f"- Khoảng `[H, H+1)`: {n(w['n'])} dòng, {w['cells']} ô, từ {w['s']} đến {w['e']}; null: {w['nulls']}; thiếu giờ H+1: {w['no_next']}.",
          "- Căn giờ: nhiệt độ, độ ẩm, gió, gió giật @H; mưa, tuyết, weather_code @H+1 (D-09)."]
    (REPORTS / "phase3_staging_report.md").write_text("\n".join(L), encoding="utf-8")
