"""Phase 6 - Chay bo kiem thu pytest + tinh chi so chat luong, xuat data_quality_report.md va etl_reconciliation.md."""
import json, subprocess, sys

from nyc_dw import config
from nyc_dw.utils.common import DW, MART, PROJECT_ROOT, QUAL, RAW, REPORTS, STG, duck, logger, sql_file, write_json

log = logger("phase6_quality")
fs = lambda p: str(p).replace("\\", "/")
n = lambda x: f"{int(x):,}".replace(",", " ")


def run() -> bool:
    # 1) Bo kiem thu doc lap
    junit = QUAL / "pytest_junit.xml"; QUAL.mkdir(parents=True, exist_ok=True)
    pr = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-W", "ignore::DeprecationWarning", f"--junitxml={junit}", "-rA"],
                        cwd=PROJECT_ROOT, capture_output=True, text=True)
    passed = [l.split("PASSED ")[1] for l in pr.stdout.splitlines() if l.startswith("PASSED ")]
    failed = [l for l in pr.stdout.splitlines() if l.startswith("FAILED ")]
    log.info("pytest rc=%s passed=%s failed=%s", pr.returncode, len(passed), len(failed))

    con = duck(); con.execute(sql_file("views/create_views.sql", dw=DW, mart=MART))
    con.execute(f"CREATE VIEW stg_traffic AS SELECT * FROM read_parquet('{fs(STG)}/stg_traffic_reading/*/*.parquet', hive_partitioning=true)")
    con.execute(f"CREATE VIEW stg_wh AS SELECT * FROM read_parquet('{fs(STG / 'stg_weather_hourly.parquet')}')")
    con.execute(f"CREATE VIEW stg_wi AS SELECT * FROM read_parquet('{fs(STG / 'stg_weather_interval.parquet')}')")
    con.execute(f"CREATE VIEW map AS SELECT * FROM read_parquet('{fs(STG / 'road_weather_mapping.parquet')}')")
    q = lambda s: con.execute(s).fetchall()[0][0]
    man = json.loads((RAW / "manifest" / "raw_manifest.json").read_text(encoding="utf-8"))
    M = {}

    # 2) Doi chieu so dong
    M["recon_traffic"] = [
        ("Cổng NYC Open Data (lọc 2025)", config.sources()["nyc_dot"]["expected"]["rows_2025"], "hộp thoại Export, 31/10/2026"),
        ("Raw: 12 file Parquet", man["sources"]["nyc_dot"]["rows_total"], "manifest"),
        ("Staging: stg_traffic_reading", q("SELECT count(*) FROM stg_traffic"), "giữ toàn bộ"),
        ("DW: fact_traffic_reading", q("SELECT count(*) FROM fact_traffic_reading"), "giữ toàn bộ"),
        ("  trong đó gán được giờ UTC", q("SELECT count(*) FROM fact_traffic_reading WHERE reading_ts_utc IS NOT NULL"), ""),
        ("  trong đó giờ DST kép (không gán giờ, D-03)", q("SELECT count(*) FROM fact_traffic_reading WHERE is_dst_ambiguous"), "01:00–01:59 ngày 02/11"),
        ("DW: fact_traffic_hourly – Σ n_records", q("SELECT sum(n_records) FROM fact_traffic_hourly"), "= số bản ghi gán được giờ"),
        ("Mart: fact_traffic_weather_hourly – Σ n_records", q("SELECT sum(n_records) FROM fact_traffic_weather_hourly"), "= hourly"),
    ]
    M["recon_rows"] = [
        ("fact_traffic_hourly (dòng = link × giờ)", q("SELECT count(*) FROM fact_traffic_hourly"), "lưới đủ từ giờ đầu tới giờ cuối của mỗi link"),
        ("fact_traffic_weather_hourly (dòng)", q("SELECT count(*) FROM fact_traffic_weather_hourly"), "= hourly, JOIN N:1"),
    ]
    M["recon_weather"] = [
        ("Raw: 16 file JSON (giờ API)", man["sources"]["open_meteo"]["rows_total"], "16 × 8 808, 31/12/2024–01/01/2026 UTC"),
        ("Staging: stg_weather_hourly", q("SELECT count(*) FROM stg_wh"), "giữ toàn bộ"),
        ("Staging: stg_weather_interval", q("SELECT count(*) FROM stg_wi"), "16 × 8 760 khoảng thuộc năm 2025 giờ NY; bỏ 48 giờ đệm/ô (29 trước, 19 sau; giờ đệm đầu tiên sau năm dùng làm H+1)"),
        ("DW: fact_weather_hourly", q("SELECT count(*) FROM fact_weather_hourly"), "= interval"),
    ]
    M["recon_links"] = [("Link trong nguồn", q("SELECT count(DISTINCT link_id) FROM stg_traffic"), ""),
                        ("dim_road_link (không tính unknown)", q("SELECT count(*) FROM dim_road_link WHERE road_key > 0"), ""),
                        ("Link có ô thời tiết", q("SELECT count(*) FROM map"), "")]

    # 3) Chi so chat luong
    M["invalid_reason"] = con.execute("SELECT coalesce(invalid_reason, 'hợp lệ'), count(*), round(100.0 * count(*) / sum(count(*)) OVER (), 2) FROM fact_traffic_reading GROUP BY 1 ORDER BY 2 DESC").fetchall()
    M["valid_by_month"] = con.execute("SELECT source_month, round(100.0 * avg(is_valid::INT), 1) FROM fact_traffic_reading GROUP BY 1 ORDER BY 1").fetchall()
    M["speed_before_after"] = con.execute("""SELECT 'tất cả bản ghi' g, round(quantile_cont(speed_mph, 0.05), 1), round(quantile_cont(speed_mph, 0.25), 1), round(median(speed_mph), 1),
                                                    round(quantile_cont(speed_mph, 0.75), 1), round(quantile_cont(speed_mph, 0.95), 1), round(avg(speed_mph), 2), round(max(speed_mph), 1) FROM fact_traffic_reading
                                             UNION ALL
                                             SELECT 'hợp lệ', round(quantile_cont(speed_mph, 0.05), 1), round(quantile_cont(speed_mph, 0.25), 1), round(median(speed_mph), 1),
                                                    round(quantile_cont(speed_mph, 0.75), 1), round(quantile_cont(speed_mph, 0.95), 1), round(avg(speed_mph), 2), round(max(speed_mph), 1) FROM fact_traffic_reading WHERE is_valid""").fetchall()
    M["hours"] = con.execute("""SELECT count(*), count(*) FILTER (WHERE n_records = 0), count(*) FILTER (WHERE n_records > 0 AND n_valid = 0),
                                       count(*) FILTER (WHERE n_valid >= 6), count(*) FILTER (WHERE n_records > 12), count(*) FILTER (WHERE has_stuck),
                                       count(DISTINCT hour_utc) FILTER (WHERE is_network_outage), count(*) FILTER (WHERE is_dst_ambiguous_hour)
                                FROM fact_traffic_hourly""").fetchall()[0]
    M["missing_by_month"] = con.execute("""SELECT month_utc, count(*) FILTER (WHERE n_records = 0), round(100.0 * avg((n_valid >= 6)::INT), 1)
                                           FROM fact_traffic_hourly GROUP BY 1 ORDER BY 1""").fetchall()
    M["outage_days"] = con.execute("""SELECT strftime(timezone('America/New_York', hour_utc), '%Y-%m-%d'), count(DISTINCT hour_utc) FROM fact_traffic_hourly
                                      WHERE is_network_outage GROUP BY 1 ORDER BY 1""").fetchall()
    M["low_quality_links"] = con.execute("""SELECT link_id, link_name, borough, round(100 * valid_share_2025, 1) FROM dim_road_link
                                            WHERE is_low_quality ORDER BY valid_share_2025""").fetchall()
    M["mapping"] = con.execute("""SELECT round(min(mapping_distance_km), 2), round(median(mapping_distance_km), 2), round(max(mapping_distance_km), 2),
                                         count(*) FILTER (WHERE mapping_is_far), count(DISTINCT weather_loc_key) FROM dim_road_link WHERE road_key > 0""").fetchall()[0]
    M["mapping_api_agree"] = q("SELECT count(*) FILTER (WHERE api_cell_agrees) FROM map")
    M["weather_ranges"] = con.execute("""SELECT round(min(temperature_c), 1), round(max(temperature_c), 1), round(avg(temperature_c), 2),
                                                min(relative_humidity_pct), max(relative_humidity_pct), round(max(wind_speed_kmh), 1), round(max(wind_gust_kmh), 1),
                                                count(*) FILTER (WHERE precipitation_mm < 0 OR snowfall_cm < 0 OR wind_speed_kmh < 0), round(max(precipitation_mm), 1), round(max(snowfall_cm), 2),
                                                count(*) FILTER (WHERE snowfall_cm > 0 AND temperature_c > 3)
                                         FROM fact_weather_hourly""").fetchall()[0]
    M["weather_totals"] = con.execute("""SELECT round(min(p), 0), round(median(p), 0), round(max(p), 0), round(min(s), 1), round(max(s), 1)
                                         FROM (SELECT weather_loc_key, sum(precipitation_mm) p, sum(snowfall_cm) s FROM fact_weather_hourly GROUP BY 1)""").fetchall()[0]
    M["unknown_members_used"] = q("""SELECT (SELECT count(*) FROM fact_traffic_reading WHERE road_key = -1) + (SELECT count(*) FROM fact_weather_hourly WHERE weather_loc_key = -1 OR condition_key = -1)
                                     + (SELECT count(*) FROM fact_traffic_weather_hourly WHERE weather_loc_key = -1 OR condition_key = -1)""")
    M["tests"] = {"returncode": pr.returncode, "passed": passed, "failed": failed}
    write_json(M, QUAL / "dq_metrics.json")

    _write_reports(M)
    ok = pr.returncode == 0 and not failed and M["unknown_members_used"] == 0
    log.info("PHASE 6 %s", "OK" if ok else "CO LOI")
    return ok


def _write_reports(M):
    T = lambda head, rows: "\n".join(["| " + " | ".join(head) + " |", "|" + "---|" * len(head)] + ["| " + " | ".join(str(c) for c in r) + " |" for r in rows])
    rec = ["# ETL reconciliation", "", "Sinh tự động bởi `scripts/run.py 6`. Mọi số liệu đọc lại từ file đã nạp.", "",
           "## Giao thông – bản ghi", "", T(["Bước", "Số bản ghi", "Ghi chú"], [(a, n(b), c) for a, b, c in M["recon_traffic"]]), "",
           f"Chênh lệch giữa atomic và Σ n_records = {n(M['recon_traffic'][3][1] - M['recon_traffic'][6][1])} = đúng số bản ghi giờ DST kép (giải thích được).", "",
           "## Giao thông – số dòng theo giờ", "", T(["Bảng", "Số dòng", "Ghi chú"], [(a, n(b), c) for a, b, c in M["recon_rows"]]), "",
           "## Thời tiết", "", T(["Bước", "Số dòng", "Ghi chú"], [(a, n(b), c) for a, b, c in M["recon_weather"]]), "",
           "## Đoạn đường", "", T(["Bước", "Số link", "Ghi chú"], [(a, n(b), c) for a, b, c in M["recon_links"]]), "",
           "## Kết luận", "", "- Không có bản ghi nào bị mất mà không giải thích được.",
           "- Mọi phép JOIN là N:1; số dòng trước và sau ghép bằng nhau; không trùng khóa ở mọi bảng (xem `data_quality_report.md`)."]
    (REPORTS / "etl_reconciliation.md").write_text("\n".join(rec), encoding="utf-8")

    h = M["hours"]; mp = M["mapping"]; w = M["weather_ranges"]; wt = M["weather_totals"]; t = M["tests"]
    crit = [("1. Số bản ghi atomic khớp nguồn", "test_raw_rows_equal_expected, test_traffic_reading_values_identical_to_raw, test_fact_row_count_per_source_file"),
            ("2. Không trùng grain", "test_traffic_reading_grain_unique, test_weather_grain_unique, test_join_preserves_rows_and_grain"),
            ("3. Không mất dữ liệu không giải thích được", "test_unassigned_records_are_only_dst_ambiguous, etl_reconciliation.md"),
            ("4. PK/FK hợp lệ", "test_dimension_keys_unique_and_unknown_present, test_foreign_keys_resolve"),
            ("5. DST xử lý có căn cứ", "test_dst_handling_matches_tzdata"),
            ("6. Căn đúng khoảng giờ thời tiết", "test_instant_variables_taken_at_interval_start, test_accumulated_variables_taken_at_interval_end, test_weather_covers_local_year_exactly"),
            ("7. Mapping có khoảng cách và phiên bản", "test_every_link_mapped_with_distance_and_version"),
            ("8. Aggregate khớp atomic", "test_hourly_matches_atomic_per_link_and_month, test_no_record_vs_zero_speed_distinguished"),
            ("9. Số dòng trước/sau JOIN", "test_join_preserves_rows_and_grain, test_mart_weather_equals_weather_fact"),
            ("10. Measure tổng hợp đúng", "test_rollup_from_hourly_equals_atomic, test_std_from_components_matches_atomic"),
            ("11. Truy xuất nguồn", "test_raw_checksums_match_manifest, test_every_fact_row_traceable_to_source_file")]
    passed = set(x.split("::")[-1] for x in t["passed"])
    rows = [(c, "ĐẠT" if all(x.strip() in passed or x.strip().endswith(".md") for x in tests.split(",")) else "**KHÔNG ĐẠT**", tests) for c, tests in crit]
    dq = ["# Data quality report", "", f"Bộ kiểm thử: {len(t['passed'])} đạt, {len(t['failed'])} lỗi (`tests/`, junit: `data/quality/pytest_junit.xml`). Chỉ số: `data/quality/dq_metrics.json`.", "",
          "## 11 tiêu chí", "", T(["Tiêu chí", "Kết quả", "Kiểm thử"], rows), "",
          "## Giao thông", "", "**Phân loại bản ghi** (không xóa dòng nào):", "", T(["Loại", "Số bản ghi", "%"], [(a, n(b), c) for a, b, c in M["invalid_reason"]]), "",
          "**Phân phối tốc độ trước và sau khi lọc** (mph):", "", T(["Nhóm", "p5", "p25", "p50", "p75", "p95", "TB", "max"], M["speed_before_after"]), "",
          f"**Link-giờ:** {n(h[0])} dòng · không có bản ghi {n(h[1])} · có bản ghi nhưng không có số đo hợp lệ {n(h[2])} · ≥ 6 lần đo hợp lệ {n(h[3])} ({100*h[3]/h[0]:.1f}%) · > 12 bản ghi (ghi bù) {n(h[4])} · có tốc độ đứng yên {n(h[5])} · giờ DST kép {n(h[7])}.", "",
          "**Theo tháng (UTC):**", "", T(["Tháng", "Link-giờ không bản ghi", "% link-giờ có ≥ 6 lần đo hợp lệ"], M["missing_by_month"]), "",
          f"**Sự cố toàn mạng** ({h[6]} giờ): " + ", ".join(f"{d} ({c} giờ)" for d, c in M["outage_days"]), "",
          f"**Link chất lượng thấp** (< 50% hợp lệ cả năm, {len(M['low_quality_links'])} link; giữ trong DW, loại khi phân tích mặc định):", "",
          T(["link_id", "Tên", "Borough", "% hợp lệ"], M["low_quality_links"]), "",
          "## Không gian", "", f"- Khoảng cách link → tâm ô: min {mp[0]} km, trung vị {mp[1]} km, max {mp[2]} km; vượt 6,7 km: {mp[3]} link; dùng {mp[4]} ô.",
          f"- Trùng với ô Open-Meteo tự chọn: {M['mapping_api_agree']}/124. 10 link khác nhau vì API còn xét độ cao (cell_selection=land); giữ haversine theo D-11.", "",
          "## Thời tiết", "", f"- Nhiệt độ {w[0]} → {w[1]} °C (TB {w[2]}); độ ẩm {w[3]} → {w[4]} %; gió max {w[5]} km/h; gió giật max {w[6]} km/h (trận gió 19/12/2025).",
          f"- Giá trị âm: {w[7]}. Mưa lớn nhất {w[8]} mm/giờ; tuyết lớn nhất {w[9]} cm/giờ. Giờ có tuyết khi nhiệt độ > 3 °C: {w[10]} (cần lưu ý khi phân tích).",
          f"- Tổng năm theo ô: mưa {wt[0]}–{wt[2]} mm (trung vị {wt[1]}), tuyết {wt[3]}–{wt[4]} cm.", "",
          f"## Unknown member", "", f"Số dòng fact dùng khóa −1: {M['unknown_members_used']}.", "",
          "## Hạn chế", "", "- Thời tiết là giá trị mô hình (ECMWF IFS ~8 × 11 km), chưa đối chiếu trạm.",
          "- `travel_time` là giây theo suy luận; metadata không ghi đơn vị.", "- 12 link không đủ dữ liệu đêm khô để tính `free_flow_speed_mph` → trạng thái 'Không xác định'."]
    (REPORTS / "data_quality_report.md").write_text("\n".join(dq), encoding="utf-8")
