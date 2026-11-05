"""Phase 7 - Chay cac truy van OLAP trong sql/olap tren file DuckDB (view), luu CSV va bao cao Markdown."""
from nyc_dw.utils.common import DATA, REPORTS, SQL, duck, logger

log = logger("phase7_olap")
SHOW = {"q1": 60, "q2": 30, "q3": 10, "q4": 15, "q5": 40, "q6": 12, "q7": 20, "q8": 10}


def _md(df, k):
    d = df.head(k)
    head = "| " + " | ".join(map(str, d.columns)) + " |\n|" + "---|" * len(d.columns)
    rows = ["| " + " | ".join("" if v is None or (isinstance(v, float) and v != v) else str(v) for v in r) + " |" for r in d.itertuples(index=False)]
    return head + "\n" + "\n".join(rows)


def run() -> bool:
    con = duck(str(DATA / "nyc_dw.duckdb"))
    out = REPORTS / "olap"; out.mkdir(parents=True, exist_ok=True)
    md = ["# Phase 7 – Minh họa OLAP", "", "Chạy trên `data/nyc_dw.duckdb` (view trên Parquet). SQL: `sql/olap/`. Kết quả đầy đủ: `reports/olap/*.csv`.",
          "Mọi tốc độ trung bình đều tính `SUM(sum_speed) / SUM(n_valid)` (không dùng AVG của AVG). Ngưỡng trạng thái giao thông là quy tắc phân tích (D-18).", ""]
    checks, res = {}, {}
    for f in sorted((SQL / "olap").glob("q*.sql")):
        sql = f.read_text(encoding="utf-8"); key = f.stem[:2]
        df = con.execute(sql).df(); res[key] = df
        df.to_csv(out / f"{f.stem}.csv", index=False, encoding="utf-8-sig")
        title = sql.splitlines()[0].lstrip("- ").strip()
        md += [f"## {title}", "", "```sql", sql.strip(), "```", "", f"{len(df)} dòng kết quả (hiển thị {min(len(df), SHOW[key])}):", "", _md(df, SHOW[key]), ""]
        log.info("%s rows=%s", f.stem, len(df))
    # kiem tra tinh nhat quan cua roll-up / drill-down
    q6 = res["q6"]; q7 = res["q7"]
    checks["q6_borough_sum_equals_nyc"] = int(q6[q6.level == 1].n_valid_readings.sum()) == int(q6[q6.level == 3].n_valid_readings.iloc[0])
    checks["q6_links_sum_equals_borough"] = all(int(q6[(q6.level == 0) & (q6.borough == b)].n_valid_readings.sum()) == int(g.n_valid_readings.iloc[0])
                                                for b, g in q6[q6.level == 1].groupby("borough"))
    checks["q7_days_sum_equals_month"] = int(q7[q7.level == 1].n_valid_readings.sum()) == int(q7[q7.level == 3].n_valid_readings.iloc[0])
    checks["q7_hours_sum_equals_days"] = int(q7[q7.level == 0].n_valid_readings.sum()) == int(q7[q7.level == 1].n_valid_readings.sum())
    checks["q5_has_rows"] = len(res["q5"]) > 0
    md += ["## Kiểm tra nhất quán roll-up / drill-down", "", "| Kiểm tra | Kết quả |", "|---|---|"] + [f"| {k} | {'ĐẠT' if v else '**KHÔNG ĐẠT**'} |" for k, v in checks.items()]
    (REPORTS / "olap_results.md").write_text("\n".join(md), encoding="utf-8")
    ok = all(checks.values()); log.info("PHASE 7 %s %s", "OK" if ok else "CO LOI", checks)
    return ok
