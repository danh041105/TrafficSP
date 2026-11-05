"""Phase 0 - Kiem ke moi truong va nguon du lieu (chi doc, khong sua du lieu goc).
Chay tu thu muc du an:  python -I scripts/phase0_inventory.py
Dau ra: reports/environment_check.md, reports/source_inventory.md, reports/phase0_inventory.json
"""
import datetime as dt, glob, importlib, json, os, platform, shutil, subprocess, sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from nyc_dw import config  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")
SRC = config.sources(); RULES = config.load("quality_rules")
REP = ROOT / "reports"; REP.mkdir(exist_ok=True)
NOW = dt.datetime.now().astimezone().isoformat(timespec="seconds")
J = {"generated_at": NOW}

def md_table(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)
def n(x): return f"{x:,}".replace(",", " ")

# ================= 1. Moi truong =================
env = {"os": platform.platform(), "python": sys.version.split()[0], "python_exe": sys.executable, "packages": {}}
for m in ["pyarrow", "pandas", "numpy", "yaml", "duckdb", "pytest", "holidays", "matplotlib"]:
    try: env["packages"][m] = getattr(importlib.import_module(m), "__version__", "ok")
    except Exception: env["packages"][m] = None
try: env["git"] = subprocess.run(["git", "--version"], capture_output=True, text=True).stdout.strip()
except Exception: env["git"] = None
env["disk"] = {d: {"total_gb": round(shutil.disk_usage(d).total / 1e9, 1), "free_gb": round(shutil.disk_usage(d).free / 1e9, 1)} for d in ["C:\\", "D:\\"]}
J["environment"] = env

# ================= 2. NYC DOT =================
nd = SRC["nyc_dot"]; files = sorted(glob.glob(os.path.join(nd["local_dir"], nd["file_glob"])))
per_file, frames, schemas = [], [], set()
for f in files:
    pf = pq.ParquetFile(f); md = pf.metadata
    schemas.add(str(pf.schema_arrow.remove_metadata()))
    d = pq.read_table(f).to_pandas()
    mon = os.path.basename(f)[:7]
    in_month = d.data_as_of.dt.strftime("%Y-%m") == mon
    per_file.append({"file": os.path.basename(f), "bytes": os.path.getsize(f), "rows": md.num_rows, "row_groups": md.num_row_groups,
                     "created_by": md.created_by, "min_ts": str(d.data_as_of.min()), "max_ts": str(d.data_as_of.max()),
                     "rows_outside_file_month": int((~in_month).sum()), "links": int(d.link_id.nunique()),
                     "nulls": int(d.isna().sum().sum())})
    frames.append(d[["id", "link_id", "data_as_of", "speed", "travel_time", "status", "borough", "owner", "link_name", "link_points", "transcom_id"]])
D = pd.concat(frames, ignore_index=True)
rows_total = int(len(D))
nyc = {"dir": nd["local_dir"], "files": per_file, "n_files": len(files), "rows_total": rows_total,
       "rows_expected": nd["expected"]["rows_2025"], "rows_match_expected": rows_total == nd["expected"]["rows_2025"],
       "schema_identical_across_files": len(schemas) == 1, "schema": list(schemas)[0] if len(schemas) == 1 else list(schemas),
       "bytes_total": sum(p["bytes"] for p in per_file)}
nyc["ts_tz"] = str(D.data_as_of.dt.tz)
nyc["ts_min"], nyc["ts_max"] = str(D.data_as_of.min()), str(D.data_as_of.max())
nyc["dup_business_key_link_id_data_as_of"] = int(D.duplicated(["link_id", "data_as_of"]).sum())
nyc["dup_full_rows"] = int(D.duplicated().sum())
nyc["nulls_by_column"] = {c: int(v) for c, v in D.isna().sum().items()}
nyc["distinct"] = {c: int(D[c].nunique()) for c in ["id", "link_id", "transcom_id", "status", "borough", "owner", "link_name", "link_points"]}
nyc["id_vs_link"] = {"id_equals_link_id_rows": int((D.id == D.link_id).sum()),
                     "links_with_1_id": int((D.groupby("link_id").id.nunique() == 1).sum()),
                     "ids_with_1_link": int((D.groupby("id").link_id.nunique() == 1).sum())}
nyc["transcom_equals_link_id_rows"] = int((D.transcom_id == D.link_id).sum())
nyc["status_values"] = {k: int(v) for k, v in D.status.value_counts().items()}
nyc["borough_values"] = {k: int(v) for k, v in D.borough.value_counts().items()}
nyc["links_per_borough"] = {k: int(v) for k, v in D.groupby("borough").link_id.nunique().items()}
nyc["borough_per_link_max"] = int(D.groupby("link_id").borough.nunique().max())
nyc["link_points_versions_per_link_max"] = int(D.groupby("link_id").link_points.nunique().max())
nyc["link_name_versions_per_link_max"] = int(D.groupby("link_id").link_name.nunique().max())
nyc["speed"] = {"min": float(D.speed.min()), "max": float(D.speed.max()), "zero": int((D.speed == 0).sum()), "gt100": int((D.speed > 100).sum()), "neg": int((D.speed < 0).sum())}
nyc["travel_time"] = {"min": int(D.travel_time.min()), "max": int(D.travel_time.max()), "zero": int((D.travel_time == 0).sum()), "neg": int((D.travel_time < 0).sum())}
v = (D.speed > 0) & (D.speed <= 100) & (D.travel_time > 0); nyc["valid_share_rule_0_100_tt"] = round(float(v.mean()), 4)
for day, hrs in [("2025-03-09", (1, 2, 3)), ("2025-11-02", (0, 1, 2))]:
    s = D.data_as_of[D.data_as_of.dt.strftime("%Y-%m-%d") == day]
    nyc[f"dst_{day}"] = {f"h{h:02d}": int((s.dt.hour == h).sum()) for h in hrs}
fl = D.groupby("link_id").data_as_of.agg(["min", "max"])
nyc["links_first_seen_after_jan10"] = fl[fl["min"] > "2025-01-10"].reset_index().astype(str).values.tolist()
nyc["links_last_seen_before_dec20"] = fl[fl["max"] < "2025-12-20"].reset_index().astype(str).values.tolist()
J["nyc_dot"] = nyc
del frames

# ================= 3. Open-Meteo =================
om = SRC["open_meteo"]; raw = sorted(glob.glob(os.path.join(om["raw_json_dir"], "ifs_*.json")))
man = json.load(open(os.path.join(om["raw_json_dir"], "_manifest.json"), encoding="utf-8"))
raw_info, raw_frames = [], []
for f in raw:
    b = json.load(open(f, encoding="utf-8")); h = b["hourly"]
    raw_info.append({"file": os.path.basename(f), "bytes": os.path.getsize(f), "lat": b["latitude"], "lon": b["longitude"], "elevation": b.get("elevation"),
                     "timezone": b["timezone"], "utc_offset_seconds": b["utc_offset_seconds"], "hours": len(h["time"]),
                     "first": h["time"][0], "last": h["time"][-1], "vars": [k for k in h if k != "time"]})
    t = pd.DataFrame(h); t["weather_location_id"] = os.path.basename(f)[:-5]; raw_frames.append(t)
R = pd.concat(raw_frames, ignore_index=True)
units = b["hourly_units"]
S = pq.read_table(om["staging_parquet"]).to_pandas()
VARS = om["variables"]
S2 = S.assign(time=S.time_utc.dt.tz_convert("UTC").dt.strftime("%Y-%m-%dT%H:%M"))[["weather_location_id", "time"] + VARS]
cmp = R.merge(S2, on=["weather_location_id", "time"], suffixes=("_raw", "_stg"), how="outer", indicator=True)
mism = {v: int(((cmp[f"{v}_raw"] != cmp[f"{v}_stg"]) & ~(cmp[f"{v}_raw"].isna() & cmp[f"{v}_stg"].isna())).sum()) for v in VARS}
wx = {"raw_dir": om["raw_json_dir"], "raw_files": len(raw), "raw_bytes_total": sum(r["bytes"] for r in raw_info), "raw_files_info": raw_info,
      "raw_units": units, "manifest_entries": len(man), "manifest_all_same_cell": all(m["same_cell_returned"] for m in man),
      "manifest_downloaded_utc": sorted({m["downloaded_utc"][:10] for m in man}),
      "staging_path": om["staging_parquet"], "staging_rows": int(len(S)), "staging_schema": str(pq.read_schema(om["staging_parquet"]).remove_metadata()),
      "staging_cells": int(S.weather_location_id.nunique()), "staging_rows_per_cell": sorted(set(S.groupby("weather_location_id").size().tolist())),
      "staging_time_min": str(S.time_utc.min()), "staging_time_max": str(S.time_utc.max()),
      "staging_nulls": {c: int(x) for c, x in S.isna().sum().items()}, "staging_dup_key": int(S.duplicated(["weather_location_id", "time_utc"]).sum()),
      "raw_vs_staging_rows_only_one_side": int((cmp._merge != "both").sum()), "raw_vs_staging_value_mismatch": mism,
      "weather_code_values": sorted(S.weather_code.dropna().astype(int).unique().tolist())}
J["open_meteo"] = wx

json.dump(J, open(REP / "phase0_inventory.json", "w", encoding="utf-8"), indent=1, ensure_ascii=False, default=str)

# ================= 4. Bao cao Markdown =================
miss = [k for k, x in env["packages"].items() if x is None]
CF, CT = env["disk"]["C:\\"]["free_gb"], env["disk"]["C:\\"]["total_gb"]
DF, DT = env["disk"]["D:\\"]["free_gb"], env["disk"]["D:\\"]["total_gb"]
PKG_ROWS = md_table(["Thu vien", "Phien ban", "Vai tro"], [[k, x or "**CHUA CAI**", {"pyarrow": "doc/ghi Parquet", "pandas": "bang nho", "numpy": "tinh toan", "yaml": "doc config", "duckdb": "engine SQL cho DW (bat buoc tu Phase 2)", "pytest": "kiem thu (bat buoc tu Phase 2)", "holidays": "lich ngay le cho dim_date (co the thay bang danh sach co nguon)", "matplotlib": "so do/bao cao"}[k]] for k, x in env["packages"].items()])
envmd = f"""# Environment check (Phase 0)

Sinh tu `scripts/phase0_inventory.py` luc {NOW}.

| Hang muc | Gia tri |
|---|---|
| He dieu hanh | {env['os']} |
| Python | {env['python']} (`{env['python_exe']}`) |
| Git | {env['git']} |
| O C: | trong {CF} / {CT} GB |
| O D: | trong {DF} / {DT} GB |

## Thu vien Python

{PKG_ROWS}

## Ket luan
- Thieu: {', '.join(miss) if miss else 'khong'}. Can xac nhan truoc khi cai (`python -m pip install duckdb pytest`), dung luong cai dat nho (~50 MB) nhung o C: chi con {CF} GB.
- Du an dat tai `D:/nyc-traffic-weather-dw` (o D: con {DF} GB). Du lieu nguon giu nguyen vi tri, chi doc qua `config/data_sources.yaml`.
- Phase 0 va Phase 1 khong can DuckDB (dung PyArrow).
"""
open(REP / "environment_check.md", "w", encoding="utf-8").write(envmd)

pf_rows = [[p["file"], n(p["bytes"]), n(p["rows"]), p["row_groups"], p["min_ts"], p["max_ts"], p["rows_outside_file_month"], p["links"], p["nulls"]] for p in per_file]
srcmd = f"""# Source inventory (Phase 0)

Sinh tu `scripts/phase0_inventory.py` luc {NOW}. Moi so lieu duoi day doc truc tiep tu file; khong dung lai ket qua cu.

## 1. NYC DOT Traffic Speeds (`{nd['dataset_id']}`)

Vi tri: `{nd['local_dir']}` · {nyc['n_files']} file · {n(nyc['bytes_total'])} byte · trich xuat bang Colab (SODA API, tung ngay) ngay {nd['extraction']['run_date']}.

{md_table(["File", "Byte", "Dong", "Row groups", "data_as_of min", "data_as_of max", "Dong ngoai thang", "Link", "Null"], pf_rows)}

| Kiem tra | Ket qua | Danh gia |
|---|---|---|
| Tong so dong | {n(rows_total)} (ky vong {n(nyc['rows_expected'])}) | {'KHOP' if nyc['rows_match_expected'] else '**LECH**'} |
| Schema giong nhau giua 12 file | {nyc['schema_identical_across_files']} | |
| Mui gio gan trong `data_as_of` | {nyc['ts_tz']} (timestamp khong mui gio) | dung voi gia dinh "gio dia phuong, khong offset" |
| Khoang `data_as_of` | {nyc['ts_min']} -> {nyc['ts_max']} | |
| Trung khoa nghiep vu (`link_id`, `data_as_of`) | {nyc['dup_business_key_link_id_data_as_of']} | khoa duy nhat |
| Trung toan dong | {nyc['dup_full_rows']} | |
| Null | {sum(nyc['nulls_by_column'].values())} | |
| So gia tri khac nhau | {', '.join(f'{k}={x}' for k, x in nyc['distinct'].items())} | |
| `id` vs `link_id` | id = link_id: {nyc['id_vs_link']['id_equals_link_id_rows']} dong; link co dung 1 id: {nyc['id_vs_link']['links_with_1_id']}/{nyc['distinct']['link_id']} | `id` la ma noi bo theo link, khong phai khoa ban ghi |
| `transcom_id` = `link_id` | {n(nyc['transcom_equals_link_id_rows'])}/{n(rows_total)} dong | du thua |
| `status` | {nyc['status_values']} | metadata: "Artifact - not useful" |
| Link theo borough | {nyc['links_per_borough']} | moi link 1 borough (max {nyc['borough_per_link_max']}) |
| Phien ban `link_points` / link | toi da {nyc['link_points_versions_per_link_max']} | hinh hoc khong doi trong 2025 |
| Phien ban `link_name` / link | toi da {nyc['link_name_versions_per_link_max']} | co ten bi loi (sua SCD1) |
| `speed` | min {nyc['speed']['min']}, max {nyc['speed']['max']}, =0: {n(nyc['speed']['zero'])}, >100: {nyc['speed']['gt100']}, <0: {nyc['speed']['neg']} | |
| `travel_time` | min {nyc['travel_time']['min']}, max {n(nyc['travel_time']['max'])}, =0: {n(nyc['travel_time']['zero'])}, <0: {nyc['travel_time']['neg']} | |
| Ty le hop le (0<speed<=100, tt>0) | {nyc['valid_share_rule_0_100_tt']} | |
| DST 09/03 (gio 01/02/03) | {nyc['dst_2025-03-09']} | gio 02 = 0 -> gio dia phuong |
| DST 02/11 (gio 00/01/02) | {nyc['dst_2025-11-02']} | gio 01 du hon -> gio kep bi trung mot phan |
| Link xuat hien sau 10/01 | {nyc['links_first_seen_after_jan10']} | |
| Link ngung truoc 20/12 | {nyc['links_last_seen_before_dec20']} | |

Schema (giong nhau o ca 12 file):
```
{nyc['schema'] if isinstance(nyc['schema'], str) else nyc['schema']}
```

## 2. Open-Meteo (ECMWF IFS)

| Kiem tra | Ket qua |
|---|---|
| JSON goc | {wx['raw_files']} file, {n(wx['raw_bytes_total'])} byte, `{wx['raw_dir']}` |
| timezone / utc_offset trong JSON | {sorted({r['timezone'] for r in raw_info})} / {sorted({r['utc_offset_seconds'] for r in raw_info})} |
| So gio moi file | {sorted({r['hours'] for r in raw_info})} (tu {raw_info[0]['first']} den {raw_info[0]['last']}) |
| Bien | {raw_info[0]['vars']} |
| Don vi | {wx['raw_units']} |
| Manifest | {wx['manifest_entries']} muc, tra dung o yeu cau: {wx['manifest_all_same_cell']}, ngay tai (UTC): {wx['manifest_downloaded_utc']} |
| Staging Parquet | {n(wx['staging_rows'])} dong, {wx['staging_cells']} o, {wx['staging_rows_per_cell']} dong/o |
| Khoang thoi gian staging | {wx['staging_time_min']} -> {wx['staging_time_max']} |
| Null staging | {sum(wx['staging_nulls'].values())} |
| Trung khoa (o, time_utc) | {wx['staging_dup_key']} |
| Doi chieu JSON goc <-> staging | dong chi co o mot phia: {wx['raw_vs_staging_rows_only_one_side']}; gia tri lech: {wx['raw_vs_staging_value_mismatch']} |
| Ma weather_code xuat hien | {wx['weather_code_values']} |

Schema staging:
```
{wx['staging_schema']}
```

Toa do 16 o (lat, lon, do cao m):
{md_table(["File", "lat", "lon", "elevation"], [[r['file'], r['lat'], r['lon'], r['elevation']] for r in raw_info])}

## 3. Ket qua da co truoc (chi tham chieu)
- EDA: `D:/DE_dataset_sizing/eda/EDA_NYC_DOT_OpenMeteo_2025.html` (+ `eda_numbers.json`).
- Danh gia tich hop: `D:/DE_dataset_sizing/phase1/DANH_GIA_TICH_HOP_OPENMETEO_NYCDOT.md`.
- Khao sat hinh hoc link: `D:/DE_dataset_sizing/phase1/link_geometry_survey.csv` (tam = trung binh diem, CHUA lam sach -> chi tham chieu).
- Thu muc `D:/DE_dataset_sizing/open_meteo_2025/`: **khong dung** (tai voi timezone=America/New_York, offset co dinh -4h).

## 4. Kha nang truy cap
- File nguon doc duoc tu may; khong can mang cho cac phase tiep theo.
- API NYC Open Data van bi chan 403 tu mang hien tai (khao sat 31/10/2026); khong anh huong vi du lieu da tai du.
"""
open(REP / "source_inventory.md", "w", encoding="utf-8").write(srcmd)
print("OK", REP)
print(json.dumps({"nyc_rows": rows_total, "match": nyc["rows_match_expected"], "dup_bk": nyc["dup_business_key_link_id_data_as_of"],
                  "links": nyc["distinct"]["link_id"], "wx_rows": wx["staging_rows"], "wx_mismatch": wx["raw_vs_staging_value_mismatch"],
                  "wx_one_side": wx["raw_vs_staging_rows_only_one_side"], "missing_pkgs": miss}, ensure_ascii=False))
