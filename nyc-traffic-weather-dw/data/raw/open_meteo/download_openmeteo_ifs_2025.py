"""Tai Open-Meteo Historical (ECMWF IFS) theo tung o luoi, gio UTC.
- Danh sach o: lay tu phase1/link_to_cell_by_model.csv (model ecmwf_ifs) -> 16 o duy nhat.
- Pham vi: 2024-12-31 -> 2026-01-01 (GMT) de phu lech mui gio NY->UTC va dac trung tre.
- Luu goc (JSON nguyen ban) va chuan hoa (Parquet) rieng biet.
Chay: python -I download_openmeteo_ifs_2025.py <root D:/DE_dataset_sizing>
"""
import datetime as dt, json, os, sys, time, urllib.error, urllib.parse, urllib.request
import pandas as pd
sys.stdout.reconfigure(encoding="utf-8")
ROOT = sys.argv[1]
API = "https://archive-api.open-meteo.com/v1/archive"
MODEL = "ecmwf_ifs"
VARS = ["temperature_2m", "relative_humidity_2m", "precipitation", "snowfall", "wind_speed_10m", "wind_gusts_10m", "weather_code"]
START, END = "2024-12-31", "2026-01-01"
raw_dir = os.path.join(ROOT, "data", "raw", "open_meteo", MODEL)
stg_dir = os.path.join(ROOT, "data", "staging")
os.makedirs(raw_dir, exist_ok=True); os.makedirs(stg_dir, exist_ok=True)

m = pd.read_csv(os.path.join(ROOT, "phase1", "link_to_cell_by_model.csv"))
cells = m[m.model == MODEL][["cell_lat", "cell_lon"]].drop_duplicates().sort_values(["cell_lat", "cell_lon"]).reset_index(drop=True)
cells["weather_location_id"] = [f"ifs_{a:.4f}_{b:.4f}" for a, b in zip(cells.cell_lat, cells.cell_lon)]

def fetch(lat, lon, tries=5):
    q = {"latitude": f"{lat:.5f}", "longitude": f"{lon:.5f}", "start_date": START, "end_date": END,
         "hourly": ",".join(VARS), "timezone": "GMT", "models": MODEL}
    url = API + "?" + urllib.parse.urlencode(q, safe=",")
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                body = r.read(); d = json.loads(body)
                if d.get("error"): raise RuntimeError(d.get("reason"))
                return url, body, d
        except urllib.error.HTTPError as e:
            msg = e.read().decode("utf-8", "replace")[:200]
            if e.code in (400, 404): raise RuntimeError(f"HTTP {e.code}: {msg}")
            print(f"  HTTP {e.code}, thu lai ({i+1})"); time.sleep(2 ** i * 2)
        except (urllib.error.URLError, TimeoutError, RuntimeError) as e:
            print(f"  loi: {e}, thu lai ({i+1})"); time.sleep(2 ** i * 2)
    raise RuntimeError("that bai sau nhieu lan thu")

manifest, frames = [], []
for _, c in cells.iterrows():
    url, body, d = fetch(c.cell_lat, c.cell_lon)
    fn = os.path.join(raw_dir, f"{c.weather_location_id}.json")
    open(fn, "wb").write(body)
    h = pd.DataFrame(d["hourly"])
    h["time_utc"] = pd.to_datetime(h.pop("time")).dt.tz_localize("UTC")
    h.insert(0, "weather_location_id", c.weather_location_id)
    h["model"] = MODEL; h["grid_lat"] = d["latitude"]; h["grid_lon"] = d["longitude"]; h["elevation"] = d.get("elevation")
    snapped = abs(d["latitude"] - c.cell_lat) < 1e-3 and abs(d["longitude"] - c.cell_lon) < 1e-3
    manifest.append({"weather_location_id": c.weather_location_id, "url": url, "bytes": len(body), "rows": len(h),
                     "nulls": int(h[VARS].isna().sum().sum()), "same_cell_returned": bool(snapped),
                     "downloaded_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"})
    frames.append(h)
    print(c.weather_location_id, len(h), "dong", "OK" if snapped else "O KHAC!")
    time.sleep(0.5)

W = pd.concat(frames, ignore_index=True)
W.to_parquet(os.path.join(stg_dir, "weather_ifs_2024-12-31_2026-01-01_utc.parquet"), index=False, compression="zstd")
cells.to_csv(os.path.join(stg_dir, "dim_weather_location_ifs.csv"), index=False)
json.dump(manifest, open(os.path.join(raw_dir, "_manifest.json"), "w"), indent=1)
print("TONG", len(W), "dong |", W.weather_location_id.nunique(), "o | null:", int(W[VARS].isna().sum().sum()),
      "| trung khoa:", int(W.duplicated(["weather_location_id", "time_utc"]).sum()))
