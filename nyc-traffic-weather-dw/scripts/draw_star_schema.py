"""Ve so do Star Schema: moi fact la mot ngoi sao rieng (2x2), duong FK thang dung/ngang, khong cat nhau.
Chay: python -I scripts/draw_star_schema.py  -> docs/star_schema.png, docs/star_schema.svg
"""
from pathlib import Path
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

ROOT = Path(__file__).resolve().parents[1]
INK, INK2, LINE, SURF = "#0b0b0b", "#52514e", "#898781", "#fcfcfb"
DIM_FILL, DIM_EDGE = "#e8f1fc", "#2a78d6"
FACT_FILL, FACT_EDGE = "#fdeee6", "#eb6834"
plt.rcParams.update({"font.family": "Segoe UI", "figure.facecolor": SURF})

DIMS = {
    "dim_date": ["PK date_key (YYYYMMDD)", "full_date, day_type", "is_holiday, is_weekend", "month, quarter, year"],
    "dim_time": ["PK time_key (0–23)", "hour_label", "day_part"],
    "dim_road_link": ["PK road_key", "BK link_id, version", "link_name, borough, city", "geometry_wkt, rep_lat/lon", "weather_loc_key (mapping)"],
    "dim_weather_location": ["PK weather_loc_key", "BK weather_location_id", "grid_lat/lon, elevation_m"],
    "dim_weather_condition": ["PK condition_key", "BK weather_code", "category, precip_type"],
}
FACTS = {
    "fact_traffic_reading": ("Atomic · 1 lần đo / link", ["PK road_key + reading_ts_local", "FK date_key, time_key", "reading_ts_utc (NULL nếu DST kép)",
                              "speed_mph, travel_time_s (gốc)", "is_valid, invalid_reason", "is_dst_ambiguous, is_stuck", "source_file, load_batch_id"]),
    "fact_traffic_hourly": ("Aggregate · link × giờ UTC (lưới đủ)", ["PK road_key + hour_utc", "FK date_key, time_key", "n_records, n_valid, n_zero,",
                             "n_invalid_other", "sum_speed, sum_speed_sq,", "sum_travel_time, min/max_speed", "median_speed, coverage_ratio"]),
    "fact_weather_hourly": ("Atomic · ô × khoảng [H, H+1) UTC", ["PK weather_loc_key + interval_start_utc", "FK date_key, time_key, condition_key",
                             "temperature_c, rel_humidity_pct  (@H)", "wind_speed_kmh, wind_gust_kmh (@H)", "precipitation_mm, snowfall_cm (@H+1)",
                             "model, source_file, load_batch_id"]),
    "fact_traffic_weather_hourly": ("Consolidated · link × giờ UTC", ["PK road_key + hour_utc", "FK date_key, time_key,", "     weather_loc_key, condition_key",
                                     "measures giao thông (như hourly)", "temperature_c, precipitation_mm,", "snowfall_cm, wind_gust_kmh …"]),
}

def box(ax, x, y, w, title, lines, fill, edge, sub=None):
    lh = 0.36; h = 0.62 + lh * len(lines) + (0.3 if sub else 0)
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12", fc=fill, ec=edge, lw=1.6))
    ax.text(x + 0.15, y + h - 0.35, title, fontsize=10.5, fontweight="bold", color=INK, va="center")
    yy = y + h - 0.72
    if sub:
        ax.text(x + 0.15, yy, sub, fontsize=8.5, color=INK2, va="center", style="italic"); yy -= 0.3
    for l in lines:
        ax.text(x + 0.15, yy, l, fontsize=8.5, color=INK, va="center", family="Consolas"); yy -= lh
    return (x, y, w, h)

def link(ax, p0, p1, label, label_side="above"):
    ax.plot([p0[0], p1[0]], [p0[1], p1[1]], color=LINE, lw=1.4, zorder=0)
    mx, my = (p0[0] + p1[0]) / 2, (p0[1] + p1[1]) / 2
    if p0[1] == p1[1]:
        ax.text(mx, my + 0.14, label, fontsize=8, color=INK2, ha="center", va="bottom", family="Consolas")
    elif p0[0] == p1[0]:
        ax.text(mx + 0.1, my, label, fontsize=8, color=INK2, ha="left", va="center", family="Consolas")
    else:  # duong cheo: nhan dat ve phia ngoai
        ax.text(mx + (0.15 if p1[0] > p0[0] else -0.15), my, label, fontsize=8, color=INK2,
                ha="left" if p1[0] > p0[0] else "right", va="center", family="Consolas")

def star(ax, fact, dims):
    ax.set_xlim(0, 14); ax.set_ylim(0, 10); ax.axis("off")
    sub, lines = FACTS[fact]
    fw = 4.6; fx = 7 - fw / 2
    fb = box(ax, fx, 3.0, fw, fact, lines, FACT_FILL, FACT_EDGE, sub=sub)
    fy_mid = fb[1] + fb[3] / 2
    placed = {}
    for name, pos in dims.items():
        dl = DIMS[name]; w = 3.6
        if pos == "left":   b = box(ax, 0.2, fy_mid - (0.62 + 0.36 * len(dl)) / 2, w, name, dl, DIM_FILL, DIM_EDGE)
        elif pos == "right": b = box(ax, 14 - 0.2 - w, fy_mid - (0.62 + 0.36 * len(dl)) / 2, w, name, dl, DIM_FILL, DIM_EDGE)
        elif pos == "top":   b = box(ax, 7 - w / 2, 10 - 0.15 - (0.62 + 0.36 * len(dl)), w, name, dl, DIM_FILL, DIM_EDGE)
        elif pos == "top_l": b = box(ax, 1.2, 10 - 0.15 - (0.62 + 0.36 * len(dl)), w, name, dl, DIM_FILL, DIM_EDGE)
        elif pos == "top_r": b = box(ax, 14 - 1.2 - w, 10 - 0.15 - (0.62 + 0.36 * len(dl)), w, name, dl, DIM_FILL, DIM_EDGE)
        elif pos == "bottom": b = box(ax, 7 - w / 2, 0.15, w, name, dl, DIM_FILL, DIM_EDGE)
        placed[name] = (pos, b)
    for name, (pos, b) in placed.items():
        key = DIMS[name][0].replace("PK ", "").split(" ")[0]
        if pos == "left":   link(ax, (b[0] + b[2], fy_mid), (fb[0], fy_mid), key)
        elif pos == "right": link(ax, (fb[0] + fb[2], fy_mid), (b[0], fy_mid), key)
        elif pos == "top":   link(ax, (7, fb[1] + fb[3]), (7, b[1]), key)
        elif pos == "top_l": link(ax, (fb[0] + 0.9, fb[1] + fb[3]), (b[0] + b[2] / 2, b[1]), key)
        elif pos == "top_r": link(ax, (fb[0] + fb[2] - 0.9, fb[1] + fb[3]), (b[0] + b[2] / 2, b[1]), key)
        elif pos == "bottom": link(ax, (7, fb[1]), (7, b[1] + b[3]), key)

fig, axs = plt.subplots(2, 2, figsize=(20, 14.5))
star(axs[0, 0], "fact_traffic_reading", {"dim_date": "left", "dim_time": "right", "dim_road_link": "top"})
star(axs[0, 1], "fact_traffic_hourly", {"dim_date": "left", "dim_time": "right", "dim_road_link": "top"})
star(axs[1, 0], "fact_weather_hourly", {"dim_date": "left", "dim_time": "right", "dim_weather_location": "top", "dim_weather_condition": "bottom"})
star(axs[1, 1], "fact_traffic_weather_hourly", {"dim_date": "left", "dim_time": "right", "dim_road_link": "top_l", "dim_weather_location": "top_r", "dim_weather_condition": "bottom"})
for ax, t in zip(axs.flat, ["A. Traffic Monitoring – atomic", "B. Traffic Monitoring – aggregate", "C. Weather Observation – atomic", "D. Traffic × Weather – consolidated (mart OLAP)"]):
    ax.set_title(t, fontsize=13, fontweight="semibold", color=INK, loc="left")
fig.suptitle("NYC Traffic × Weather DW 2025 – Star Schema (mỗi fact một ngôi sao; dim cùng tên là dimension dùng chung)", fontsize=15, fontweight="bold", color=INK, y=0.995)
fig.text(0.5, 0.005, "Mọi quan hệ là N (fact) : 1 (dimension). dim_date và dim_time dùng chung cho cả hai quy trình; road_link riêng giao thông; weather_location và weather_condition riêng thời tiết.",
         ha="center", fontsize=10, color=INK2)
fig.tight_layout(rect=(0, 0.02, 1, 0.97))
for ext in ("png", "svg"):
    fig.savefig(ROOT / "docs" / f"star_schema.{ext}", dpi=110)
print("ok")
