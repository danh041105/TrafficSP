# nyc-traffic-weather-dw

Data Warehouse (Kimball) tích hợp **NYC DOT Traffic Speeds** và **Open-Meteo (ECMWF IFS)** năm 2025.

## Trạng thái
| Phase | Nội dung | Kết quả | Báo cáo |
|---|---|---|---|
| 0 | Khảo sát môi trường và nguồn | ✅ | `reports/environment_check.md`, `reports/source_inventory.md` |
| 1 | Thiết kế DW | ✅ đã duyệt | `docs/` |
| 2 | Raw & lineage | ✅ 28/28 checksum khớp | `reports/raw_manifest_report.md`, `data/raw/manifest/raw_manifest.json` |
| 3 | Staging | ✅ 11/11 tiêu chí | `reports/phase3_staging_report.md` |
| 4 | Dimensions & atomic facts | ✅ 16/16 | `reports/phase4_dw_atomic_report.md` |
| 5 | Aggregate & mart | ✅ 13/13 | `reports/phase5_aggregate_report.md` |
| 6 | Kiểm định chất lượng | ✅ 22/22 kiểm thử, 11/11 tiêu chí | `reports/data_quality_report.md`, `reports/etl_reconciliation.md` |
| 7 | OLAP | ✅ 8 truy vấn | `reports/olap_results.md`, `sql/olap/` |

## Bảng
| Bảng | Grain | Dòng | Vị trí |
|---|---|---|---|
| `dim_date`, `dim_time`, `dim_road_link`, `dim_weather_location`, `dim_weather_condition` | | 366 / 25 / 125 / 17 / 29 | `data/dw/dimensions/` |
| `fact_traffic_reading` | 1 lần đo / link | 12 714 844 | `data/dw/facts/fact_traffic_reading/source_month=*/` |
| `fact_weather_hourly` | ô × khoảng giờ UTC | 140 160 | `data/dw/facts/fact_weather_hourly.parquet` |
| `fact_traffic_hourly` | link × giờ UTC (lưới đủ) | 1 078 584 | `data/dw/facts/fact_traffic_hourly/month_utc=*/` |
| `fact_traffic_weather_hourly` | link × giờ UTC + thời tiết | 1 078 584 | `data/mart/fact_traffic_weather_hourly/month_utc=*/` |

`data/nyc_dw.duckdb` chỉ chứa view trên các file Parquet.

## Chạy lại
```bash
python -m pip install -e .[dev]
```
```bash
python -I scripts/run.py 2
```
Lần lượt `2 → 7`. Mỗi bước ghi vào thư mục tạm rồi mới thay bản cũ (bản trước giữ ở `*.prev` để rollback); surrogate key giữ ổn định qua `data/dw/dimensions/_key_registry/`. Kiểm thử độc lập:
```bash
python -m pytest -q
```
Truy vấn thử:
```bash
python -c "import duckdb; print(duckdb.connect('data/nyc_dw.duckdb').sql('SELECT borough, sum(sum_speed)/sum(n_valid) FROM fact_traffic_hourly JOIN dim_road_link USING (road_key) GROUP BY 1'))"
```

## Cấu trúc
```
config/    data_sources.yaml, settings.yaml, quality_rules.yaml, calendar.yaml
docs/      bus_matrix, dimensional_model, data_dictionary, decisions, architecture, star_schema.png/.svg
sql/       staging/, facts/, marts/, views/, olap/
src/nyc_dw ingestion/ (raw manifest) · transformation/ (staging, geometry) · modeling/ (dw_atomic, dw_aggregate, reference) · validation/ (quality, olap_demo) · utils/
tests/     22 kiểm thử pytest (đối chiếu ngược về dữ liệu gốc)
scripts/   run.py, phase0_inventory.py, draw_star_schema.py, provenance/ (mã Colab đã dùng để trích xuất)
reports/   báo cáo từng phase + logs/
```

## Nguồn
- NYC DOT: https://data.cityofnewyork.us/Transportation/DOT-Traffic-Speeds/i4gi-tjb9/about_data
- Open-Meteo: https://open-meteo.com/en/docs/historical-weather-api
