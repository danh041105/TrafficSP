# Source inventory (Phase 0)

Sinh tu `scripts/phase0_inventory.py` luc 2026-11-02T16:58:03+07:00. Moi so lieu duoi day doc truc tiep tu file; khong dung lai ket qua cu.

## 1. NYC DOT Traffic Speeds (`i4gi-tjb9`)

Vi tri: `D:/DE_dataset_sizing/nyc_dot_2025` · 12 file · 55 424 813 byte · trich xuat bang Colab (SODA API, tung ngay) ngay 2026-10-31.

| File | Byte | Dong | Row groups | data_as_of min | data_as_of max | Dong ngoai thang | Link | Null |
|---|---|---|---|---|---|---|---|---|
| 2025-01.parquet | 4 545 221 | 1 079 027 | 2 | 2025-01-01 00:03:03 | 2025-01-31 23:58:10 | 0 | 122 | 0 |
| 2025-02.parquet | 4 148 570 | 975 744 | 1 | 2025-02-01 00:03:03 | 2025-02-28 23:58:09 | 0 | 121 | 0 |
| 2025-03.parquet | 5 072 450 | 1 078 263 | 2 | 2025-03-01 00:03:03 | 2025-03-31 23:58:11 | 0 | 121 | 0 |
| 2025-04.parquet | 4 208 584 | 1 037 270 | 1 | 2025-04-01 00:03:03 | 2025-04-30 23:59:03 | 0 | 121 | 0 |
| 2025-05.parquet | 5 009 527 | 1 066 866 | 2 | 2025-05-01 00:03:03 | 2025-05-31 23:59:08 | 0 | 120 | 0 |
| 2025-06.parquet | 4 773 245 | 1 005 175 | 1 | 2025-06-01 00:03:03 | 2025-06-30 23:58:09 | 0 | 123 | 0 |
| 2025-07.parquet | 4 571 228 | 1 084 419 | 2 | 2025-07-01 00:03:07 | 2025-07-31 23:59:03 | 0 | 124 | 0 |
| 2025-08.parquet | 4 613 860 | 1 095 253 | 2 | 2025-08-01 00:03:03 | 2025-08-31 23:58:11 | 0 | 124 | 0 |
| 2025-09.parquet | 4 430 713 | 1 058 347 | 2 | 2025-09-01 00:04:03 | 2025-09-30 23:58:09 | 0 | 124 | 0 |
| 2025-10.parquet | 4 531 623 | 1 078 806 | 2 | 2025-10-01 00:02:12 | 2025-10-31 23:59:09 | 0 | 124 | 0 |
| 2025-11.parquet | 5 035 438 | 1 061 273 | 2 | 2025-11-01 00:03:03 | 2025-11-30 23:58:11 | 0 | 124 | 0 |
| 2025-12.parquet | 4 484 354 | 1 094 401 | 2 | 2025-12-01 00:02:13 | 2025-12-31 23:58:09 | 0 | 124 | 0 |

| Kiem tra | Ket qua | Danh gia |
|---|---|---|
| Tong so dong | 12 714 844 (ky vong 12 714 844) | KHOP |
| Schema giong nhau giua 12 file | True | |
| Mui gio gan trong `data_as_of` | None (timestamp khong mui gio) | dung voi gia dinh "gio dia phuong, khong offset" |
| Khoang `data_as_of` | 2025-01-01 00:03:03 -> 2025-12-31 23:58:09 | |
| Trung khoa nghiep vu (`link_id`, `data_as_of`) | 0 | khoa duy nhat |
| Trung toan dong | 0 | |
| Null | 0 | |
| So gia tri khac nhau | id=124, link_id=124, transcom_id=124, status=2, borough=5, owner=7, link_name=126, link_points=122 | |
| `id` vs `link_id` | id = link_id: 0 dong; link co dung 1 id: 124/124 | `id` la ma noi bo theo link, khong phai khoa ban ghi |
| `transcom_id` = `link_id` | 12 714 844/12 714 844 dong | du thua |
| `status` | {'0': 9100239, '-101': 3614605} | metadata: "Artifact - not useful" |
| Link theo borough | {'Bronx': 24, 'Brooklyn': 12, 'Manhattan': 26, 'Queens': 37, 'Staten Island': 25} | moi link 1 borough (max 1) |
| Phien ban `link_points` / link | toi da 1 | hinh hoc khong doi trong 2025 |
| Phien ban `link_name` / link | toi da 2 | co ten bi loi (sua SCD1) |
| `speed` | min 0.0, max 119.3, =0: 2 802 917, >100: 108, <0: 0 | |
| `travel_time` | min -52, max 14 896, =0: 2 802 725, <0: 72 | |
| Ty le hop le (0<speed<=100, tt>0) | 0.7795 | |
| DST 09/03 (gio 01/02/03) | {'h01': 1452, 'h02': 0, 'h03': 1452} | gio 02 = 0 -> gio dia phuong |
| DST 02/11 (gio 00/01/02) | {'h00': 1488, 'h01': 2468, 'h02': 1488} | gio 01 du hon -> gio kep bi trung mot phan |
| Link xuat hien sau 10/01 | [['4616318', '2025-06-09 13:09:03', '2025-12-31 23:58:03'], ['4616319', '2025-06-09 13:09:03', '2025-12-31 23:58:03']] | |
| Link ngung truoc 20/12 | [] | |

Schema (giong nhau o ca 12 file):
```
id: string
speed: double
travel_time: int64
status: string
data_as_of: timestamp[ns]
link_id: string
link_points: string
encoded_poly_line: string
encoded_poly_line_lvls: string
owner: string
transcom_id: string
borough: string
link_name: string
```

## 2. Open-Meteo (ECMWF IFS)

| Kiem tra | Ket qua |
|---|---|
| JSON goc | 16 file, 6 821 337 byte, `D:/DE_dataset_sizing/data/raw/open_meteo/ecmwf_ifs` |
| timezone / utc_offset trong JSON | ['GMT'] / [0] |
| So gio moi file | [8808] (tu 2024-12-31T00:00 den 2026-01-01T23:00) |
| Bien | ['temperature_2m', 'relative_humidity_2m', 'precipitation', 'snowfall', 'wind_speed_10m', 'wind_gusts_10m', 'weather_code'] |
| Don vi | {'time': 'iso8601', 'temperature_2m': '°C', 'relative_humidity_2m': '%', 'precipitation': 'mm', 'snowfall': 'cm', 'wind_speed_10m': 'km/h', 'wind_gusts_10m': 'km/h', 'weather_code': 'wmo code'} |
| Manifest | 16 muc, tra dung o yeu cau: True, ngay tai (UTC): ['2026-11-01'] |
| Staging Parquet | 140 928 dong, 16 o, [8808] dong/o |
| Khoang thoi gian staging | 2024-12-31 00:00:00+00:00 -> 2026-01-01 23:00:00+00:00 |
| Null staging | 0 |
| Trung khoa (o, time_utc) | 0 |
| Doi chieu JSON goc <-> staging | dong chi co o mot phia: 0; gia tri lech: {'temperature_2m': 0, 'relative_humidity_2m': 0, 'precipitation': 0, 'snowfall': 0, 'wind_speed_10m': 0, 'wind_gusts_10m': 0, 'weather_code': 0} |
| Ma weather_code xuat hien | [0, 1, 2, 3, 45, 51, 53, 55, 61, 63, 65, 71, 73, 75, 80, 81, 82, 85, 86, 95] |

Schema staging:
```
weather_location_id: string
temperature_2m: double
relative_humidity_2m: int64
precipitation: double
snowfall: double
wind_speed_10m: double
wind_gusts_10m: double
weather_code: int64
time_utc: timestamp[ns, tz=UTC]
model: string
grid_lat: double
grid_lon: double
elevation: double
```

Toa do 16 o (lat, lon, do cao m):
| File | lat | lon | elevation |
|---|---|---|---|
| ifs_40.5272_-74.2373.json | 40.52724 | -74.237274 | 20.0 |
| ifs_40.5975_-73.9604.json | 40.597538 | -73.96039 | 8.0 |
| ifs_40.5975_-74.0877.json | 40.597538 | -74.08771 | 27.0 |
| ifs_40.5975_-74.2150.json | 40.597538 | -74.215 | 1.0 |
| ifs_40.6678_-73.6827.json | 40.667835 | -73.68274 | 21.0 |
| ifs_40.6678_-73.8102.json | 40.667835 | -73.81021 | 9.0 |
| ifs_40.6678_-73.9377.json | 40.667835 | -73.93768 | 39.0 |
| ifs_40.6678_-74.1927.json | 40.667835 | -74.19266 | 8.0 |
| ifs_40.7381_-73.7872.json | 40.738136 | -73.78723 | 23.0 |
| ifs_40.7381_-73.9149.json | 40.738136 | -73.91489 | 32.0 |
| ifs_40.7381_-74.0425.json | 40.738136 | -74.04254 | 2.0 |
| ifs_40.8084_-73.7642.json | 40.808434 | -73.76422 | 0.0 |
| ifs_40.8084_-73.8921.json | 40.808434 | -73.89206 | 6.0 |
| ifs_40.8084_-74.0199.json | 40.808434 | -74.0199 | 8.0 |
| ifs_40.8787_-73.8691.json | 40.878735 | -73.86914 | 27.0 |
| ifs_40.8787_-73.9972.json | 40.878735 | -73.99716 | 9.0 |

## 3. Ket qua da co truoc (chi tham chieu)
- EDA: `D:/DE_dataset_sizing/eda/EDA_NYC_DOT_OpenMeteo_2025.html` (+ `eda_numbers.json`).
- Danh gia tich hop: `D:/DE_dataset_sizing/phase1/DANH_GIA_TICH_HOP_OPENMETEO_NYCDOT.md`.
- Khao sat hinh hoc link: `D:/DE_dataset_sizing/phase1/link_geometry_survey.csv` (tam = trung binh diem, CHUA lam sach -> chi tham chieu).
- Thu muc `D:/DE_dataset_sizing/open_meteo_2025/`: **khong dung** (tai voi timezone=America/New_York, offset co dinh -4h).

## 4. Kha nang truy cap
- File nguon doc duoc tu may; khong can mang cho cac phase tiep theo.
- API NYC Open Data van bi chan 403 tu mang hien tai (khao sat 31/10/2026); khong anh huong vi du lieu da tai du.
