# Environment check (Phase 0)

Sinh tu `scripts/phase0_inventory.py` luc 2026-11-02T16:58:03+07:00.

| Hang muc | Gia tri |
|---|---|
| He dieu hanh | Windows-10-10.0.19045-SP0 |
| Python | 3.11.0 (`C:\Users\Dell\AppData\Local\Programs\Python\Python311\python.exe`) |
| Git | git version 2.50.1.windows.1 |
| O C: | trong 9.1 / 167.4 GB |
| O D: | trong 51.5 / 172.0 GB |

## Thu vien Python

| Thu vien | Phien ban | Vai tro |
|---|---|---|
| pyarrow | 23.0.0 | doc/ghi Parquet |
| pandas | 2.3.0 | bang nho |
| numpy | 2.1.3 | tinh toan |
| yaml | 6.0.2 | doc config |
| duckdb | **CHUA CAI** | engine SQL cho DW (bat buoc tu Phase 2) |
| pytest | **CHUA CAI** | kiem thu (bat buoc tu Phase 2) |
| holidays | **CHUA CAI** | lich ngay le cho dim_date (co the thay bang danh sach co nguon) |
| matplotlib | 3.10.0 | so do/bao cao |

## Ket luan
- Thieu: duckdb, pytest, holidays. Can xac nhan truoc khi cai (`python -m pip install duckdb pytest`), dung luong cai dat nho (~50 MB) nhung o C: chi con 9.1 GB.
- Du an dat tai `D:/nyc-traffic-weather-dw` (o D: con 51.5 GB). Du lieu nguon giu nguyen vi tri, chi doc qua `config/data_sources.yaml`.
- Phase 0 va Phase 1 khong can DuckDB (dung PyArrow).
