# ETL reconciliation

Sinh tự động bởi `scripts/run.py 6`. Mọi số liệu đọc lại từ file đã nạp.

## Giao thông – bản ghi

| Bước | Số bản ghi | Ghi chú |
|---|---|---|
| Cổng NYC Open Data (lọc 2025) | 12 714 844 | hộp thoại Export, 31/10/2026 |
| Raw: 12 file Parquet | 12 714 844 | manifest |
| Staging: stg_traffic_reading | 12 714 844 | giữ toàn bộ |
| DW: fact_traffic_reading | 12 714 844 | giữ toàn bộ |
|   trong đó gán được giờ UTC | 12 712 376 |  |
|   trong đó giờ DST kép (không gán giờ, D-03) | 2 468 | 01:00–01:59 ngày 02/11 |
| DW: fact_traffic_hourly – Σ n_records | 12 712 376 | = số bản ghi gán được giờ |
| Mart: fact_traffic_weather_hourly – Σ n_records | 12 712 376 | = hourly |

Chênh lệch giữa atomic và Σ n_records = 2 468 = đúng số bản ghi giờ DST kép (giải thích được).

## Giao thông – số dòng theo giờ

| Bảng | Số dòng | Ghi chú |
|---|---|---|
| fact_traffic_hourly (dòng = link × giờ) | 1 078 584 | lưới đủ từ giờ đầu tới giờ cuối của mỗi link |
| fact_traffic_weather_hourly (dòng) | 1 078 584 | = hourly, JOIN N:1 |

## Thời tiết

| Bước | Số dòng | Ghi chú |
|---|---|---|
| Raw: 16 file JSON (giờ API) | 140 928 | 16 × 8 808, 31/12/2024–01/01/2026 UTC |
| Staging: stg_weather_hourly | 140 928 | giữ toàn bộ |
| Staging: stg_weather_interval | 140 160 | 16 × 8 760 khoảng thuộc năm 2025 giờ NY; bỏ 48 giờ đệm/ô (29 trước, 19 sau; giờ đệm đầu tiên sau năm dùng làm H+1) |
| DW: fact_weather_hourly | 140 160 | = interval |

## Đoạn đường

| Bước | Số link | Ghi chú |
|---|---|---|
| Link trong nguồn | 124 |  |
| dim_road_link (không tính unknown) | 124 |  |
| Link có ô thời tiết | 124 |  |

## Kết luận

- Không có bản ghi nào bị mất mà không giải thích được.
- Mọi phép JOIN là N:1; số dòng trước và sau ghép bằng nhau; không trùng khóa ở mọi bảng (xem `data_quality_report.md`).