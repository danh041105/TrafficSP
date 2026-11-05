# Architecture (giai đoạn batch)

```
Nguồn (chỉ đọc)                  Raw & lineage            Staging (chuẩn hóa)           DW (Kimball)                    Mart
───────────────                  ──────────────           ───────────────────           ────────────                    ────
NYC DOT 12×Parquet  ──┐          manifest: checksum,      stg_traffic_reading           dim_date, dim_time              fact_traffic_weather_hourly
(D:/DE_dataset_sizing)│  Phase 2  số dòng, query,    ──>  stg_road_link          ──>    dim_road_link                   (phân vùng theo tháng)
Open-Meteo 16×JSON  ──┘          ngày tải                 road_weather_mapping   Phase4 dim_weather_location       Phase5
+ staging Parquet                                        stg_weather_hourly     ─────  dim_weather_condition
                                                         (Phase 3)                     fact_traffic_reading
                                                                                       fact_weather_hourly
                                                                                       fact_traffic_hourly
                                                 Phase 6: data_quality_report, etl_reconciliation · Phase 7: SQL OLAP
```

| Lớp | Định dạng | Ghi chú |
|---|---|---|
| Raw | Parquet/JSON gốc, không sửa | Giai đoạn đầu trỏ tới vị trí hiện có; manifest có checksum |
| Staging | Parquet | Giữ business key, timestamp gốc, cột chuẩn hóa, lineage |
| DW | Parquet (zstd), DuckDB đọc trực tiếp | Surrogate key, PK/FK kiểm bằng SQL assertion |
| Mart | Parquet phân vùng theo tháng UTC | Fact hợp nhất phục vụ OLAP |

Mỗi bước là một script chạy lại được: đọc config, ghi ra thư mục tạm, chạy kiểm thử, rồi mới thay thế bản cũ (giữ bản trước để rollback). Mỗi lần nạp có một `load_batch_id`.

Hướng mở rộng (chưa làm): thay thư mục bằng MinIO và dùng Iceberg cho các bảng; dùng `fact_traffic_reading` sắp theo `reading_ts_utc` làm nguồn phát lại cho Kafka (Lambda: batch = DW này, speed layer = Flink).
