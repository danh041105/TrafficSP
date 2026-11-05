# Bus Matrix

## Quy trình nghiệp vụ

| Quy trình | Nguồn | Sự kiện được đo |
|---|---|---|
| **Traffic Monitoring** | NYC DOT Traffic Speeds (`i4gi-tjb9`) | Mỗi lần cảm biến gửi tốc độ trung bình của một đoạn đường (~5 phút/lần) |
| **Weather Observation** | Open-Meteo, mô hình ECMWF IFS | Thời tiết của một ô lưới (~8 × 11 km) trong một giờ |
| **Traffic × Weather Analytics** | Tích hợp hai quy trình trên | Không phải quy trình nguồn; là lớp phân tích (fact hợp nhất) |

## Ma trận

| Fact (grain) | Date | Time | Road Link | Weather Location | Weather Condition |
|---|:-:|:-:|:-:|:-:|:-:|
| `fact_traffic_reading` (1 lần đo của 1 link) | ✕ | ✕ | ✕ | | |
| `fact_traffic_hourly` (1 link × 1 giờ UTC) | ✕ | ✕ | ✕ | | |
| `fact_weather_hourly` (1 ô × 1 khoảng giờ UTC) | ✕ | ✕ | | ✕ | ✕ |
| `fact_traffic_weather_hourly` (1 link × 1 giờ UTC) | ✕ | ✕ | ✕ | ✕ | ✕ |

## Dimension dùng chung và dimension riêng

| Dimension | Phạm vi | Ghi chú |
|---|---|---|
| **Date** | **Dùng chung thực sự** (cả 2 quy trình) | Ngày theo lịch New York, tính từ giờ UTC bằng cơ sở dữ liệu múi giờ (có DST) |
| **Time** | **Dùng chung thực sự** | Giờ trong ngày (0–23) theo giờ New York |
| **Road Link** | Riêng Traffic Monitoring | Fact hợp nhất dùng lại cùng bảng |
| **Weather Location** | Riêng Weather Observation | Fact hợp nhất dùng lại cùng bảng |
| **Weather Condition** | Riêng Weather Observation | Fact hợp nhất dùng lại cùng bảng |

## Drill-across

`fact_traffic_hourly` và `fact_weather_hourly` có chung **Date + Time + giờ UTC**. Để nối theo không gian, dùng thuộc tính `weather_loc_key` trong `dim_road_link`: mỗi link được gán đúng 1 ô (bảng `road_weather_mapping` ở staging). Phép nối là nhiều-một, nên không nhân bản dòng.

```
fact_traffic_hourly ── road_key ──> dim_road_link.weather_loc_key ──┐
                     └─ hour_utc ─────────────────────────────────── ├─> fact_weather_hourly (weather_loc_key, interval_start_utc)
```

`fact_traffic_weather_hourly` là kết quả drill-across đã được vật chất hóa sẵn để tiện truy vấn OLAP. Bảng này **không thay thế** hai fact nguyên tử.
