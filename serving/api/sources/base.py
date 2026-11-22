"""Giao diện nguồn dữ liệu của Serving API.

Hai cài đặt:
  * TrinoSource (mặc định): đọc view lakehouse.serving.* trên Trino - kiến trúc thật của nhóm.
  * DemoSource: ClickHouse cục bộ + replay file parquet, để chạy giao diện trên máy yếu không có lakehouse.

Mọi timestamp trả ra là datetime có timezone (UTC). "Đồng hồ" (clock) là thời điểm dữ liệu mới nhất,
KHÔNG phải giờ hệ thống, vì dữ liệu là bản phát lại năm 2025.
"""
from abc import ABC, abstractmethod
from datetime import datetime


class Source(ABC):
    name = "base"

    @abstractmethod
    def clock(self) -> datetime | None:
        """Event clock: event_time lớn nhất đã nhận."""

    @abstractmethod
    def links(self) -> list[dict]:
        """[{link_id, link_name, borough, free_flow_speed, cell_id, latitude, longitude, link_points}]"""

    @abstractmethod
    def latest(self) -> list[dict]:
        """Trạng thái mới nhất mỗi link:
        [{link_id, speed, travel_time, event_time, traffic_state}]"""

    @abstractmethod
    def history_15min(self, link_id: int | None, start: datetime, end: datetime,
                      borough: str | None = None) -> list[dict]:
        """[{t, speed, min_speed}] gộp 15 phút. link_id=None -> toàn mạng (hoặc theo quận)."""

    @abstractmethod
    def dow_hour_profile(self, link_id: int | None = None, borough: str | None = None) -> list[dict]:
        """Tốc độ TB theo thứ (1=T2..7=CN) x giờ New York (0..23): [{dow, hour, speed}]"""

    @abstractmethod
    def weather_at(self, at: datetime) -> list[dict]:
        """Thời tiết khoảng giờ chứa `at`, mỗi ô: [{cell_id, interval_start, temperature_c,
        precipitation_mm, wind_speed_kmh, weather_code, weather_status}]"""

    def forecast(self, link_id: int) -> list[dict]:
        """[{t, horizon_min, speed, lower, upper, issued_at, model_name, model_version}] - rỗng nếu chưa có model."""
        return []
