"""Nguồn dữ liệu thật: các view lakehouse.serving.* (jobs/serving/trino_views.sql) qua Trino.

Chỉ đọc view của Serving Layer + silver.link_info (lấy link_points để vẽ đường) - không đọc bảng nội bộ khác,
nên khi speed/batch layer đổi bảng bên dưới thì API không phải sửa, miễn view giữ nguyên cột.
"""
import os
import threading
from datetime import datetime

import trino

from .base import Source

CATALOG = "lakehouse"


class TrinoSource(Source):
    name = "trino"

    def __init__(self):
        self._local = threading.local()
        self._forecast_ok: bool | None = None

    # ------------------------------------------------------------------ kết nối
    def _cur(self):
        if not hasattr(self._local, "conn"):
            self._local.conn = trino.dbapi.connect(
                host=os.getenv("TRINO_HOST", "localhost"),
                port=int(os.getenv("TRINO_PORT", "8088")),
                user=os.getenv("TRINO_USER", "serving_api"),
                catalog=CATALOG,
                schema="serving",
                http_scheme=os.getenv("TRINO_SCHEME", "http"),
                request_timeout=float(os.getenv("TRINO_TIMEOUT", "60")),
            )
        return self._local.conn.cursor()

    def _rows(self, sql: str, params: list | None = None) -> list[dict]:
        cur = self._cur()
        cur.execute(sql, params or None)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]

    # ------------------------------------------------------------------ Source
    def clock(self) -> datetime | None:
        r = self._rows("SELECT max(event_clock) AS c FROM lakehouse.serving.v_traffic_latest")
        return r[0]["c"] if r else None

    def links(self) -> list[dict]:
        return self._rows("""
            SELECT d.link_id, d.link_name, d.borough, d.free_flow_speed, d.cell_id,
                   d.latitude, d.longitude, l.link_points
            FROM lakehouse.serving.v_link_dim d
            LEFT JOIN lakehouse.silver.link_info l ON l.link_id = d.link_id
        """)

    def latest(self) -> list[dict]:
        return self._rows("""
            SELECT link_id, speed, travel_time, last_valid_event_time AS event_time, traffic_state
            FROM lakehouse.serving.v_traffic_latest
        """)

    def history_15min(self, link_id, start, end, borough=None):
        where, params = ["u.window_start_utc >= ?", "u.window_start_utc < ?"], [start, end]
        join = ""
        if link_id is not None:
            where.append("u.link_id = ?")
            params.append(int(link_id))
        elif borough:
            join = "JOIN lakehouse.serving.v_link_dim d ON d.link_id = u.link_id"
            where.append("d.borough = ?")
            params.append(borough)
        return self._rows(f"""
            SELECT from_unixtime(floor(to_unixtime(u.window_start_utc) / 900) * 900) AS t,
                   sum(u.sum_speed) / nullif(sum(u.n_valid), 0) AS speed,
                   min(u.min_speed) AS min_speed
            FROM lakehouse.serving.v_traffic_5min_unified u {join}
            WHERE {' AND '.join(where)}
            GROUP BY 1 ORDER BY 1
        """, params)

    def dow_hour_profile(self, link_id=None, borough=None):
        where, params, join = ["h.n_valid > 0"], [], ""
        if link_id is not None:
            where.append("h.link_id = ?")
            params.append(int(link_id))
        elif borough:
            join = "JOIN lakehouse.serving.v_link_dim d ON d.link_id = h.link_id"
            where.append("d.borough = ?")
            params.append(borough)
        return self._rows(f"""
            SELECT day_of_week(h.hour_utc AT TIME ZONE 'America/New_York') AS dow,
                   hour(h.hour_utc AT TIME ZONE 'America/New_York') AS hour,
                   sum(h.sum_speed) / sum(h.n_valid) AS speed
            FROM lakehouse.serving.v_traffic_hourly_unified h {join}
            WHERE {' AND '.join(where)}
            GROUP BY 1, 2 ORDER BY 1, 2
        """, params)

    def weather_at(self, at):
        return self._rows("""
            SELECT cell_id, interval_start, temperature_c, precipitation_mm, wind_speed_kmh,
                   weather_code, weather_status
            FROM lakehouse.serving.v_weather_interval
            WHERE interval_start = (SELECT max(interval_start) FROM lakehouse.serving.v_weather_interval
                                    WHERE interval_start <= ?)
        """, [at])

    def forecast(self, link_id):
        # Hợp đồng chờ phần MLOps: view serving.v_traffic_forecast với các cột dưới đây.
        if self._forecast_ok is False:
            return []
        try:
            rows = self._rows("""
                SELECT target_time AS t, horizon_min, pred_speed AS speed, pred_lower AS lower,
                       pred_upper AS upper, issued_at, model_name, model_version
                FROM lakehouse.serving.v_traffic_forecast
                WHERE link_id = ?
                  AND issued_at = (SELECT max(issued_at) FROM lakehouse.serving.v_traffic_forecast WHERE link_id = ?)
                ORDER BY target_time
            """, [int(link_id), int(link_id)])
            self._forecast_ok = True
            return rows
        except trino.exceptions.TrinoUserError:
            self._forecast_ok = False      # view chưa tồn tại -> không hỏi lại mỗi request
            return []
