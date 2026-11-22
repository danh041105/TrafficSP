"""Nguồn dữ liệu CHẾ ĐỘ DEMO: ClickHouse cục bộ do serving/demo/*.py nạp và phát lại.
Cho ra cùng hình dạng dữ liệu như TrinoSource để giao diện không phải biết đang chạy nguồn nào."""
import os
import threading
from datetime import timedelta

import clickhouse_connect

from ..rules import classify
from .base import Source


class DemoSource(Source):
    name = "demo"

    def __init__(self):
        self._local = threading.local()

    def _c(self):
        if not hasattr(self._local, "c"):
            self._local.c = clickhouse_connect.get_client(
                host=os.getenv("DEMO_CH_HOST", "localhost"),
                port=int(os.getenv("DEMO_CH_PORT", "8123")),
                username=os.getenv("DEMO_CH_USER", "demo"),
                password=os.getenv("DEMO_CH_PASSWORD", "demo"),
                database="traffic",
            )
        return self._local.c

    def _rows(self, sql, params=None):
        return list(self._c().query(sql, parameters=params or {}).named_results())

    def clock(self):
        r = self._rows("SELECT max(event_time) AS c, count() AS n FROM traffic.reading")
        return r[0]["c"] if r and r[0]["n"] else None

    def links(self):
        return self._rows("SELECT * FROM traffic.dim_link FINAL")

    def latest(self):
        clock = self.clock()
        if clock is None:
            return []
        ff = {r["link_id"]: r["free_flow_speed"] for r in self.links()}
        rows = self._rows("""
            SELECT link_id,
                   argMaxIf(speed, event_time, is_valid = 1)       AS speed,
                   argMaxIf(travel_time, event_time, is_valid = 1) AS travel_time,
                   maxIf(event_time, is_valid = 1)                 AS valid_time,
                   countIf(is_valid = 1)                           AS n_valid,
                   max(event_time)                                 AS obs_time,
                   argMax(is_valid, event_time)                    AS obs_valid
            FROM traffic.reading WHERE event_time > {c:DateTime} - INTERVAL 1 DAY
            GROUP BY link_id
        """, {"c": clock})
        out = []
        for r in rows:
            if not r["n_valid"] or (r["obs_time"] > r["valid_time"] and not r["obs_valid"]):
                state, speed, t = "NO_VALID_DATA", None, (r["valid_time"] if r["n_valid"] else None)
            else:
                age = (clock - r["valid_time"]).total_seconds() / 60
                state, speed, t = classify(r["speed"], ff.get(r["link_id"]), age), r["speed"], r["valid_time"]
            out.append({"link_id": r["link_id"], "speed": speed, "travel_time": r["travel_time"],
                        "event_time": t, "traffic_state": state})
        return out

    def history_15min(self, link_id, start, end, borough=None):
        cond = ""
        if link_id is not None:
            cond = "AND link_id = {id:UInt64}"
        elif borough:
            cond = "AND link_id IN (SELECT link_id FROM traffic.dim_link WHERE borough = {b:String})"
        # alias không được trùng tên cột `speed`: ClickHouse sẽ hiểu minIf(speed) là hàm lồng trong avgIf
        rows = self._rows(f"""
            SELECT toStartOfFifteenMinutes(event_time) AS t,
                   avgIf(speed, is_valid = 1) AS avg_spd, minIf(speed, is_valid = 1) AS min_spd
            FROM traffic.reading
            WHERE event_time >= {{s:DateTime}} AND event_time < {{e:DateTime}} {cond}
            GROUP BY t HAVING countIf(is_valid = 1) > 0 ORDER BY t
        """, {"s": start, "e": end, "id": link_id or 0, "b": borough or ""})
        return [{"t": r["t"], "speed": r["avg_spd"], "min_speed": r["min_spd"]} for r in rows]

    def dow_hour_profile(self, link_id=None, borough=None):
        cond = ""
        if link_id is not None:
            cond = "WHERE link_id = {id:UInt64}"
        elif borough:
            cond = "WHERE link_id IN (SELECT link_id FROM traffic.dim_link WHERE borough = {b:String})"
        return self._rows(f"""
            SELECT toDayOfWeek(toTimeZone(hour_utc, 'America/New_York')) AS dow,
                   toHour(toTimeZone(hour_utc, 'America/New_York')) AS hour,
                   sum(sum_speed) / sum(n_valid) AS speed
            FROM traffic.hourly {cond} GROUP BY dow, hour ORDER BY dow, hour
        """, {"id": link_id or 0, "b": borough or ""})

    def weather_at(self, at):
        h = at.replace(minute=0, second=0, microsecond=0)
        rows = self._rows("""
            SELECT a.cell_id AS cell_id, a.time_utc AS interval_start, a.temperature_c AS temperature_c,
                   a.wind_speed_kmh AS wind_speed_kmh,
                   if(b.cell_id != '', b.precipitation_mm, a.precipitation_mm) AS precipitation_mm,
                   if(b.cell_id != '', b.weather_code, a.weather_code) AS weather_code,
                   if(b.cell_id != '', 'FINAL', 'PROVISIONAL') AS weather_status
            FROM (SELECT * FROM traffic.weather WHERE time_utc = {h:DateTime}) a
            LEFT JOIN (SELECT * FROM traffic.weather WHERE time_utc = {h1:DateTime} AND {h1:DateTime} <= {at:DateTime}) b
                   ON b.cell_id = a.cell_id
        """, {"h": h, "h1": h + timedelta(hours=1), "at": at})
        return rows

    def forecast(self, link_id):
        return self._rows("""
            SELECT target_time AS t, horizon_min, pred_speed AS speed, pred_lower AS lower, pred_upper AS upper,
                   issued_at, model_name, model_version
            FROM traffic.forecast FINAL
            WHERE link_id = {id:UInt64}
              AND issued_at = (SELECT max(issued_at) FROM traffic.forecast WHERE link_id = {id:UInt64})
            ORDER BY target_time
        """, {"id": link_id})

