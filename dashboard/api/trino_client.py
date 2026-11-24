"""Client Trino REST tối giản (thư viện chuẩn) cho dashboard: trả về danh sách dict theo tên cột.

Chỉ đọc. Mỗi truy vấn của dashboard đều có điều kiện thời gian / LIMIT để không quét cả bảng.
"""
import json
import os
import time
import urllib.request

TRINO_URL = os.getenv("TRINO_URL", "http://trino:8080")
TRINO_USER = os.getenv("TRINO_USER", "dashboard")


class TrinoError(RuntimeError):
    pass


def query(sql: str, timeout: int = 60) -> list[dict]:
    req = urllib.request.Request(f"{TRINO_URL}/v1/statement", data=sql.encode("utf-8"), method="POST",
                                 headers={"X-Trino-User": TRINO_USER, "X-Trino-Catalog": "lakehouse",
                                          "X-Trino-Source": "trafficsp-dashboard",
                                          # chặn truy vấn chạy quá lâu ngay trên Trino (không để chồng chất chiếm heap)
                                          "X-Trino-Session": "query_max_run_time=90s"})
    deadline = time.time() + timeout
    cols, rows = None, []
    with urllib.request.urlopen(req, timeout=30) as r:
        res = json.load(r)
    while True:
        if "error" in res:
            raise TrinoError(res["error"].get("message", str(res["error"])))
        if cols is None and res.get("columns"):
            cols = [c["name"] for c in res["columns"]]
        rows.extend(res.get("data") or [])
        nxt = res.get("nextUri")
        if not nxt:
            break
        if time.time() > deadline:
            raise TrinoError(f"Trino quá {timeout}s: {sql[:80]}")
        with urllib.request.urlopen(urllib.request.Request(nxt, headers={"X-Trino-User": TRINO_USER}), timeout=30) as r:
            res = json.load(r)
    cols = cols or []
    return [dict(zip(cols, row)) for row in rows]
