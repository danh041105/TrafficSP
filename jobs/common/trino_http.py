"""Client Trino tối giản qua REST API (/v1/statement) bằng thư viện chuẩn - dùng ở Airflow (không có trino CLI) và test.
Theo nextUri cho tới khi hết; lỗi truy vấn -> RuntimeError."""
import json
import os
import time
import urllib.request

TRINO_URL = os.getenv("TRINO_URL", "http://trino:8080")
TRINO_USER = os.getenv("TRINO_USER", "trafficsp")


def query(sql: str, url: str = TRINO_URL, user: str = TRINO_USER, timeout: int = 300) -> list[list]:
    return query_with_stats(sql, url, user, timeout)[0]


def query_with_stats(sql: str, url: str = TRINO_URL, user: str = TRINO_USER, timeout: int = 300) -> tuple[list[list], dict]:
    """Trả (rows, stats) - stats là khối 'stats' cuối cùng của Trino (elapsedTimeMillis, physicalInputBytes, ...)."""
    req = urllib.request.Request(f"{url}/v1/statement", data=sql.encode("utf-8"), method="POST",
                                 headers={"X-Trino-User": user, "X-Trino-Catalog": "lakehouse"})
    rows, deadline = [], time.time() + timeout
    with urllib.request.urlopen(req, timeout=60) as r:
        res = json.load(r)
    while True:
        if "error" in res:
            raise RuntimeError(res["error"].get("message", str(res["error"])))
        rows.extend(res.get("data") or [])
        nxt = res.get("nextUri")
        if not nxt:
            return rows, res.get("stats") or {}
        if time.time() > deadline:
            raise TimeoutError(sql[:80])
        with urllib.request.urlopen(urllib.request.Request(nxt, headers={"X-Trino-User": user}), timeout=60) as r:
            res = json.load(r)
