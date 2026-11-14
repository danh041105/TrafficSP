"""Tiện ích chung cho các file demo từng bước (chạy bằng nút ▷ Run trong VS Code, trên máy host).
Chỉ ĐỌC dữ liệu, trừ bước 1 (phát dữ liệu vào Kafka) và bước 4 (bấm chạy DAG Airflow)."""
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "jobs"))
from common.trino_http import query  # noqa: E402

try:                                   # terminal Windows: in tiếng Việt không lỗi
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
os.environ["PATH"] = r"D:\Program\resources\bin;" + os.environ.get("PATH", "")
TRINO = os.getenv("TRINO_URL", "http://localhost:8088")
COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.dev.yml", "--profile", "tools"]


def title(text: str):
    print("\n" + "=" * 78 + f"\n  {text}\n" + "=" * 78, flush=True)


def say(text: str):
    print(f"\n▶ {text}", flush=True)


def tq(sql: str) -> list[list]:
    return query(sql, url=TRINO, user="demo")


def _cell(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.1f}"
    s = str(v)
    if len(s) >= 19 and s[4] == "-" and s[10] == " " and s[13] == ":":   # timestamp -> "YYYY-MM-DD HH:MM:SS"
        return s[:19]
    return s


def table(rows, headers):
    rows = [[_cell(v) for v in r] for r in rows]
    w = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    print("  " + " │ ".join(h.ljust(w[i]) for i, h in enumerate(headers)))
    print("  " + "─┼─".join("─" * x for x in w))
    for r in rows:
        print("  " + " │ ".join(r[i].ljust(w[i]) for i in range(len(headers))))
    print(flush=True)


def run(cmd: list[str], show=True) -> str:
    """Chạy lệnh, in từng dòng ra màn hình ngay khi có."""
    p = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                         encoding="utf-8", errors="replace")
    out = []
    for line in p.stdout:
        out.append(line)
        if show:
            print("   " + line.rstrip(), flush=True)
    p.wait()
    return "".join(out)


def kafka_total() -> int:
    out = run(["docker", "exec", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "kafka:29092",
               "--topic", "traffic_raw"], show=False)
    return sum(int(l.rsplit(":", 1)[1]) for l in out.splitlines() if l.count(":") == 2)


def short(ts) -> str:
    return str(ts)[:16] if ts else "–"


def pause(sec: float):
    time.sleep(sec)
