"""Đọc conf/ml/forecast.yaml. Dùng PyYAML nếu có; container Spark không có PyYAML nên có bộ đọc dự phòng cho tập con YAML
mà file cấu hình dùng (dict lồng nhau theo thụt lề, giá trị vô hướng, list inline). test_ml_config.py so khớp hai cách đọc."""
import os
from pathlib import Path

_CANDIDATES = [
    os.getenv("ML_CONFIG", ""),
    str(Path(__file__).resolve().parents[2] / "conf" / "ml" / "forecast.yaml"),   # repo (host, máy train)
    "/opt/ml_conf/forecast.yaml",                                                   # container Spark (docker cp)
]


def _scalar(s: str):
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        return [_scalar(x) for x in inner.split(",")] if inner else []
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        return s[1:-1]
    if s in ("true", "True"):
        return True
    if s in ("false", "False"):
        return False
    if s in ("null", "~", ""):
        return None
    for cast in (int, float):
        try:
            return cast(s)
        except ValueError:
            pass
    return s


def parse_yaml_subset(text: str) -> dict:
    root: dict = {}
    stack = [(-1, root)]
    for raw in text.splitlines():
        line = raw.split(" #", 1)[0].rstrip() if not raw.lstrip().startswith("#") else ""
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        key, _, value = line.strip().partition(":")
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value.strip() == "":
            parent[key] = {}
            stack.append((indent, parent[key]))
        else:
            parent[key] = _scalar(value)
    return root


def load_config(path: str | None = None) -> dict:
    for p in ([path] if path else []) + _CANDIDATES:
        if p and Path(p).is_file():
            text = Path(p).read_text(encoding="utf-8")
            try:
                import yaml
                return yaml.safe_load(text)
            except ImportError:
                return parse_yaml_subset(text)
    raise FileNotFoundError("không tìm thấy conf/ml/forecast.yaml (đặt ML_CONFIG=...)")
