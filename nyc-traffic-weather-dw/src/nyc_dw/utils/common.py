"""Tien ich dung chung: duong dan, ket noi DuckDB (UTC), batch id, log, ghi an toan co rollback."""
import contextlib, datetime as dt, hashlib, json, logging, shutil
from pathlib import Path

import duckdb

from nyc_dw.config import PROJECT_ROOT, load

SETTINGS = load("settings")
P = {k: PROJECT_ROOT / v for k, v in SETTINGS["paths"].items()}
DATA = PROJECT_ROOT / "data"
RAW = DATA / "raw"
STG = DATA / "staging"
DW = DATA / "dw"
MART = DATA / "mart"
QUAL = DATA / "quality"
REPORTS = PROJECT_ROOT / "reports"
SQL = PROJECT_ROOT / "sql"


def batch_id(phase: str) -> str:
    return f"{phase}-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"


def logger(name: str) -> logging.Logger:
    (REPORTS / "logs").mkdir(parents=True, exist_ok=True)
    lg = logging.getLogger(name)
    if not lg.handlers:
        lg.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh = logging.FileHandler(REPORTS / "logs" / f"{name}.log", encoding="utf-8"); fh.setFormatter(fmt)
        sh = logging.StreamHandler(); sh.setFormatter(fmt)
        lg.addHandler(fh); lg.addHandler(sh)
    return lg


def duck(db: str | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(db or ":memory:")
    con.execute("SET TimeZone='UTC'")
    return con


def sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(chunk):
            h.update(b)
    return h.hexdigest()


def write_json(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, ensure_ascii=False, default=str), encoding="utf-8")


@contextlib.contextmanager
def atomic_output(target: Path):
    """Ghi vao <target>.tmp; neu thanh cong: <target> cu -> <target>.prev (rollback), .tmp -> <target>."""
    tmp = target.with_name(target.name + ".tmp")
    prev = target.with_name(target.name + ".prev")
    if tmp.exists():
        shutil.rmtree(tmp) if tmp.is_dir() else tmp.unlink()
    yield tmp
    if prev.exists():
        shutil.rmtree(prev) if prev.is_dir() else prev.unlink()
    if target.exists():
        target.rename(prev)
    tmp.rename(target)


def sql_file(rel: str, **params) -> str:
    """Doc file SQL va thay {placeholder} bang duong dan (dau '/' de DuckDB doc tren Windows)."""
    text = (SQL / rel).read_text(encoding="utf-8")
    return text.format(**{k: (str(v).replace("\\", "/") if isinstance(v, Path) else v) for k, v in params.items()})
