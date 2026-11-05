import json, sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from nyc_dw.utils.common import DW, MART, RAW, STG, duck, sql_file  # noqa: E402


@pytest.fixture(scope="session")
def con():
    c = duck()
    c.execute(sql_file("views/create_views.sql", dw=DW, mart=MART))
    raw = str(RAW / "nyc_dot" / "2025-*.parquet").replace("\\", "/")
    c.execute(f"CREATE VIEW raw_nyc_dot AS SELECT *, parse_filename(filename) AS source_file FROM read_parquet('{raw}', filename = true)")
    c.execute(f"CREATE VIEW stg_weather_hourly AS SELECT * FROM read_parquet('{str(STG / 'stg_weather_hourly.parquet').replace(chr(92), '/')}')")
    c.execute(f"CREATE VIEW road_weather_mapping AS SELECT * FROM read_parquet('{str(STG / 'road_weather_mapping.parquet').replace(chr(92), '/')}')")
    yield c
    c.close()


@pytest.fixture(scope="session")
def manifest():
    return json.loads((RAW / "manifest" / "raw_manifest.json").read_text(encoding="utf-8"))


def one(con, sql):
    return con.execute(sql).fetchall()[0][0]
