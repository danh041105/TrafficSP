"""Doc cau hinh YAML; cho phep bien moi truong ghi de duong dan nguon."""
import os
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load(name: str) -> dict:
    with open(PROJECT_ROOT / "config" / f"{name}.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def sources() -> dict:
    s = load("data_sources")
    s["nyc_dot"]["local_dir"] = os.environ.get("NYC_DOT_DIR", s["nyc_dot"]["local_dir"])
    s["open_meteo"]["raw_json_dir"] = os.environ.get("OPEN_METEO_RAW_DIR", s["open_meteo"]["raw_json_dir"])
    s["open_meteo"]["staging_parquet"] = os.environ.get("OPEN_METEO_STAGING", s["open_meteo"]["staging_parquet"])
    return s
