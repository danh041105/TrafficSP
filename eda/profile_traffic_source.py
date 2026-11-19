"""Read-only source EDA for Silver-to-Gold mapping (TASK-20261118-007).

Reads bounded Parquet batches for January-September 2025 and writes a JSON
report. It never writes lakehouse data. Geometry lengths are diagnostics,
not approved road lengths. Encoded polylines are inspected literally; no
backslash normalization, geometry repair or source mutation is performed.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


METADATA = ["link_name", "owner", "borough", "transcom_id", "link_points"]
POINT = re.compile(r"^-?\d+\.\d+,-?\d+\.\d+$")


def segment_lengths(points):
    distances = []
    for (lat1, lon1), (lat2, lon2) in zip(points, points[1:]):
        a, b, c, d = map(math.radians, (lat1, lon1, lat2, lon2))
        hav = math.sin((c-a)/2)**2 + math.cos(a)*math.cos(c)*math.sin((d-b)/2)**2
        distances.append(6371000 * 2 * math.asin(min(1, math.sqrt(max(0, hav)))))
    return distances


def geometry_diagnostics(raw):
    raw = raw or ""
    tokens = raw.strip().split()
    if len(raw) >= 255:
        tokens = tokens[:-1]  # Match current clean_link_points behavior.
    points = [tuple(map(float, p.split(","))) for p in tokens if POINT.fullmatch(p)]
    distances = segment_lengths(points)
    return {
        "raw_chars": len(raw),
        "current_silver_point_count": len(points),
        "coordinate_bounds_ok": all(-90 <= a <= 90 and -180 <= b <= 180 for a, b in points),
        "diagnostic_silver_geometry_length_m": round(sum(distances), 3),
        "largest_segment_m": round(max(distances, default=0), 3),
    }


def profile(source_dir):
    links = {}
    months = []
    columns = ["link_id", *METADATA, "encoded_poly_line", "encoded_poly_line_lvls", "status", "data_as_of"]
    for source in sorted(source_dir.glob("2025-0[1-9].parquet")):
        month = {"month": source.stem, "rows": 0, "retained_rows": 0}
        for batch in pq.ParquetFile(source).iter_batches(batch_size=65536, columns=columns):
            table = pa.Table.from_batches([batch])
            month["rows"] += len(table)
            keep = pc.fill_null(pc.not_equal(table["status"], "-101"), True)
            month["retained_rows"] += pc.sum(pc.cast(keep, pa.int64())).as_py()
            groups = table.group_by(["link_id", *METADATA, "status"]).aggregate(
                [("data_as_of", "min"), ("data_as_of", "max"), ("link_id", "count")]
            )
            for row in groups.to_pylist():
                link_id = row["link_id"]
                state = links.setdefault(link_id, {
                    "rows": 0, "retained_rows": 0, "statuses": {},
                    "metadata_versions": {}, "encodings": set(), "levels": set(),
                })
                count = row["link_id_count"]
                state["rows"] += count
                retained = row["status"] != "-101"
                state["retained_rows"] += count if retained else 0
                status = str(row["status"])
                state["statuses"][status] = state["statuses"].get(status, 0) + count
                key = tuple(row[c] for c in METADATA)
                version = state["metadata_versions"].setdefault(key, {
                    "first_source_local_ts": row["data_as_of_min"],
                    "last_source_local_ts": row["data_as_of_max"],
                    "rows": 0, "retained_rows": 0,
                })
                version["first_source_local_ts"] = min(version["first_source_local_ts"], row["data_as_of_min"])
                version["last_source_local_ts"] = max(version["last_source_local_ts"], row["data_as_of_max"])
                version["rows"] += count
                version["retained_rows"] += count if retained else 0
            encodings = table.group_by(["link_id", "encoded_poly_line", "encoded_poly_line_lvls"]).aggregate([])
            for row in encodings.to_pylist():
                state = links[row["link_id"]]
                state["encodings"].add(row["encoded_poly_line"])
                state["levels"].add(row["encoded_poly_line_lvls"])
        months.append(month)
        print(f"{source.stem}: {month['rows']:,} rows; {month['retained_rows']:,} retained", flush=True)

    result_links = []
    for link_id, state in sorted(links.items()):
        encodings = state.pop("encodings")
        levels = state.pop("levels")
        metadata = state.pop("metadata_versions")
        versions = []
        for values, version in sorted(metadata.items(), key=lambda item: item[1]["first_source_local_ts"]):
            version = dict(version)
            version.update(zip(METADATA, values))
            version["geometry_diagnostics"] = geometry_diagnostics(version["link_points"])
            versions.append(version)
        result_links.append({
            "link_id": link_id, **state, "metadata_versions": versions,
            "distinct_encoded_poly_line": len(encodings),
            "distinct_encoded_poly_line_lvls": len(levels),
            "max_literal_backslash_run": max(
                (len(run) for value in encodings if value for run in re.findall(r"\\+", value)), default=0),
        })
    return {
        "task": "TASK-20261118-007",
        "scope": "Raw NYC DOT Jan-Sep 2025; cross-check against runtime Silver before using counts",
        "filter": "status IS NULL OR status <> '-101'; all metadata retained for dimension diagnostics",
        "geometry_policy": "Haversine diagnostics on current Silver cleaning behavior; lengths not approved, no encoding repair",
        "rows": sum(m["rows"] for m in months),
        "retained_rows": sum(m["retained_rows"] for m in months),
        "distinct_links": len(result_links),
        "links_with_retained_readings": sum(l["retained_rows"] > 0 for l in result_links),
        "removed_only_links": [l["link_id"] for l in result_links if l["retained_rows"] == 0],
        "months": months,
        "links": result_links,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=Path("data/raw/nyc_dot"))
    parser.add_argument("--output", type=Path, default=Path("eda/traffic_source_profile.json"))
    args = parser.parse_args()
    result = profile(args.source_dir)
    if not result["months"]:
        parser.error("No January-September 2025 source files found")
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "links"}, ensure_ascii=False))
    print(f"Metadata versions: {sum(len(l['metadata_versions']) for l in result['links'])}")


if __name__ == "__main__":
    main()
