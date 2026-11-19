"""Read-only, sequential Trino EDA runner; standard library only.

Usage: python eda/profile_gold_mapping.py
No table writes, DDL, service changes or credentials are used.
"""
import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
BASE = "http://localhost:8088"


def query(sql):
    if not sql.lstrip().upper().startswith(("SELECT", "WITH")):
        raise ValueError("Only SELECT/WITH is permitted")
    request = urllib.request.Request(
        BASE + "/v1/statement", data=sql.encode(),
        headers={"X-Trino-User": "traffic_sql", "X-Trino-Time-Zone": "UTC"},
    )
    rows, columns, query_id = [], [], None
    while True:
        with urllib.request.urlopen(request, timeout=120) as response:
            page = json.load(response)
        query_id = page.get("id", query_id)
        if "error" in page:
            raise RuntimeError(json.dumps(page["error"], ensure_ascii=False))
        if "columns" in page:
            columns = [c["name"] for c in page["columns"]]
        rows.extend(page.get("data", []))
        if "nextUri" not in page:
            return {"query_id": query_id, "columns": columns, "rows": rows}
        next_url = urllib.parse.urlsplit(page["nextUri"])
        if not next_url.path.startswith("/v1/statement/"):
            raise ValueError("Unexpected Trino nextUri")
        request = urllib.request.Request(
            BASE + next_url.path + ("?" + next_url.query if next_url.query else ""),
            headers={"X-Trino-User": "traffic_sql"},
        )
        time.sleep(0.05)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", nargs="+", help="Run named profiles and merge into existing results")
    args = parser.parse_args()
    source = HERE / "gold_mapping_profile.sql"
    output = HERE / "gold_mapping_profile_results.json"
    results = {"task": "TASK-20261118-007", "started_utc": datetime.now(timezone.utc).isoformat(), "queries": {}}
    if args.only and output.exists():
        results = json.loads(output.read_text(encoding="utf-8"))
    for block in source.read_text(encoding="utf-8").split(";"):
        lines = block.strip().splitlines()
        if not lines:
            continue
        label = next((line[3:].strip() for line in lines if line.startswith("-- ")), "query")
        if args.only and label not in args.only:
            continue
        sql = "\n".join(line for line in lines if not line.lstrip().startswith("--")).strip()
        if not sql:
            continue
        started = time.monotonic()
        try:
            result = query(sql)
            result["elapsed_seconds"] = round(time.monotonic() - started, 2)
            results["queries"][label] = result
            print(label, "PASS", len(result["rows"]), "rows", flush=True)
        except Exception as error:
            results["queries"][label] = {"error": str(error)}
            print(label, "FAIL", str(error)[:250], flush=True)
        output.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    if any("error" in value for value in results["queries"].values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
