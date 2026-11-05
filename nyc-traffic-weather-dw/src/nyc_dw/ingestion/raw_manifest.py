"""Phase 2 - Sao chep nguyen trang du lieu goc vao data/raw (chi doc), lap manifest va kiem tra toan ven.
Khong sua file goc. Neu ban sao da ton tai voi checksum khac -> dung lai, khong ghi de.
"""
import datetime as dt, glob, json, os, shutil, stat
from pathlib import Path

import pyarrow.parquet as pq

from nyc_dw import config
from nyc_dw.utils.common import PROJECT_ROOT, RAW, REPORTS, batch_id, logger, sha256, write_json

log = logger("phase2_raw")


def _copy_verified(src: Path, dst: Path) -> dict:
    src_sha = sha256(src)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst_sha = sha256(dst)
        if dst_sha != src_sha:
            raise RuntimeError(f"Ban sao {dst} da ton tai nhung checksum khac nguon -> dung, khong ghi de")
        action = "da_co_khop"
    else:
        shutil.copy2(src, dst)
        dst_sha = sha256(dst)
        if dst_sha != src_sha:
            raise RuntimeError(f"Checksum ban sao {dst} khac nguon")
        os.chmod(dst, stat.S_IREAD)          # danh dau chi doc
        action = "sao_chep_moi"
    return {"source_path": str(src).replace("\\", "/"), "raw_path": str(dst.relative_to(PROJECT_ROOT)).replace("\\", "/"),
            "bytes": src.stat().st_size, "sha256": src_sha, "copy_sha256_match": dst_sha == src_sha, "action": action,
            "source_mtime": dt.datetime.fromtimestamp(src.stat().st_mtime).isoformat(timespec="seconds")}


def run() -> dict:
    bid = batch_id("phase2"); log.info("batch %s", bid)
    src = config.sources(); nd, om = src["nyc_dot"], src["open_meteo"]
    man = {"batch_id": bid, "created_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"), "sources": {}}

    # ---- NYC DOT ----
    files = []
    for f in sorted(glob.glob(os.path.join(nd["local_dir"], nd["file_glob"]))):
        e = _copy_verified(Path(f), RAW / "nyc_dot" / Path(f).name)
        t = pq.read_table(RAW / "nyc_dot" / Path(f).name)              # doc toan bo -> kiem tra toan ven
        ts = t.column("data_as_of").to_pandas()
        e.update({"rows": t.num_rows, "columns": t.schema.names, "data_as_of_min": str(ts.min()), "data_as_of_max": str(ts.max()),
                  "readable": True})
        files.append(e); log.info("nyc_dot %s rows=%s %s", Path(f).name, t.num_rows, e["action"])
    zip_path = Path(nd["extraction"]["drive_zip"])
    man["sources"]["nyc_dot"] = {
        "dataset_id": nd["dataset_id"], "portal_url": nd["portal_url"], "api_endpoint": nd["api_endpoint"],
        "extraction": nd["extraction"], "extraction_code": "scripts/provenance/colab_download_nyc_dot_2025.py",
        "drive_zip": {"path": str(zip_path).replace("\\", "/"), "bytes": zip_path.stat().st_size if zip_path.exists() else None,
                      "sha256": sha256(zip_path) if zip_path.exists() else None},
        "expected_rows": nd["expected"]["rows_2025"], "files": files, "rows_total": sum(x["rows"] for x in files)}

    # ---- Open-Meteo ----
    rawdir = Path(om["raw_json_dir"]); dl = {m["weather_location_id"]: m for m in json.loads((rawdir / "_manifest.json").read_text(encoding="utf-8"))}
    wfiles = []
    for f in sorted(rawdir.glob("ifs_*.json")):
        e = _copy_verified(f, RAW / "open_meteo" / om["model"] / f.name)
        b = json.loads((RAW / "open_meteo" / om["model"] / f.name).read_text(encoding="utf-8"))
        h = b["hourly"]; wid = f.stem
        e.update({"weather_location_id": wid, "grid_lat": b["latitude"], "grid_lon": b["longitude"], "elevation": b.get("elevation"),
                  "timezone": b["timezone"], "utc_offset_seconds": b["utc_offset_seconds"], "hours": len(h["time"]),
                  "time_first": h["time"][0], "time_last": h["time"][-1], "variables": [k for k in h if k != "time"],
                  "units": b["hourly_units"], "api_url": dl[wid]["url"], "downloaded_utc": dl[wid]["downloaded_utc"],
                  "nulls": sum(v is None for k, arr in h.items() if k != "time" for v in arr)})
        wfiles.append(e)
    for extra in ["_manifest.json"]:
        _copy_verified(rawdir / extra, RAW / "open_meteo" / om["model"] / extra)
    _copy_verified(Path(om["download_script"]), RAW / "open_meteo" / "download_openmeteo_ifs_2025.py")
    man["sources"]["open_meteo"] = {"api_endpoint": om["api_endpoint"], "docs_url": om["docs_url"], "model": om["model"],
                                    "request_timezone": om["request_timezone"], "date_range": om["date_range"], "files": wfiles,
                                    "rows_total": sum(x["hours"] for x in wfiles), "download_code": "data/raw/open_meteo/download_openmeteo_ifs_2025.py"}
    write_json(man, RAW / "manifest" / "raw_manifest.json")
    return man


def report(man: dict):
    nd, om = man["sources"]["nyc_dot"], man["sources"]["open_meteo"]
    n = lambda x: f"{x:,}".replace(",", " ")
    rows_ok = nd["rows_total"] == nd["expected_rows"]
    wx_ok = all(f["hours"] == 8808 and f["nulls"] == 0 and f["timezone"] == "GMT" for f in om["files"])
    lines = [f"# Raw manifest & đối chiếu (Phase 2)", "", f"Batch `{man['batch_id']}` · tạo lúc {man['created_at']} · manifest: `data/raw/manifest/raw_manifest.json`", "",
             "## Kết luận", "",
             f"| Tiêu chí | Kết quả |", "|---|---|",
             f"| Checksum bản sao = bản gốc (NYC DOT) | {'ĐẠT' if all(f['copy_sha256_match'] for f in nd['files']) else 'KHÔNG ĐẠT'} ({len(nd['files'])}/12 file) |",
             f"| Checksum bản sao = bản gốc (Open-Meteo) | {'ĐẠT' if all(f['copy_sha256_match'] for f in om['files']) else 'KHÔNG ĐẠT'} ({len(om['files'])}/16 file) |",
             f"| Tổng dòng NYC DOT | {n(nd['rows_total'])} / kỳ vọng {n(nd['expected_rows'])} → {'ĐẠT' if rows_ok else 'KHÔNG ĐẠT'} |",
             f"| Open-Meteo: 8 808 giờ/ô, 0 null, timezone GMT | {'ĐẠT' if wx_ok else 'KHÔNG ĐẠT'} (tổng {n(om['rows_total'])}) |",
             f"| Đọc được toàn bộ file | {'ĐẠT' if all(f['readable'] for f in nd['files']) else 'KHÔNG ĐẠT'} |", "",
             "## NYC DOT", "", f"Trích xuất: {nd['extraction']['tool']} · ngày {nd['extraction']['run_date']} · mã: `{nd['extraction_code']}`",
             f"Zip Google Drive: `{nd['drive_zip']['path']}` · {n(nd['drive_zip']['bytes'] or 0)} byte · sha256 `{(nd['drive_zip']['sha256'] or '')[:16]}…`", "",
             "| File | Byte | Dòng | data_as_of min → max | SHA-256 (16 ký tự đầu) | Hành động |", "|---|---|---|---|---|---|"]
    lines += [f"| {Path(f['raw_path']).name} | {n(f['bytes'])} | {n(f['rows'])} | {f['data_as_of_min']} → {f['data_as_of_max']} | `{f['sha256'][:16]}` | {f['action']} |" for f in nd["files"]]
    lines += ["", "## Open-Meteo (ECMWF IFS)", "", f"Mã tải: `{om['download_code']}` · timezone yêu cầu: {om['request_timezone']} · khoảng {om['date_range']}", "",
              "| Ô | lat, lon | Giờ | Đầu → cuối (UTC) | Null | Tải lúc (UTC) | SHA-256 (16) |", "|---|---|---|---|---|---|---|"]
    lines += [f"| {f['weather_location_id']} | {f['grid_lat']}, {f['grid_lon']} | {f['hours']} | {f['time_first']} → {f['time_last']} | {f['nulls']} | {f['downloaded_utc']} | `{f['sha256'][:16]}` |" for f in om["files"]]
    lines += ["", f"Ví dụ câu truy vấn API (ô đầu tiên): `{om['files'][0]['api_url']}`", "",
              "Ghi chú: bản sao trong `data/raw` được đặt thuộc tính chỉ đọc; file gốc ở `D:/DE_dataset_sizing` không bị thay đổi."]
    (REPORTS / "raw_manifest_report.md").write_text("\n".join(lines), encoding="utf-8")
    return rows_ok and wx_ok
