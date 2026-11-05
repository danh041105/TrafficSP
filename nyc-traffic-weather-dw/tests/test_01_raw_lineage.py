"""(1) so ban ghi khop nguon, (11) truy xuat nguon goc."""
from conftest import ROOT, one
from nyc_dw.utils.common import sha256


def test_raw_checksums_match_manifest(manifest):
    for src in ("nyc_dot", "open_meteo"):
        for f in manifest["sources"][src]["files"]:
            assert sha256(ROOT / f["raw_path"]) == f["sha256"], f["raw_path"]


def test_raw_rows_equal_expected(con, manifest):
    nd = manifest["sources"]["nyc_dot"]
    assert one(con, "SELECT count(*) FROM raw_nyc_dot") == nd["expected_rows"] == nd["rows_total"]


def test_every_fact_row_traceable_to_source_file(con, manifest):
    files = {f["raw_path"].split("/")[-1] for f in manifest["sources"]["nyc_dot"]["files"]}
    got = {r[0] for r in con.execute("SELECT DISTINCT source_file FROM fact_traffic_reading").fetchall()}
    assert got == files
    assert one(con, "SELECT count(*) FROM fact_traffic_reading WHERE load_batch_id IS NULL OR source_file IS NULL") == 0
    wfiles = {f["raw_path"].split("/")[-1] for f in manifest["sources"]["open_meteo"]["files"]}
    assert {r[0] for r in con.execute("SELECT DISTINCT source_file FROM fact_weather_hourly").fetchall()} == wfiles


def test_fact_row_count_per_source_file(con):
    bad = con.execute("""SELECT r.source_file, r.n, f.n FROM (SELECT source_file, count(*) n FROM raw_nyc_dot GROUP BY 1) r
                         FULL JOIN (SELECT source_file, count(*) n FROM fact_traffic_reading GROUP BY 1) f USING (source_file)
                         WHERE r.n IS DISTINCT FROM f.n""").fetchall()
    assert bad == []
