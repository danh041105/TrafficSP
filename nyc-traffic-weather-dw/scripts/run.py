"""Chay tung phase cua pipeline:  python -I scripts/run.py <phase>   (phase: 2..7)"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.stdout.reconfigure(encoding="utf-8")

phase = sys.argv[1] if len(sys.argv) > 1 else ""
if phase == "2":
    from nyc_dw.ingestion import raw_manifest as m
    ok = m.report(m.run()); print("PHASE 2", "OK" if ok else "CO LOI"); sys.exit(0 if ok else 1)
elif phase == "3":
    from nyc_dw.transformation import staging as m
    sys.exit(0 if m.run() else 1)
elif phase == "4":
    from nyc_dw.modeling import dw_atomic as m
    sys.exit(0 if m.run() else 1)
elif phase == "5":
    from nyc_dw.modeling import dw_aggregate as m
    sys.exit(0 if m.run() else 1)
elif phase == "6":
    from nyc_dw.validation import quality as m
    sys.exit(0 if m.run() else 1)
elif phase == "7":
    from nyc_dw.validation import olap_demo as m
    sys.exit(0 if m.run() else 1)
else:
    print(__doc__); sys.exit(2)
