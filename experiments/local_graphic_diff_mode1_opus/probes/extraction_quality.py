#!/usr/bin/env python3
"""Dual extraction metric over every block of the benchmark, at two darkness
thresholds.

`dark_thr` decides what counts as "visible ink" in the render.  At 200 only
firmly dark strokes count, and the light-grey underlay layers that AR/OV plans
are full of are treated as background — which shows up as low precision even
though the geometry is real.  At 245 those layers count as visible.  Both are
reported; neither is "the" answer.
"""
from __future__ import annotations

import json
import pathlib
import statistics
import sys
from concurrent.futures import ProcessPoolExecutor

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

from experiments.local_graphic_diff_mode1_opus.probes.run_benchmark import blocks_of  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.m1.quality import extraction_quality  # noqa: E402


def one(p):
    out = {"pair_id": p["pair_id"], "bucket": p["bucket"], "discipline": p["discipline"]}
    try:
        a, b = blocks_of(p)
        for side, blk in (("left", a), ("right", b)):
            for thr in (200, 245):
                q = extraction_quality(blk, dark_thr=thr)
                out[f"{side}_{thr}"] = {k: q[k] for k in
                                        ("precision", "recall", "visible_cells", "predicted_cells",
                                         "missed_big_components", "missed_largest_component",
                                         "segments", "fill_polygons", "invisible_paths",
                                         "segments_dropped_invisible", "text_spans",
                                         "text_as_curves_suspected", "raster_backed")}
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)[:150]
    return out


def main():
    bench = json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]
    rows = []
    with ProcessPoolExecutor(max_workers=8) as ex:
        for r in ex.map(one, bench, chunksize=1):
            rows.append(r)
            print(r["pair_id"], r.get("error") or
                  f"P200={r['left_200']['precision']}/{r['right_200']['precision']} "
                  f"P245={r['left_245']['precision']}/{r['right_245']['precision']} "
                  f"R245={r['left_245']['recall']}/{r['right_245']['recall']}", flush=True)
    def col(thr, key):
        return [r[f"{s}_{thr}"][key] for r in rows if "error" not in r for s in ("left", "right")
                if r[f"{s}_{thr}"][key] is not None]
    summary = {}
    for thr in (200, 245):
        p, rc = col(thr, "precision"), col(thr, "recall")
        summary[f"thr_{thr}"] = {
            "blocks": len(p),
            "precision": {"min": min(p), "median": round(statistics.median(p), 4),
                          "below_0_95": sum(1 for v in p if v < 0.95)},
            "recall": {"min": min(rc), "median": round(statistics.median(rc), 4),
                       "below_0_95": sum(1 for v in rc if v < 0.95)},
            "blocks_with_big_missed_component": sum(
                1 for r in rows if "error" not in r for s in ("left", "right")
                if r[f"{s}_{thr}"]["missed_big_components"] > 0),
        }
    inv = [r[f"{s}_200"]["segments_dropped_invisible"] for r in rows if "error" not in r for s in ("left", "right")]
    seg = [r[f"{s}_200"]["segments"] for r in rows if "error" not in r for s in ("left", "right")]
    summary["invisible_ink"] = {
        "blocks_with_invisible_paths": sum(1 for r in rows if "error" not in r for s in ("left", "right")
                                           if r[f"{s}_200"]["invisible_paths"] > 0),
        "segments_dropped_total": sum(inv),
        "segments_kept_total": sum(seg),
        "dropped_share": round(sum(inv) / max(1, sum(inv) + sum(seg)), 4),
    }
    (ART / "extraction_quality.json").write_text(json.dumps(
        {"probe": "extraction_quality", "research_only": True, "summary": summary, "blocks": rows},
        ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
