#!/usr/bin/env python3
"""Run MODE 1 over the benchmark and store one local_graphic_diff.json per pair.

Also measures the dual extraction metric (precision AND completeness) for both
sides of every pair, because a diff on top of incomplete geometry is worthless
no matter how good the diff looks.
"""
from __future__ import annotations

import json
import pathlib
import sys
import time
from concurrent.futures import ProcessPoolExecutor

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

from experiments.local_graphic_diff_mode1_opus.m1.core import PAGES, block_from_record  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.m1.diff import local_diff  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.m1.quality import extraction_quality  # noqa: E402


def blocks_of(p):
    la = PAGES.page(p["pdf_left"], p["page_index_left"])["rect"]
    rb = PAGES.page(p["pdf_right"], p["page_index_right"])["rect"]
    a = block_from_record(p["pdf_left"], {"coords_norm": p["bbox_left"], "page_index": p["page_index_left"],
                                          "id": p["block_left"], "ocr_label": p.get("label", "")}, la)
    b = block_from_record(p["pdf_right"], {"coords_norm": p["bbox_right"], "page_index": p["page_index_right"],
                                           "id": p["block_right"], "ocr_label": p.get("label", "")}, rb)
    return a, b


def one(p):
    pid = p["pair_id"]
    try:
        a, b = blocks_of(p)
        t0 = time.time()
        out = local_diff(a, b, allow_rotation=True)
        el = time.time() - t0
        out["pair_id"] = pid
        out["bucket"] = p["bucket"]
        out["discipline"] = p["discipline"]
        out["latency_s"] = round(el, 3)
        t1 = time.time()
        qa = extraction_quality(a)
        qb = extraction_quality(b)
        out["extraction_quality"] = {"left": qa, "right": qb, "latency_s": round(time.time() - t1, 3)}
        (ART / "diff_runs").mkdir(parents=True, exist_ok=True)
        (ART / "diff_runs" / f"{pid}.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
        return out
    except Exception as e:  # noqa: BLE001
        import traceback
        return {"pair_id": pid, "bucket": p["bucket"], "error": f"{type(e).__name__}: {e}",
                "trace": traceback.format_exc()[-1500:]}


def main():
    bench = json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]
    only = set(sys.argv[1:]) or None
    todo = [p for p in bench if not only or p["pair_id"] in only]
    res = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=8) as ex:
        for r in ex.map(one, todo, chunksize=1):
            res.append(r)
            if "error" in r:
                print(f"{r['pair_id']:28s} ERROR {r['error'][:90]}", flush=True)
            else:
                d = r["diff"]; reg = r["registration"]
                print(f"{r['pair_id']:28s} {r['route']:22s} {r['verdict']:20s} "
                      f"cov={reg['coverage']['sym_cov']:.3f} regions={d['n_regions_published']:>3} "
                      f"chg={d['changed_ink_fraction']:.4f} {r['latency_s']:.1f}s", flush=True)
    slim = []
    for r in res:
        if "error" in r:
            slim.append(r)
            continue
        slim.append({k: r[k] for k in ("pair_id", "bucket", "discipline", "mode", "route", "route_reason",
                                       "verdict", "latency_s", "registration", "diff", "extraction",
                                       "change_regions", "extraction_quality")})
    (ART / "local_diff_results.json").write_text(json.dumps(
        {"probe": "run_benchmark", "research_only": True, "n": len(slim),
         "elapsed_s": round(time.time() - t0, 1), "results": slim}, ensure_ascii=False), encoding="utf-8")
    print("done", len(res), f"{time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
