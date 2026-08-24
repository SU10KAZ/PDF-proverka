#!/usr/bin/env python3
"""Honest test of the cheap raster prefilter (§14).

Question: can a coarse raster comparison throw away the obviously identical
parts of a block before the vector diff runs — without ever becoming the source
of truth?

Method: the common area is tiled (128 pt tiles).  Both sides are rendered at a
coarse resolution and compared with a tolerance; a tile with no raster
difference is declared "skippable".  Then two things are measured:

* SAFETY   — does any GT change region fall inside a skipped tile?
* PAYOFF   — what share of the block's area/ink the prefilter removes, and what
             it costs in seconds compared with the vector path.

Vector provenance is untouched: the prefilter only proposes what NOT to look at.
"""
from __future__ import annotations

import json
import math
import pathlib
import sys
import time

import cv2
import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

from experiments.local_graphic_diff_mode1_opus.m1.core import render_gray  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.probes.run_benchmark import blocks_of  # noqa: E402

TILE_PT = 128.0
DARK = 200


def prefilter(a, b, cell=1.5, tol_px=1):
    t0 = time.time()
    ga, gb = render_gray(a, cell), render_gray(b, cell)
    h = min(ga.shape[0], gb.shape[0]); w = min(ga.shape[1], gb.shape[1])
    A = (ga[:h, :w] < DARK).astype(np.uint8)
    B = (gb[:h, :w] < DARK).astype(np.uint8)
    try:
        win = cv2.createHanningWindow((w, h), cv2.CV_32F)
        (dx, dy), _ = cv2.phaseCorrelate(A.astype(np.float32) * win, B.astype(np.float32) * win)
        A = cv2.warpAffine(A, np.float32([[1, 0, dx], [0, 1, dy]]), (w, h), flags=cv2.INTER_NEAREST)
    except Exception:
        dx = dy = 0.0
    ker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * tol_px + 1, 2 * tol_px + 1))
    diff = ((A & ~cv2.dilate(B, ker)) | (B & ~cv2.dilate(A, ker))).astype(np.uint8)
    step = int(round(TILE_PT / cell))
    tiles = []
    for ty in range(0, h, step):
        for tx in range(0, w, step):
            sub = diff[ty:ty + step, tx:tx + step]
            ink = int(A[ty:ty + step, tx:tx + step].sum()) + int(B[ty:ty + step, tx:tx + step].sum())
            tiles.append({
                "px": [tx, ty, min(w, tx + step), min(h, ty + step)],
                # the left render is warped onto the right one, so the tile grid
                # lives in the RIGHT block's frame
                "pt": [b.bbox_vis[0] + tx * cell, b.bbox_vis[1] + ty * cell,
                       b.bbox_vis[0] + min(w, tx + step) * cell, b.bbox_vis[1] + min(h, ty + step) * cell],
                "diff_cells": int(sub.sum()), "ink_cells": ink,
            })
    return {"tiles": tiles, "shift_px": [round(float(dx), 2), round(float(dy), 2)],
            "shape": [h, w], "cell_pt": cell, "elapsed_s": round(time.time() - t0, 3),
            "diff_cells_total": int(diff.sum()), "ink_cells_total": int(A.sum()) + int(B.sum())}


def sweep():
    """Trade-off: how safe is the prefilter at different tolerances."""
    gt = {r["pair_id"]: r for r in json.loads((ART / "human_ground_truth.json").read_text(encoding="utf-8"))["pairs"]}
    bench = {p["pair_id"]: p for p in json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]}
    out = []
    for tol in (0, 1, 2):
        for cell in (1.5, 0.8):
            inked = skip = missed = checked = 0
            secs = 0.0
            for pid, p in bench.items():
                try:
                    a, b = blocks_of(p)
                    pf = prefilter(a, b, cell=cell, tol_px=tol)
                except Exception:
                    continue
                secs += pf["elapsed_s"]
                tiles = [t for t in pf["tiles"] if t["ink_cells"] > 0]
                sk = [t for t in tiles if t["diff_cells"] == 0]
                inked += len(tiles); skip += len(sk)
                for r in gt[pid]["gt_regions"]:
                    if (r.get("text_share") or 0.0) >= 0.5:
                        continue
                    checked += 1
                    bb = r["bbox_pt"]
                    cx, cy = (bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2
                    if any(t["pt"][0] <= cx <= t["pt"][2] and t["pt"][1] <= cy <= t["pt"][3] for t in sk):
                        missed += 1
            row = {"tol_px": tol, "cell_pt": cell, "tolerance_pt": round(tol * cell, 2),
                   "tiles_with_ink": inked, "tiles_skippable": skip,
                   "skippable_share": round(skip / max(1, inked), 3),
                   "gt_regions_checked": checked, "gt_regions_lost": missed,
                   "loss_rate": round(missed / max(1, checked), 4),
                   "seconds": round(secs, 1)}
            out.append(row)
            print(row, flush=True)
    (ART / "prefilter_sweep.json").write_text(json.dumps(
        {"probe": "prefilter.sweep", "research_only": True, "rows": out}, ensure_ascii=False, indent=1),
        encoding="utf-8")


def main():
    gt = {r["pair_id"]: r for r in json.loads((ART / "human_ground_truth.json").read_text(encoding="utf-8"))["pairs"]}
    bench = {p["pair_id"]: p for p in json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]}
    res = {r["pair_id"]: r for r in json.loads((ART / "local_diff_results.json").read_text(encoding="utf-8"))["results"]}
    rows = []
    for pid, p in bench.items():
        try:
            a, b = blocks_of(p)
            pf = prefilter(a, b)
        except Exception as e:  # noqa: BLE001
            rows.append({"pair_id": pid, "error": str(e)[:120]})
            continue
        tiles = pf["tiles"]
        inked = [t for t in tiles if t["ink_cells"] > 0]
        skip = [t for t in inked if t["diff_cells"] == 0]
        # safety: does a GT region (right-side coordinates) fall in a skipped tile?
        missed = 0
        for r in gt[pid]["gt_regions"]:
            if (r.get("text_share") or 0.0) >= 0.5:
                continue
            bb = r["bbox_pt"]
            cx, cy = (bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2
            for t in skip:
                tp = t["pt"]
                if tp[0] <= cx <= tp[2] and tp[1] <= cy <= tp[3]:
                    missed += 1
                    break
        r0 = res.get(pid, {})
        rows.append({
            "pair_id": pid, "bucket": gt[pid]["bucket"], "gt_label": gt[pid]["human_label"],
            "tiles": len(tiles), "tiles_with_ink": len(inked), "tiles_skippable": len(skip),
            "skippable_share": round(len(skip) / max(1, len(inked)), 3),
            "ink_share_skippable": round(sum(t["ink_cells"] for t in skip) / max(1, sum(t["ink_cells"] for t in inked)), 3),
            "gt_regions_checked": sum(1 for r in gt[pid]["gt_regions"] if (r.get("text_share") or 0.0) < 0.5),
            "gt_regions_inside_skipped_tile": missed,
            "prefilter_s": pf["elapsed_s"],
            "vector_s": r0.get("latency_s"),
            "speedup_if_perfect": round((r0.get("latency_s") or 0) / max(1e-3, pf["elapsed_s"]), 2),
        })
        print(f"{pid:28s} tiles={len(inked):>4} skip={len(skip):>4} ({rows[-1]['skippable_share']:.2f}) "
              f"missed_gt={missed} pf={pf['elapsed_s']:.2f}s vec={r0.get('latency_s')}s", flush=True)
    inked_tot = sum(r.get("tiles_with_ink", 0) for r in rows)
    skip_tot = sum(r.get("tiles_skippable", 0) for r in rows)
    missed_tot = sum(r.get("gt_regions_inside_skipped_tile", 0) for r in rows)
    checked_tot = sum(r.get("gt_regions_checked", 0) for r in rows)
    summary = {
        "tiles_with_ink": inked_tot, "tiles_skippable": skip_tot,
        "skippable_share": round(skip_tot / max(1, inked_tot), 3),
        "gt_regions_checked": checked_tot, "gt_regions_lost_to_prefilter": missed_tot,
        "prefilter_seconds_total": round(sum(r.get("prefilter_s", 0) for r in rows), 1),
        "vector_seconds_total": round(sum(r.get("vector_s") or 0 for r in rows), 1),
    }
    (ART / "prefilter_results.json").write_text(json.dumps(
        {"probe": "prefilter", "research_only": True, "tile_pt": TILE_PT,
         "summary": summary, "rows": rows}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "sweep":
        sweep()
    else:
        main()
