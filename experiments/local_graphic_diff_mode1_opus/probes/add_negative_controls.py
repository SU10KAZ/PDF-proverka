#!/usr/bin/env python3
"""Find the negative controls the brief demands: text-only and table-only pairs.

A pair is a text-only candidate when the whole visible difference between the two
renders sits inside text boxes.  A table-only candidate is a text-only candidate
whose block is a ruled table (most of the ink is long axis-aligned rules and the
block is full of text).

These are appended to the benchmark as separate buckets; the labels still come
from a human looking at the renders.
"""
from __future__ import annotations

import json
import pathlib
import sys
from concurrent.futures import ProcessPoolExecutor

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

from experiments.local_graphic_diff_mode1_opus.probes.select_benchmark import text_change_share  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.probes.gt_tool import block_of  # noqa: E402
from experiments.local_graphic_diff_mode1_opus.m1.core import extract_ink, ink_length  # noqa: E402


def table_score(p) -> float:
    b = block_of(p["pdf_right"], p["page_index_right"], p["bbox_right"])
    ink = extract_ink(b)
    s = ink["segments"]
    if len(s) == 0:
        return 0.0
    dx = np.abs(s[:, 2] - s[:, 0]); dy = np.abs(s[:, 3] - s[:, 1])
    L = np.hypot(dx, dy)
    axis = ((dy < 0.4) | (dx < 0.4)) & (L > 20)
    return float(L[axis].sum() / max(1e-6, L.sum()))


def one(p):
    try:
        share = text_change_share(p)
        out = {"key": (p["doc"], p["block_left"]), "share": share}
        if share is not None and share >= 0.9:
            out["table_score"] = round(table_score(p), 4)
        return out
    except Exception as e:  # noqa: BLE001
        return {"key": (p["doc"], p["block_left"]), "error": str(e)[:100]}


def main() -> None:
    sig = json.loads((ART / "pair_signals.json").read_text(encoding="utf-8"))["pairs"]
    ps = [p for p in sig if "error" not in p and p.get("raster")]
    for p in ps:
        p["cf"] = p["raster"]["changed_fraction"]
        p["max_ink"] = max(p["ink_pt"])
    cand = [p for p in ps if 0 < p["cf"] <= 0.05 and p["max_ink"] > 500 and max(p["texts"]) > 20]
    print("candidates", len(cand), flush=True)
    res = {}
    with ProcessPoolExecutor(max_workers=10) as ex:
        for r in ex.map(one, cand, chunksize=4):
            res[tuple(r["key"])] = r
    hits = [(p, res[(p["doc"], p["block_left"])]) for p in cand
            if res.get((p["doc"], p["block_left"]), {}).get("share") is not None
            and res[(p["doc"], p["block_left"])]["share"] >= 0.95]
    hits.sort(key=lambda t: (-t[1]["share"], -t[0]["max_ink"]))
    print("text-only candidates", len(hits))

    bench = json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))
    have = {(r["doc"], r["block_left"]) for r in bench["pairs"]}
    docs_used = {}
    added = {"text_only": 0, "table_only": 0}
    rows = []
    for p, r in hits:
        key = (p["doc"], p["block_left"])
        if key in have:
            continue
        ts = r.get("table_score", 0.0)
        bucket = "table_only" if ts >= 0.6 else "text_only"
        if added[bucket] >= 4:
            continue
        if docs_used.get(p["doc"], 0) >= 2:
            continue
        docs_used[p["doc"]] = docs_used.get(p["doc"], 0) + 1
        added[bucket] += 1
        rows.append({
            "pair_id": f"{p['discipline'].lower()}_{bucket}_{len(bench['pairs'])+len(rows)+1:02d}",
            "bucket": bucket, "origin": "real_revision_pair",
            "stage_claim": "revision of the same document (RD); NOT a documented P->RD pair",
            "doc": p["doc"], "discipline": p["discipline"],
            "version_left": p["version_left"], "version_right": p["version_right"],
            "pdf_left": p["pdf_left"], "pdf_right": p["pdf_right"],
            "page_index_left": p["page_index_left"], "page_index_right": p["page_index_right"],
            "sheet_no": p["sheet_no"], "sheet_name": p["sheet_name"],
            "block_left": p["block_left"], "block_right": p["block_right"],
            "bbox_left": p["bbox_left"], "bbox_right": p["bbox_right"], "bbox_iou": p["bbox_iou"],
            "label": p["label_right"] or p["label_left"],
            "signals": {"raster_changed_fraction": p["cf"], "ink_pt": p["ink_pt"],
                        "segments": p["segments"], "texts": p["texts"],
                        "page_rotation": p["page_rotation"], "page_images": p["page_images"],
                        "width_pt": p["width_pt"], "height_pt": p["height_pt"],
                        "text_change_share": r["share"], "table_score": ts},
        })
        if added["text_only"] >= 4 and added["table_only"] >= 4:
            break
    bench["pairs"].extend(rows)
    bench["n"] = len(bench["pairs"])
    (ART / "benchmark_pairs.json").write_text(json.dumps(bench, ensure_ascii=False, indent=1), encoding="utf-8")
    print("added", added, "total", bench["n"])


if __name__ == "__main__":
    main()
