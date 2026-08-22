#!/usr/bin/env python3
"""FMC probe step 5 — batch raster diff over matched pages of selected version steps.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_batch_diff \
        [--px 900] [--max-pages-per-step 60] [--out artifacts/fmc_batch_diff.json] [--docs A,B,...]
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sys
import time
from pathlib import Path

import fitz
import numpy as np
from scipy import ndimage

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"


def _render(pdf: Path, idx: int, px: int) -> np.ndarray:
    doc = fitz.open(pdf)
    page = doc[idx]
    scale = px / max(page.rect.width, page.rect.height)
    pm = page.get_pixmap(matrix=fitz.Matrix(scale, scale), colorspace=fitz.csGRAY, alpha=False)
    arr = np.frombuffer(pm.samples, dtype=np.uint8).reshape(pm.height, pm.width).copy()
    doc.close()
    return arr


def _job(task):
    lp, li, rp, ri, px = task
    try:
        a = _render(ROOT / lp, li, px)
        b = _render(ROOT / rp, ri, px)
    except Exception as exc:  # pragma: no cover
        return {"error": str(exc), "li": li, "ri": ri, "left": lp, "right": rp}
    h, w = min(a.shape[0], b.shape[0]), min(a.shape[1], b.shape[1])
    d = np.abs(a[:h, :w].astype(np.int16) - b[:h, :w].astype(np.int16)) > 60
    frac = float(d.mean())
    ink_l = float((a[:h, :w] < 200).mean())
    ink_r = float((b[:h, :w] < 200).mean())
    dd = ndimage.binary_dilation(d, np.ones((5, 5), bool))
    lab, n = ndimage.label(dd)
    regions = []
    if n:
        for idx, sl in enumerate(ndimage.find_objects(lab), start=1):
            cnt = int((lab[sl] == idx).sum())
            if cnt < 30:
                continue
            y0, y1, x0, x1 = sl[0].start, sl[0].stop, sl[1].start, sl[1].stop
            regions.append({"px": cnt, "bbox_norm": [round(x0 / w, 5), round(y0 / h, 5), round(x1 / w, 5), round(y1 / h, 5)]})
        regions.sort(key=lambda r: -r["px"])
    return {
        "left": lp, "li": li, "right": rp, "ri": ri,
        "changed_frac": round(frac, 6), "ink": [round(ink_l, 5), round(ink_r, 5)],
        "n_regions": len(regions), "top_regions": regions[:6], "grid": [w, h],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--px", type=int, default=900)
    ap.add_argument("--max-pages-per-step", type=int, default=60)
    ap.add_argument("--docs", default="")
    ap.add_argument("--out", default=str(ART / "fmc_batch_diff.json"))
    ap.add_argument("--procs", type=int, default=6)
    a = ap.parse_args()
    from .fmc_io import read_json
    cand = read_json(ART / "fmc_candidates.json")
    wanted = {d for d in a.docs.split(",") if d}
    tasks, meta = [], []
    for st in cand:
        if wanted and st["document"] not in wanted:
            continue
        for m in st["matched"][: a.max_pages_per_step]:
            tasks.append((st["left"]["pdf"], m["i"], st["right"]["pdf"], m["j"], a.px))
            meta.append((st["discipline"], st["document"], st["left"]["version"], st["right"]["version"], m))
    print(f"tasks={len(tasks)}", file=sys.stderr)
    t0 = time.perf_counter()
    with mp.Pool(a.procs) as pool:
        res = pool.map(_job, tasks, chunksize=1)
    rows = []
    for (disc, doc, va, vb, m), r in zip(meta, res):
        r.update({"discipline": disc, "document": doc, "va": va, "vb": vb,
                  "word_jaccard": m["word_jaccard"], "text_identical": m["text_identical"],
                  "num_delta": m["num_added"] + m["num_removed"],
                  "word_delta": m["word_added"] + m["word_removed"],
                  "n_images": m["n_images"], "rect_changed": m["rect_changed"]})
        rows.append(r)
    Path(a.out).write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {a.out} in {time.perf_counter()-t0:.1f}s", file=sys.stderr)


if __name__ == "__main__":
    main()
