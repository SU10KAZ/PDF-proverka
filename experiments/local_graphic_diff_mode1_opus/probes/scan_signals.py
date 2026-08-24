#!/usr/bin/env python3
"""Cheap per-pair signals over all mined candidates.

Two independent channels, deliberately kept apart:

* VECTOR  — ink length, segment counts, text spans, fills, page rotation;
* RASTER  — the cheap structural prefilter under test (§14 of the brief): the
  two prepared crops are rendered at a coarse physical resolution and compared
  after a translation-only alignment.  It is measured here as a *candidate*
  filter, never as the source of truth.

Output: artifacts/pair_signals.json
"""
from __future__ import annotations

import json
import math
import pathlib
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import cv2
import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

from experiments.local_graphic_diff_mode1_opus.m1.core import (  # noqa: E402
    PAGES, Block, block_from_record, extract_ink, ink_length, render_gray, text_spans)

CELL = 1.5          # pt per pixel for the cheap raster channel (~17 dpi)
DARK = 200


def _block(pdf: str, page_index: int, bbox_norm, bid: str, label: str) -> Block:
    rect = PAGES.page(pdf, page_index)["rect"]
    return block_from_record(pdf, {"coords_norm": bbox_norm, "page_index": page_index,
                                   "id": bid, "ocr_label": label}, rect)


def raster_signal(a: Block, b: Block, cell=CELL):
    ga = render_gray(a, cell)
    gb = render_gray(b, cell)
    h = min(ga.shape[0], gb.shape[0]); w = min(ga.shape[1], gb.shape[1])
    if h < 8 or w < 8:
        return None
    A = (ga[:h, :w] < DARK).astype(np.uint8)
    B = (gb[:h, :w] < DARK).astype(np.uint8)
    # translation-only alignment (this is all a cheap prefilter is allowed)
    try:
        win = cv2.createHanningWindow((w, h), cv2.CV_32F)
        (dx, dy), _ = cv2.phaseCorrelate(A.astype(np.float32) * win, B.astype(np.float32) * win)
        M = np.float32([[1, 0, dx], [0, 1, dy]])
        A = cv2.warpAffine(A, M, (w, h), flags=cv2.INTER_NEAREST)
    except Exception:
        dx = dy = 0.0
    ker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
    Ad, Bd = cv2.dilate(A, ker), cv2.dilate(B, ker)
    ao = int((A & ~Bd).sum()); bo = int((B & ~Ad).sum())
    tot = int(A.sum()) + int(B.sum())
    return {
        "cell_pt": cell, "shape": [h, w],
        "shift_px": [round(float(dx), 2), round(float(dy), 2)],
        "left_ink_px": int(A.sum()), "right_ink_px": int(B.sum()),
        "left_only_px": ao, "right_only_px": bo,
        "changed_fraction": round((ao + bo) / max(1, tot), 5),
    }


def one(rec):
    try:
        t0 = time.time()
        a = _block(rec["pdf_left"], rec["page_index_left"], rec["bbox_left"], rec["block_left"], rec["label_left"])
        b = _block(rec["pdf_right"], rec["page_index_right"], rec["bbox_right"], rec["block_right"], rec["label_right"])
        ia = extract_ink(a); ib = extract_ink(b)
        ta = text_spans(a); tb = text_spans(b)
        rs = raster_signal(a, b)
        out = dict(rec)
        out.update({
            "width_pt": [round(a.width, 1), round(b.width, 1)],
            "height_pt": [round(a.height, 1), round(b.height, 1)],
            "aspect_ratio_delta": round(abs((a.width / max(a.height, 1e-6)) - (b.width / max(b.height, 1e-6))), 4),
            "segments": [int(len(ia["segments"])), int(len(ib["segments"]))],
            "fills": [len(ia["fills"]), len(ib["fills"])],
            "ink_pt": [round(ink_length(ia["segments"]), 1), round(ink_length(ib["segments"]), 1)],
            "invisible_paths": [ia["n_invisible_paths"], ib["n_invisible_paths"]],
            "texts": [len(ta), len(tb)],
            "page_rotation": [ia["page_rotation"], ib["page_rotation"]],
            "page_images": [ia["n_page_images"], ib["n_page_images"]],
            "raster": rs,
            "elapsed_s": round(time.time() - t0, 3),
        })
        return out
    except Exception as e:  # noqa: BLE001
        r = dict(rec)
        r["error"] = f"{type(e).__name__}: {e}"
        return r


def main() -> None:
    cands = json.loads((ART / "pair_candidates.json").read_text(encoding="utf-8"))["candidates"]
    if len(sys.argv) > 1:
        cands = cands[: int(sys.argv[1])]
    t0 = time.time()
    res = []
    with ProcessPoolExecutor(max_workers=10) as ex:
        for i, r in enumerate(ex.map(one, cands, chunksize=4)):
            res.append(r)
            if (i + 1) % 100 == 0:
                print(f"{i+1}/{len(cands)} {time.time()-t0:.0f}s", flush=True)
    ok = [r for r in res if "error" not in r]
    (ART / "pair_signals.json").write_text(json.dumps({"probe": "scan_signals", "research_only": True,
                                                       "n": len(res), "errors": len(res) - len(ok),
                                                       "pairs": res}, ensure_ascii=False), encoding="utf-8")
    print("done", len(res), "errors", len(res) - len(ok), f"{time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
