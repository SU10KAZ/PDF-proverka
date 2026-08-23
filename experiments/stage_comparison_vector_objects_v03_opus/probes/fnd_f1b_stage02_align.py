# -*- coding: utf-8 -*-
"""F1a addendum — why our render differs from the STORED stage02 PNG.

The stored crops carry source="cloud": they were rasterised by the crop service from a
crop-PDF, not by crop_from_pdf locally.  Question: same region (and we only lose to
sub-pixel phase), or a different region?  Measured with a shift search over ±3 px.
"""
from __future__ import annotations

import json, os, sys
from pathlib import Path

import numpy as np
from PIL import Image

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")


def ink(img):
    return np.asarray(img.convert("L")).astype(np.int16) < 250


def iou(a, b):
    u = (a | b).sum()
    return float((a & b).sum() / u) if u else 1.0


def main():
    d = json.loads((ART / "fnd_rotation.json").read_text(encoding="utf-8"))
    out = []
    for r in d["rows"]:
        if not r.get("stage02_png"):
            continue
        pix = F.render_block(r["doc_pdf"] if "doc_pdf" in r else _pdf(r), r["page_index"],
                             r["coords_px"], *r["page_px"], dpi=100, min_long_side=800)
        mine = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        stored = Image.open(r["stage02_png"])
        if stored.size != mine.size:
            mine = mine.resize(stored.size, Image.LANCZOS)
        A, B = ink(stored), ink(mine)
        best, bshift = -1.0, None
        for dy in range(-3, 4):
            for dx in range(-3, 4):
                Bs = np.roll(np.roll(B, dy, axis=0), dx, axis=1)
                v = iou(A, Bs)
                if v > best:
                    best, bshift = v, (dx, dy)
        # 1-px dilation tolerance
        def dil(M):
            D = M.copy()
            for ax in (0, 1):
                for s in (-1, 1):
                    D |= np.roll(M, s, axis=ax)
            return D
        out.append({
            "block_id": r["block_id"], "rotation": r["rotation"], "discipline": r["discipline"],
            "size_stored": list(stored.size), "size_mine_native": r["render_size"],
            "ink_iou_raw": r["vs_stage02_png"]["ink_iou"],
            "ink_iou_best_shift": best, "best_shift": list(bshift),
            "ink_iou_dilated": iou(dil(A), dil(B)),
            "ink_share_stored": float(A.mean()), "ink_share_mine": float(B.mean()),
            "png": r["stage02_png"],
        })
        F.clear_caches()
    summ = {
        "n": len(out),
        "median_ink_iou_raw": float(np.median([o["ink_iou_raw"] for o in out])),
        "median_ink_iou_best_shift": float(np.median([o["ink_iou_best_shift"] for o in out])),
        "median_ink_iou_dilated": float(np.median([o["ink_iou_dilated"] for o in out])),
        "n_dilated_ge_090": sum(1 for o in out if o["ink_iou_dilated"] >= 0.90),
        "n_dilated_lt_050": sum(1 for o in out if o["ink_iou_dilated"] < 0.50),
    }
    (ART / "fnd_stage02_align.json").write_text(
        json.dumps({"summary": summ, "rows": out}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({"summary": summ, "rows": out}, ensure_ascii=False, indent=1))


def _pdf(r):
    # rebuild pdf path from the blocks index
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            if b["block_id"] == r["block_id"] and b["doc_id"] == r["doc_id"]:
                return b["pdf"]
    raise KeyError(r["block_id"])


if __name__ == "__main__":
    main()
