# -*- coding: utf-8 -*-
"""Visual check of the counterfactuals: before / after / overlay, for eyeballing.

Overlay legend:  black = ink present in both,  RED = ink only AFTER (added),
BLUE = ink only BEFORE (removed).

    python probes/cf_visual.py
-> artifacts/cf_visual/<carrier>__<cf>.png  and  artifacts/cf_visual/index.json
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXP = HERE.parent
ART = EXP / "artifacts"
OUT = ART / "cf_visual"
sys.path.insert(0, str(HERE))

import numpy as np                 # noqa: E402
import fitz                        # noqa: E402
import grp_common as G             # noqa: E402
import v03_objects as O            # noqa: E402
import v03_counterfactual as C     # noqa: E402
import cf_check as K               # noqa: E402
import v03_foundation as F         # noqa: E402

TARGET = 950

PLAN = [
    ("A", "A1_path_split", {}),
    ("A", "A4_circle_to_bezier", {}),
    ("A", "A6_round_0.5", {}),
    ("A", "A8_lineweight", {}),
    ("A", "A7_reexport_cairo", {}),
    ("B", "B1_translate", {"frac": 0.10}),
    ("B", "B2_scale", {"k": 1.2}),
    ("B", "B3_crop_jitter", {"frac": 0.10}),
    ("B", "B5_rotate_page", {"add": 90}),
    ("C", "C1_remove_object", {"bucket": "large"}),
    ("C", "C2_add_object", {"bucket": "small"}),
    ("C", "C3_move_object", {"bucket": "large", "frac": 0.05}),
    ("C", "C4_swap_objects", {}),
    ("C", "C6_reshape_object", {"bucket": "large"}),
    ("C", "C7_split_object", {}),
    ("C", "C8_merge_objects", {}),
    ("C", "C9_add_branch", {}),
    ("C", "C10_remove_opening", {}),
    ("D", "D1_text_edit", {}),
    ("D", "D2_text_move", {}),
    ("D", "D4_table_values", {}),
    ("D", "D6_dim_value_only", {}),
    ("D", "D7_dim_geometry", {}),
    ("D", "D9_text_to_curves", {}),
]

PAGE_LEVEL = {"A7_reexport_gs", "A7_reexport_cairo", "D9_text_to_curves", "B5_rotate_page"}


def _mask_to_rgb(A, B):
    h = min(A.shape[0], B.shape[0]); w = min(A.shape[1], B.shape[1])
    A, B = A[:h, :w], B[:h, :w]
    img = np.full((h, w, 3), 255, np.uint8)
    both = A & B
    only_a = A & ~B
    only_b = B & ~A
    img[both] = (0, 0, 0)
    img[only_a] = (30, 60, 230)         # removed  -> blue
    img[only_b] = (230, 30, 30)         # added    -> red
    return img


def _save_triptych(pa_mask, pb_mask, path, labels=("before", "after", "overlay")):
    h = min(pa_mask.shape[0], pb_mask.shape[0])
    w = min(pa_mask.shape[1], pb_mask.shape[1])
    A, B = pa_mask[:h, :w], pb_mask[:h, :w]
    tiles = []
    for M in (A, B):
        img = np.full((h, w, 3), 255, np.uint8)
        img[M] = (0, 0, 0)
        tiles.append(img)
    tiles.append(_mask_to_rgb(A, B))
    gap = 8
    W = w * 3 + gap * 2
    canvas = np.full((h, W, 3), 210, np.uint8)
    for i, t in enumerate(tiles):
        canvas[:, i * (w + gap): i * (w + gap) + w] = t
    pix = fitz.Pixmap(fitz.csRGB, W, h, canvas.tobytes(), False)
    pix.save(str(path))


def _pdf_masks(ex, ex2, rotate_back=0):
    p1, p2 = ex.provenance, ex2.provenance
    a = F.render_block(p1["pdf"], p1["page_index"], p1["coords_px"], p1["page_px"][0],
                       p1["page_px"][1], target_px=TARGET)
    b = F.render_block(p2["pdf"], p2["page_index"], p2["coords_px"], p2["page_px"][0],
                       p2["page_px"][1], target_px=TARGET)
    A = C.pix_to_bin(a); B = C.pix_to_bin(b)
    if rotate_back:
        best = None
        for k in (1, 2, 3):
            Bk = np.rot90(B, k=k)
            if abs(Bk.shape[0] - A.shape[0]) > 3 or abs(Bk.shape[1] - A.shape[1]) > 3:
                continue
            hh = min(A.shape[0], Bk.shape[0]); ww = min(A.shape[1], Bk.shape[1])
            sc = int((A[:hh, :ww] ^ Bk[:hh, :ww]).sum())
            if best is None or sc < best[0]:
                best = (sc, Bk)
        if best is not None:
            B = best[1]
    return A, B


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    smp = json.load(open(ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    # carriers chosen for legibility: mid-density drawings with text, different disciplines
    want = []
    seen = set()
    for r in smp:
        if r["cls"] not in ("drawing", "table"):
            continue
        if not (300 <= r["n_seg"] <= 6000 and r["n_text"] >= 5):
            continue
        if r["discipline"] in seen:
            continue
        seen.add(r["discipline"])
        want.append(r)
        if len(want) >= 6:
            break
    index = []
    for r in want:
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None:
            continue
        ex = G.extract(pb)
        if not ex.segments:
            continue
        ol = O.build_objects(ex)
        tag0 = f"{r['discipline']}_{r['block_id'][4:12]}"
        F.render_block(pb.pdf_path, pb.page_index, pb.coords_px, pb.page_px_w, pb.page_px_h,
                       target_px=TARGET, out_png=str(OUT / f"{tag0}__00_production_crop.png"))
        for cls, cf, kw in PLAN:
            try:
                ex2, man = C.apply(ex, ol, cf, **kw)
            except C.CFNotApplicable as e:
                index.append({"carrier": tag0, "cf": cf, "status": "skip", "reason": str(e)})
                continue
            except Exception as e:                                  # noqa: BLE001
                index.append({"carrier": tag0, "cf": cf, "status": "error",
                              "reason": f"{type(e).__name__}: {e}"})
                continue
            try:
                if cf in PAGE_LEVEL:
                    A, B = _pdf_masks(ex, ex2, rotate_back=kw.get("add", 0) if cf.startswith("B5") else 0)
                    src = "pdf_render"
                else:
                    fr = None if cls == "B" else C._frame_of(ex)
                    pa = C.render_extract(ex, frame=fr or C._frame_of(ex), target_px=TARGET)
                    pb2 = C.render_extract(ex2, frame=fr, target_px=TARGET)
                    A = C.pix_to_bin(pa); B = C.pix_to_bin(pb2)
                    src = "extract_render"
                png = OUT / f"{tag0}__{cf}.png"
                _save_triptych(A, B, png)
                h = min(A.shape[0], B.shape[0]); w = min(A.shape[1], B.shape[1])
                d = int((A[:h, :w] ^ B[:h, :w]).sum())
                index.append({"carrier": tag0, "cf": cf, "status": "ok", "png": png.name,
                              "render": src, "diff_px": d, "diff_frac": d / (h * w),
                              "n_seg_before": man["changed_primitives"]["n_before"],
                              "n_seg_after": man["changed_primitives"]["n_after"],
                              "expected": man["expected_verdict"],
                              "touched": [o["object_id"] for o in man.get("touched_objects", [])],
                              "change_bbox_pt": man.get("change_bbox_pt")})
                print(tag0, cf, "ok", d, flush=True)
            except Exception as e:                                  # noqa: BLE001
                index.append({"carrier": tag0, "cf": cf, "status": "render_error",
                              "reason": f"{type(e).__name__}: {e}"})
        C.cleanup_scratch()
    json.dump({"target_px": TARGET, "items": index}, open(OUT / "index.json", "w",
              encoding="utf-8"), ensure_ascii=False, indent=1)
    ok = sum(1 for i in index if i["status"] == "ok")
    print("visual items:", ok, "of", len(index))


if __name__ == "__main__":
    main()
