# -*- coding: utf-8 -*-
"""C2 — DILUTION.  ONE physical change, frames of growing size.

v0.1 lost a removed wall once the crop was enlarged ~8x.  Here the change is fixed in
PDF points (all ink inside one object's bbox is deleted) and the FRAME around it grows
from a tight crop to the whole page.  Everything else — comparator, tolerances, noise —
is held constant, so the only variable is how much other graphics surrounds the change.

    python probes/loc_c2_dilution.py <shard> <nshards>
"""
from __future__ import annotations
import json
import math
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402

FACTORS = (1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 16.0, 24.0, 32.0, 64.0, 1e9)
NOISES = ("none", "round025")
MAX_SEG_FRAME = 100000


def pick_target(ol, ex):
    """A real object of the block: the largest 'small' object (0.1-1 % of block area).

    Chosen from the object layer, not invented: it is exactly what a C1 counterfactual
    would delete, but expressed as a rectangle in PDF points so the SAME physical ink
    is deleted at every frame size.
    """
    geom = C._block_geom(ex)
    best = None
    for o in ol.objects:
        bb = o["bbox"]
        af = max(bb[2] - bb[0], 1e-9) * max(bb[3] - bb[1], 1e-9) / geom["area"]
        if not (0.001 <= af <= 0.02):
            continue
        if o["n_seg"] < 4 or o["diag"] < 2.0:
            continue
        # keep it away from the block border so that growing the frame never clips it
        if (bb[0] < geom["x0"] + 0.1 * geom["w"] or bb[2] > geom["x1"] - 0.1 * geom["w"] or
                bb[1] < geom["y0"] + 0.1 * geom["h"] or bb[3] > geom["y1"] - 0.1 * geom["h"]):
            continue
        if best is None or o["seg_len"] > best["seg_len"]:
            best = o
    return best


def frame_px(fr, rect_pt):
    return [rect_pt[0] / fr["scale_x"], rect_pt[1] / fr["scale_y"],
            rect_pt[2] / fr["scale_x"], rect_pt[3] / fr["scale_y"]]


def main():
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    carriers = pick_carriers()
    out = open(ART / "loc_runs" / f"dil_{shard}.jsonl", "w", encoding="utf-8")
    for ci, r in enumerate(carriers):
        if ci % nsh != shard:
            continue
        try:
            pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
            ex0 = G.extract(pb)
            if not ex0.segments:
                continue
            ol0 = O.build_objects(ex0)
        except Exception as e:
            print("carrier fail", r["block_id"], repr(e), flush=True)
            continue
        tgt = pick_target(ol0, ex0)
        if tgt is None:
            out.write(json.dumps({"block_id": r["block_id"], "skip": "no target object"},
                                 ensure_ascii=False) + "\n")
            continue
        tb = list(tgt["bbox"])
        tcx, tcy = (tb[0] + tb[2]) / 2, (tb[1] + tb[3]) / 2
        fr = ex0.frame
        page = fr["page_rect"]
        base = {"block_id": r["block_id"], "doc_id": r["doc_id"], "version": r["version"],
                "discipline": r["discipline"], "bucket": r["bucket"], "cls": r["cls"],
                "block_n_seg": len(ex0.segments),
                "target_object_id": tgt["object_id"], "target_cls": tgt["cls"],
                "target_bbox_pt": tb, "target_n_seg": tgt["n_seg"],
                "target_ink_pt": tgt["seg_len"], "target_label": tgt.get("label")}
        pad = 0.25 * max(tb[2] - tb[0], tb[3] - tb[1])
        tight = [tb[0] - pad, tb[1] - pad, tb[2] + pad, tb[3] + pad]
        tw, th = tight[2] - tight[0], tight[3] - tight[1]
        seen_px = set()
        for f in FACTORS:
            rect = [max(page[0], tcx - tw * f / 2), max(page[1], tcy - th * f / 2),
                    min(page[2], tcx + tw * f / 2), min(page[3], tcy + th * f / 2)]
            if rect[2] - rect[0] < 1 or rect[3] - rect[1] < 1:
                continue
            cp = tuple(round(v, 2) for v in frame_px(fr, rect))
            if cp in seen_px:
                continue
            seen_px.add(cp)
            try:
                exA = G.F.extract_block(str(ex0.provenance["pdf"]), ex0.provenance["page_index"],
                                        list(cp), *ex0.provenance["page_px"])
            except Exception as e:
                out.write(json.dumps({**base, "factor": f, "error": repr(e)},
                                     ensure_ascii=False) + "\n")
                continue
            if len(exA.segments) > MAX_SEG_FRAME or not exA.segments:
                out.write(json.dumps({**base, "factor": f, "n_seg_frame": len(exA.segments),
                                      "skip": "frame too heavy or empty"},
                                     ensure_ascii=False) + "\n")
                continue
            drop = [k for k, s in enumerate(exA.segments)
                    if tb[0] <= min(s["p0"][0], s["p1"][0]) and max(s["p0"][0], s["p1"][0]) <= tb[2]
                    and tb[1] <= min(s["p0"][1], s["p1"][1]) and max(s["p0"][1], s["p1"][1]) <= tb[3]]
            if not drop:
                out.write(json.dumps({**base, "factor": f, "skip": "target ink absent in frame",
                                      "n_seg_frame": len(exA.segments)},
                                     ensure_ascii=False) + "\n")
                continue
            dset = set(drop)
            ink_drop = sum(exA.segments[k]["len"] for k in drop)
            segs = [dict(s) for k, s in enumerate(exA.segments) if k not in dset]
            for k, s in enumerate(segs):
                s["i"] = k
            ex2 = C._clone(exA, segments=segs, prov={"loc": "dilution_delete"})
            man = {"change_bbox_pt": tb, "expected_verdict": "GRAPHIC_CHANGE",
                   "expected_ledger": [{"type": "REMOVED_OBJECT"}],
                   "touched_objects": []}
            for noise in NOISES:
                t0 = time.time()
                try:
                    exB = L.noisy(ex2, noise, seed=20260823)
                    LA, LB, meta = L.layers(exA, exB)
                    led = L.ledger(exA, exB, LA=LA, LB=LB, meta=meta)
                    sc = L.score_against_manifest(led, man)
                except Exception as e:
                    out.write(json.dumps({**base, "factor": f, "noise": noise,
                                          "error": repr(e)}, ensure_ascii=False) + "\n")
                    continue
                area = (rect[2] - rect[0]) * (rect[3] - rect[1])
                tarea = max((tb[2] - tb[0]) * (tb[3] - tb[1]), 1e-9)
                out.write(json.dumps({
                    **base, "factor": f, "noise": noise,
                    "frame_pt": [round(v, 2) for v in rect],
                    "frame_area_over_target": round(area / tarea, 2),
                    "n_seg_frame": len(exA.segments), "n_obj_frame": len(LA.objects),
                    "S_frame": round(LA.S, 3), "scale_src": meta["src_a"],
                    "ink_frame_pt": round(sum(s["len"] for s in exA.segments), 2),
                    "deleted_n_seg": len(drop), "deleted_ink_pt": round(ink_drop, 3),
                    "deleted_ink_frac": round(ink_drop / max(sum(s["len"] for s in exA.segments), 1e-9), 8),
                    "scalar": led["scalar"], "counts": led["counts"],
                    "verdict_scalar_999": L.scalar_verdict(led, 0.999),
                    "verdict_scalar_9999": L.scalar_verdict(led, 0.9999),
                    "verdict_counts": L.counts_verdict(led),
                    "score": sc, "t_sec": round(time.time() - t0, 2),
                }, ensure_ascii=False) + "\n")
            out.flush()
            if rect[0] <= page[0] and rect[1] <= page[1] and rect[2] >= page[2] and rect[3] >= page[3]:
                break
        print("done", r["block_id"], flush=True)
    out.close()


if __name__ == "__main__":
    main()
