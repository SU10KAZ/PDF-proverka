# -*- coding: utf-8 -*-
"""visprep V1(c) — CALIBRATION cases: controlled counterfactuals on real prepared blocks.

The point of a calibration case is that WE know the answer and the blind agent does not,
and that it cannot be told apart from a real case by its look.  Therefore the change is
applied at PDF level and BOTH sides are produced by exactly the same operation:

    left  = page with the patch region whited out and the ORIGINAL geometry redrawn
    right = page with the same patch whited out and the COUNTERFACTUAL geometry redrawn

so any artefact of the redraw appears on both sides and cannot leak the answer.
Geometry comes only from `v03_foundation`, objects only from `v03_objects`, the change
only from `v03_counterfactual.apply` (never hand-made).

A carrier is rejected (not silently patched) when the patch would swallow text, a raster
insert or a filled path — a white-out cannot restore those.
"""
from __future__ import annotations

import json
import math
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import fitz                        # noqa: E402
import vis_common as V             # noqa: E402
import v03_foundation as F         # noqa: E402
import v03_objects as O            # noqa: E402
import v03_counterfactual as CF    # noqa: E402
import grp_common as G             # noqa: E402

TMP = V.ART / "vis_cf_tmp"
WHITE_PAD = 1.6           # pt, covers half a stroke width around the patch
CTX_FRAC = 0.75           # context margin around the patch, fraction of its long side
CTX_MIN = 10.0
TARGET = 700


def _rect(b):
    return fitz.Rect(min(b[0], b[2]), min(b[1], b[3]), max(b[0], b[2]), max(b[1], b[3]))


def _isect(a, b):
    A, B = _rect(a), _rect(b)
    return not (A.x1 < B.x0 or B.x1 < A.x0 or A.y1 < B.y0 or B.y1 < A.y0)


def _seg_bbox(s):
    return [min(s["p0"][0], s["p1"][0]), min(s["p0"][1], s["p1"][1]),
            max(s["p0"][0], s["p1"][0]), max(s["p0"][1], s["p1"][1])]


def _key(s):
    a, b = tuple(round(v, 2) for v in s["p0"]), tuple(round(v, 2) for v in s["p1"])
    return (a, b) if a <= b else (b, a)


def diff_bbox(ex, ex2):
    ka = {}
    for s in ex.segments:
        ka[_key(s)] = ka.get(_key(s), 0) + 1
    kb = {}
    for s in ex2.segments:
        kb[_key(s)] = kb.get(_key(s), 0) + 1
    changed = []
    for k, n in ka.items():
        if kb.get(k, 0) != n:
            changed.append(k)
    for k, n in kb.items():
        if ka.get(k, 0) != n:
            changed.append(k)
    if not changed:
        return None
    xs = [p[0] for k in changed for p in k]
    ys = [p[1] for k in changed for p in k]
    return [min(xs), min(ys), max(xs), max(ys)]


def _white(c):
    if c is None:
        return True
    try:
        return all(float(v) >= 0.985 for v in c)
    except Exception:
        return True


def patch_ok(ex, ex2, patch):
    """A white-out may only touch stroked geometry: no text, no raster, no fills."""
    for e in (ex, ex2):
        for t in e.texts:
            if _isect(t["bbox"], patch):
                return "text_in_patch"
        for im in e.images:
            if _isect(im.get("bbox") or [0, 0, 0, 0], patch):
                return "raster_in_patch"
        for s in e.segments:
            if not _white(s.get("fill")) and _isect(_seg_bbox(s), patch):
                return "fill_in_patch"
    return None


def patched_pdf(pb, segments, patch, out_pdf: Path):
    src = F.open_doc(pb.pdf_path)
    nd = fitz.open()
    nd.insert_pdf(src, from_page=pb.page_index, to_page=pb.page_index)
    page = nd[0]
    if page.rotation % 360 != 0:
        raise RuntimeError("carrier page is rotated; CF carriers are restricted to /Rotate 0")
    wr = _rect(patch) + fitz.Rect(-WHITE_PAD, -WHITE_PAD, WHITE_PAD, WHITE_PAD)
    page.draw_rect(wr, color=None, fill=(1, 1, 1), width=0)
    groups = {}
    for s in segments:
        if not _isect(_seg_bbox(s), [wr.x0, wr.y0, wr.x1, wr.y1]):
            continue
        col = tuple(s.get("color") or (0.0, 0.0, 0.0))
        wid = round(max(float(s.get("w") or 0.0), 0.1), 3)
        groups.setdefault((col, wid), []).append(s)
    n = 0
    for (col, wid), items in sorted(groups.items(), key=lambda kv: repr(kv[0])):
        sh = page.new_shape()
        for s in items:
            sh.draw_line(fitz.Point(*s["p0"]), fitz.Point(*s["p1"]))
            n += 1
        sh.finish(color=col, width=wid, closePath=False)
        sh.commit()
    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    nd.save(str(out_pdf), garbage=3, deflate=True)
    nd.close()
    return n


def side_dict(pdf, pb):
    return {"pdf": str(pdf), "page_index": 0, "page_px": [pb.page_px_w, pb.page_px_h]}


def fidelity(pb, left_pdf, window):
    """How faithful the redraw is: pixel difference between the patched-original page
    and the untouched page over the same window (both rendered by F.render_block)."""
    a = V.render_region(side_dict(left_pdf, pb), window, TMP / "fid_a.png", 400)
    orig = {"pdf": pb.pdf_path, "page_index": pb.page_index,
            "page_px": [pb.page_px_w, pb.page_px_h]}
    b = V.render_region(orig, window, TMP / "fid_b.png", 400)
    import numpy as np
    pa = fitz.Pixmap(str(TMP / "fid_a.png"))
    pb_ = fitz.Pixmap(str(TMP / "fid_b.png"))
    if pa.width != pb_.width or pa.height != pb_.height:
        return 1.0
    A = CF.pix_to_bin(pa, 200)
    B = CF.pix_to_bin(pb_, 200)
    return float((A != B).mean())


def context_window(patch):
    x0, y0, x1, y1 = _rect(patch)
    m = max(x1 - x0, y1 - y0)
    pad = max(CTX_MIN, CTX_FRAC * m)
    return V.pad_rect([x0, y0, x1, y1], pad, 24.0)


def build_case(pb, ex, L, cf_id, cid, *, obj_idx=None, params=None, out_dir=None):
    params = params or {}
    ex2, man = CF.apply(ex, L, cf_id, **params)
    if obj_idx is not None:                     # class A: the case is ONE object of the block
        patch = list(L.objects[obj_idx]["bbox"])
    else:
        db = diff_bbox(ex, ex2)
        if db is None:
            return {"cand_id": cid, "reject": "no_geometric_difference"}
        patch = db
        cb = man.get("change_bbox_pt")
        if cb:
            patch = [min(patch[0], cb[0]), min(patch[1], cb[1]),
                     max(patch[2], cb[2]), max(patch[3], cb[3])]
    r = _rect(patch)
    if max(r.width, r.height) > 260 or max(r.width, r.height) < 6:
        return {"cand_id": cid, "reject": f"patch_size_{round(max(r.width, r.height),1)}"}
    bad = patch_ok(ex, ex2, patch)
    if bad:
        return {"cand_id": cid, "reject": bad}
    out_dir = out_dir or V.CAND_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    TMP.mkdir(parents=True, exist_ok=True)
    lp, rp = TMP / f"{cid}_L.pdf", TMP / f"{cid}_R.pdf"
    n_l = patched_pdf(pb, ex.segments, patch, lp)
    n_r = patched_pdf(pb, ex2.segments, patch, rp)
    win = context_window(patch)
    fid = fidelity(pb, lp, win)
    szL = V.render_region(side_dict(lp, pb), win, out_dir / f"{cid}_L.png", TARGET)
    szR = V.render_region(side_dict(rp, pb), win, out_dir / f"{cid}_R.png", TARGET)
    V.montage(out_dir / f"{cid}_L.png", out_dir / f"{cid}_R.png", out_dir / f"{cid}_M.png")
    return {"cand_id": cid, "family": "CF", "cf_id": cf_id, "cf_class": man["cf_class"],
            "expected_verdict": man["expected_verdict"],
            "doc_id": pb.doc_id, "version": pb.version, "block_id": pb.block_id,
            "discipline": pb.discipline, "page_index": pb.page_index,
            "n_seg_block": len(ex.segments), "S_pt": round(L.S, 3),
            "patch_pt": [round(v, 3) for v in patch], "window_pt": [round(v, 3) for v in win],
            "redraw_fidelity_diff": round(fid, 5),
            "n_redrawn_left": n_l, "n_redrawn_right": n_r,
            "touched_objects": man.get("touched_objects"),
            "seed": man.get("seed"), "params": man.get("params"),
            "px": [szL, szR], "left_pdf": str(lp), "right_pdf": str(rp)}


def carriers(limit=40, seed=20260823):
    rows = json.load(open(V.ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    rows = [r for r in rows if r["bucket"] in ("sparse", "medium", "dense")
            and r["cls"] in ("drawing", "vector_raster_mix", "legend_notes", "table", "stamp")
            and 150 <= r["n_seg"] <= 12000]
    random.Random(seed).shuffle(rows)
    out = []
    for r in rows:
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None or pb.rotation % 360 != 0:
            continue
        out.append((r, pb))
        if len(out) >= limit:
            break
    return out


def pick_object(L, ex, *, kinds=("symbol",), dmin=12.0, dmax=90.0, nmin=8, nmax=400, k=1, rng=None):
    cand = [i for i, o in enumerate(L.objects)
            if o["cls"] in kinds and dmin <= o["diag"] <= dmax and nmin <= o["n_seg"] <= nmax]
    rng = rng or random.Random(7)
    rng.shuffle(cand)
    return cand[:k]


def main():
    import os
    plan_A = (os.environ.get("VIS_PLAN_A") or
              "A6_round_0.5,A6_round_0.25,A3_curve_resample_down,"
              "A4_circle_to_bezier,A1_path_split,A2_path_merge").split(",")
    plan_C = (os.environ.get("VIS_PLAN_C") or
              "C1_remove_object,C2_add_object,C3_move_object,"
              "C6_reshape_object,C9_add_branch,C10_remove_opening").split(",")
    want_each = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    n_lim = int(sys.argv[3]) if len(sys.argv) > 3 else 40
    rows, n_a, n_c = [], 0, 0
    for r, pb in carriers(limit=n_lim):
        if n_a >= want_each and n_c >= want_each:
            break
        try:
            ex = G.extract(pb)
            if len(ex.segments) < 120:
                continue
            L = O.build_objects(ex)
        except Exception as e:                       # noqa: BLE001
            print("carrier fail", r["block_id"], e)
            continue
        rng = random.Random(hash(pb.block_id) & 0xFFFF)
        got_a = got_c = 0
        # ---- class A applied to ONE object: same picture, other decomposition -> SAME
        for cf_id in plan_A:
            if got_a or n_a >= want_each:
                break
            for oi in pick_object(L, ex, k=5, rng=rng):
                cid = f"C_{pb.discipline}_{pb.block_id[:8]}_{cf_id.split('_')[0]}_{oi}"
                try:
                    res = build_case(pb, ex, L, cf_id, cid, obj_idx=oi)
                except Exception as e:               # noqa: BLE001
                    res = {"cand_id": cid, "reject": f"error:{e}"}
                res.update({"carrier": r, "role": "A", "truth_hint": "SAME",
                            "obj": {k: L.objects[oi][k] for k in
                                    ("object_id", "cls", "n_seg", "diag", "bbox", "label")}})
                rows.append(res)
                print(cid, cf_id, res.get("reject") or res.get("redraw_fidelity_diff"))
                if not res.get("reject"):
                    n_a += 1; got_a = 1
                    break
        # ---- class C: a real object change -> DIFFERENT
        for cf_id in plan_C:
            if got_c or n_c >= want_each:
                break
            par = {}
            if cf_id in ("C1_remove_object", "C2_add_object", "C6_reshape_object"):
                par = {"bucket": "small"}
            if cf_id == "C3_move_object":
                par = {"bucket": "small", "frac": 0.02}
            cid = f"C_{pb.discipline}_{pb.block_id[:8]}_{cf_id.split('_')[0]}"
            try:
                res = build_case(pb, ex, L, cf_id, cid, params=par)
            except CF.CFNotApplicable as e:
                res = {"cand_id": cid, "reject": f"not_applicable:{e}"}
            except Exception as e:                   # noqa: BLE001
                res = {"cand_id": cid, "reject": f"error:{e}"}
            res.update({"carrier": r, "role": "A", "truth_hint": "DIFFERENT"})
            rows.append(res)
            print(cid, cf_id, res.get("reject") or res.get("redraw_fidelity_diff"))
            if not res.get("reject"):
                n_c += 1; got_c = 1
    with open(V.ART / (os.environ.get("VIS_CF_OUT") or "vis_cf_candidates.json"), "w", encoding="utf-8") as fh:
        json.dump({"n": len(rows), "built_A": n_a, "built_C": n_c, "candidates": rows},
                  fh, ensure_ascii=False, indent=1)
    print("built SAME/DIFFERENT:", n_a, n_c, "attempts:", len(rows))


if __name__ == "__main__":
    main()
