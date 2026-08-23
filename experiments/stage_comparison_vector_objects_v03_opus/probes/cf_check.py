# -*- coding: utf-8 -*-
"""Self-check primitives for the `cf` probe (probe-side, not part of the engine).

Four checks demanded by the task:
  1. renderer fidelity   — the extract renderer must agree with the foundation render;
  2. class A / B         — the PICTURE must not change (B: after compensation);
  3. class C             — the picture must change in EXACTLY ONE place (the manifest bbox);
  4. class D             — the inked geometry must be bit-identical (except D7 / D9).
"""
from __future__ import annotations
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fitz                       # noqa: E402
import v03_foundation as F        # noqa: E402
import v03_counterfactual as C    # noqa: E402

TARGET = 1100


def _masks(pa, pb, thr=200):
    """Ink masks of two pixmaps, cropped to their common size (differ by <=2 px)."""
    A = C.pix_to_bin(pa, thr)
    B = C.pix_to_bin(pb, thr)
    h = min(A.shape[0], B.shape[0])
    w = min(A.shape[1], B.shape[1])
    return A[:h, :w], B[:h, :w], (A.shape, B.shape)


def _diff_stats(A, B):
    import numpy as np
    D = A ^ B
    diff = int(D.sum())
    inkA, inkB = int(A.sum()), int(B.sum())
    inter = int((A & B).sum())
    bbox = None
    if diff:
        ys, xs = np.nonzero(D)
        bbox = [int(xs.min()), int(ys.min()), int(xs.max()) + 1, int(ys.max()) + 1]
    tot = A.size
    return {"w": int(A.shape[1]), "h": int(A.shape[0]), "px_total": int(tot),
            "diff_px": diff, "diff_frac": diff / tot,
            "ink_a": inkA, "ink_b": inkB,
            "ink_diff_frac_of_a": diff / max(inkA, 1),
            "ink_iou": inter / max(inkA + inkB - inter, 1),
            "diff_bbox_px": bbox}


def renderer_fidelity(ex, target=TARGET) -> dict:
    """render_extract(ex)  vs  foundation render_block of the same region."""
    p = ex.provenance
    fr = F.block_frame(p["pdf"], p["page_index"], p["coords_px"], p["page_px"][0], p["page_px"][1])
    ref = F.render_block(p["pdf"], p["page_index"], p["coords_px"], p["page_px"][0], p["page_px"][1],
                         target_px=target, frame=fr, production_clip=False)
    mine = C.render_extract(ex, target_px=target)
    A, B, shapes = _masks(ref, mine)
    d = _diff_stats(A, B)
    d["mode"] = "extract_render_vs_foundation_render"
    d["shapes"] = [list(shapes[0]), list(shapes[1])]
    return d


def picture_pair(ex, ex2, *, frame=None, target=TARGET, draw_text=True, base_mask=None):
    if base_mask is not None:
        pb = C.render_extract(ex2, frame=frame, target_px=target, draw_text=draw_text)
        B = C.pix_to_bin(pb)
        A = base_mask
        h = min(A.shape[0], B.shape[0]); w = min(A.shape[1], B.shape[1])
        A, B = A[:h, :w], B[:h, :w]
        return _diff_stats(A, B), A, B
    pa = C.render_extract(ex, frame=frame, target_px=target, draw_text=draw_text)
    pb = C.render_extract(ex2, frame=frame, target_px=target, draw_text=draw_text)
    A, B, _ = _masks(pa, pb)
    return _diff_stats(A, B), A, B


def _intersect(f1, f2):
    r = [max(f1[0], f2[0]), max(f1[1], f2[1]), min(f1[2], f2[2]), min(f1[3], f2[3])]
    if r[2] - r[0] < 1 or r[3] - r[1] < 1:
        return None
    return r


def check_A(ex, ex2, man, base_mask=None) -> dict:
    """The picture must be the same; both sides rendered in the ORIGINAL frame."""
    d, _, _ = picture_pair(ex, ex2, frame=C._frame_of(ex), draw_text=False, base_mask=base_mask)
    d["check"] = "A_picture_invariance"
    return d


def check_B(ex, ex2, man) -> dict:
    comp = man.get("compensation") or {}
    kind = comp.get("kind")
    if kind in ("translate", "scale"):
        # compensation == render each side in its OWN frame at the same pixel size
        d, _, _ = picture_pair(ex, ex2, frame=None, draw_text=False)
        d["check"] = f"B_picture_after_{kind}"
        return d
    if kind == "frame_intersection":
        fr = _intersect(C._frame_of(ex), C._frame_of(ex2))
        if fr is None:
            return {"check": "B_frame_intersection", "error": "empty intersection"}
        d, _, _ = picture_pair(ex, ex2, frame=fr, draw_text=False)
        d["check"] = "B_picture_on_intersection"
        return d
    if kind == "rotate":
        M = fitz.Matrix(*comp["matrix"])
        inv = ~M
        segs = []
        for s in ex2.segments:
            a = fitz.Point(*s["p0"]) * inv
            b = fitz.Point(*s["p1"]) * inv
            t = dict(s)
            t["p0"] = (a.x, a.y)
            t["p1"] = (b.x, b.y)
            segs.append(t)
        back = C._clone(ex2, segments=segs, frame={**ex2.frame, "clip_display": C._frame_of(ex)})
        d, _, _ = picture_pair(ex, back, frame=C._frame_of(ex), draw_text=False)
        d["check"] = "B_picture_after_derotation"
        d["geometry_match"] = geometry_match(ex, back, tol=0.02)
        return d
    return {"check": "B_none", "error": f"unknown compensation {kind}"}


def geometry_match(ex, ex2, tol=1e-6) -> dict:
    """Fraction of segments of A that have an exact twin in B (unordered)."""
    q = lambda v: round(v / tol) if tol else v
    def keyset(ex_):
        d = {}
        for s in ex_.segments:
            k = (q(s["p0"][0]), q(s["p0"][1]), q(s["p1"][0]), q(s["p1"][1]))
            k2 = (q(s["p1"][0]), q(s["p1"][1]), q(s["p0"][0]), q(s["p0"][1]))
            k = min(k, k2)
            d[k] = d.get(k, 0) + 1
        return d
    A, B = keyset(ex), keyset(ex2)
    inter = sum(min(v, B.get(k, 0)) for k, v in A.items())
    na = sum(A.values()); nb = sum(B.values())
    return {"n_a": na, "n_b": nb, "matched": inter,
            "frac_of_a": inter / max(na, 1), "frac_of_b": inter / max(nb, 1),
            "identical": inter == na == nb}


def check_C(ex, ex2, man, *, pad_px=3, base_mask=None) -> dict:
    fr = C._frame_of(ex)
    d, A, B = picture_pair(ex, ex2, frame=fr, draw_text=False, base_mask=base_mask)
    out = {"check": "C_locality", **d}
    if d.get("diff_px", 0) == 0:
        out["localised"] = False
        out["note"] = "counterfactual invisible on the raster"
        return out
    D = A ^ B
    h, w = D.shape
    rs = w / max(fr[2] - fr[0], 1e-9)
    import numpy as np
    regions = man.get("change_regions_pt") or ([man["change_bbox_pt"]]
                                               if man.get("change_bbox_pt") else [])
    if not regions:
        out["localised"] = None
        return out
    mask = np.zeros_like(D)
    boxes = []
    for bb in regions:
        x0 = max(0, int((bb[0] - fr[0]) * rs) - pad_px)
        y0 = max(0, int((bb[1] - fr[1]) * rs) - pad_px)
        x1 = min(w, int((bb[2] - fr[0]) * rs) + pad_px)
        y1 = min(h, int((bb[3] - fr[1]) * rs) + pad_px)
        if x1 > x0 and y1 > y0:
            mask[y0:y1, x0:x1] = True
            boxes.append([x0, y0, x1, y1])
    inside = int((D & mask).sum())
    total = int(D.sum())
    out["diff_inside_expected_bbox"] = inside
    out["frac_diff_inside_expected_bbox"] = inside / max(total, 1)
    out["expected_bbox_px"] = boxes
    out["expected_bbox_area_frac"] = float(mask.sum()) / max(w * h, 1)
    out["localised"] = (inside / max(total, 1)) >= 0.98
    return out


def check_D(ex, ex2, man) -> dict:
    gm = geometry_match(ex, ex2, tol=1e-6)
    txt_before = [t["text"] for t in ex.texts]
    txt_after = [t["text"] for t in ex2.texts]
    changed = sum(1 for a, b in zip(sorted(txt_before), sorted(txt_after)) if a != b) \
        if len(txt_before) == len(txt_after) else abs(len(txt_before) - len(txt_after))
    return {"check": "D_geometry_invariance", **gm,
            "n_text_before": len(txt_before), "n_text_after": len(txt_after),
            "text_lines_changed": changed}


# ---------------------------------------------------------------- PDF-level checks
# For page-level counterfactuals (A7, B5, D9) the picture can be compared WITHOUT the
# extract renderer at all: both sides are rasterised by the foundation from real PDFs.

def _render_pdf_block(pdf, page_index, coords_px, page_px, target=TARGET):
    return F.render_block(pdf, page_index, coords_px, page_px[0], page_px[1],
                          target_px=target, production_clip=True)


def check_page_level(ex, ex2, man, *, rotate_back=0) -> dict:
    import numpy as np
    p1, p2 = ex.provenance, ex2.provenance
    a = _render_pdf_block(p1["pdf"], p1["page_index"], p1["coords_px"], p1["page_px"])
    b = _render_pdf_block(p2["pdf"], p2["page_index"], p2["coords_px"], p2["page_px"])
    A = C.pix_to_bin(a)
    B = C.pix_to_bin(b)
    if rotate_back:
        best = None
        for k in (1, 2, 3):
            Bk = np.rot90(B, k=k)
            if Bk.shape[0] < A.shape[0] - 3 or Bk.shape[1] < A.shape[1] - 3:
                if abs(Bk.shape[0] - A.shape[0]) > 3 or abs(Bk.shape[1] - A.shape[1]) > 3:
                    continue
            hh = min(A.shape[0], Bk.shape[0]); ww = min(A.shape[1], Bk.shape[1])
            score = int((A[:hh, :ww] ^ Bk[:hh, :ww]).sum())
            if best is None or score < best[0]:
                best = (score, k, Bk)
        if best is None:
            return {"check": "page_level_picture_from_pdf", "error": "no rotation fits"}
        B = best[2]
        rot_k = best[1]
    else:
        rot_k = 0
    h = min(A.shape[0], B.shape[0]); w = min(A.shape[1], B.shape[1])
    A, B = A[:h, :w], B[:h, :w]
    d = _diff_stats(A, B)
    tol = (A & ~_dilate(B, 1)) | (B & ~_dilate(A, 1))
    d["diff_frac_1px_tolerant"] = float(tol.sum()) / A.size
    d["ink_diff_1px_tolerant_of_a"] = float(tol.sum()) / max(int(A.sum()), 1)
    d["check"] = "page_level_picture_from_pdf"
    d["rot90_k"] = rot_k
    d["shape_a"] = [int(a.height), int(a.width)]
    d["shape_b"] = [int(b.height), int(b.width)]
    return d


def _dilate(M, r=1):
    import numpy as np
    out = M.copy()
    for dy in range(-r, r + 1):
        for dx in range(-r, r + 1):
            if dx == 0 and dy == 0:
                continue
            sh = np.zeros_like(M)
            ys = slice(max(0, dy), M.shape[0] + min(0, dy))
            yd = slice(max(0, -dy), M.shape[0] + min(0, -dy))
            xs = slice(max(0, dx), M.shape[1] + min(0, dx))
            xd = slice(max(0, -dx), M.shape[1] + min(0, -dx))
            sh[yd, xd] = M[ys, xs]
            out |= sh
    return out


def renderer_fidelity2(ex, target=TARGET) -> dict:
    """Honest fidelity: how much of the REAL picture the extract renderer reproduces.

    The extract carries strokes only — no area fills and no glyph outlines — so a gap in
    text-heavy and hatched blocks is expected and is reported, not hidden.
    """
    p = ex.provenance
    fr = F.block_frame(p["pdf"], p["page_index"], p["coords_px"], p["page_px"][0], p["page_px"][1])
    ref = F.render_block(p["pdf"], p["page_index"], p["coords_px"], p["page_px"][0], p["page_px"][1],
                         target_px=target, frame=fr, production_clip=False)
    out = {}
    tgt = max(ref.width, ref.height)      # match the foundation render pixel-for-pixel
    for name, dt in (("with_text", True), ("no_text", False)):
        mine = C.render_extract(ex, target_px=tgt, draw_text=dt)
        A, B, _ = _masks(ref, mine)
        d = _dilate(B, 1)
        cov = int((A & d).sum()) / max(int(A.sum()), 1)
        d2 = _dilate(A, 1)
        cov2 = int((B & d2).sum()) / max(int(B.sum()), 1)
        out[name] = {"ref_ink": int(A.sum()), "mine_ink": int(B.sum()),
                     "ref_covered_by_mine": round(cov, 4),
                     "mine_covered_by_ref": round(cov2, 4),
                     "iou": round(int((A & B).sum()) / max(int((A | B).sum()), 1), 4)}
    return out
