"""MODE 1 local diff — difference map, change regions, events, routing.

The order is the one the brief demands and the previous audit earned:

    PRIMITIVES -> COMMON PHYSICAL FRAME -> LOCAL INK DIFFERENCE -> EVENT
    -> OBJECT LAYER (address / name only)

The object layer never creates an event.  It only answers "where is this".
"""
from __future__ import annotations

import math
from typing import Any, Sequence

import cv2
import numpy as np

from .core import PAGES, Block, extract_ink, rasterize, text_spans
from .register import Transform, register

DEFAULTS = {
    "cell_pt": 0.6,          # physical resolution of the difference map
    "tol_pt": 1.2,           # matching tolerance (≈0.42 mm on the sheet)
    "merge_pt": 2.4,         # morphological merge radius for change regions
    "min_region_ink_pt": 8.0,  # calibrated on the benchmark (see calibration.json)
    "border_pt": 3.0,        # distance to the crop edge that raises ambiguity
    "text_overlap_drop": 0.65,
}


def _image_coverage(block: Block) -> float:
    """Share of the block area covered by embedded raster images."""
    import fitz

    prec = PAGES.page(block.pdf, block.page_index)
    if prec["n_images"] == 0:
        return 0.0
    page, rm = prec["page"], prec["rot_matrix"]
    bx = block.bbox_vis
    area = max(1e-6, (bx[2] - bx[0]) * (bx[3] - bx[1]))
    covered = 0.0
    seen = set()
    for info in page.get_images(full=True):
        if info[0] in seen:
            continue
        seen.add(info[0])
        try:
            rects = page.get_image_rects(info[0])
        except Exception:
            continue
        for r in rects:
            rv = fitz.Rect(r) * rm
            x0 = max(bx[0], min(rv.x0, rv.x1)); y0 = max(bx[1], min(rv.y0, rv.y1))
            x1 = min(bx[2], max(rv.x0, rv.x1)); y1 = min(bx[3], max(rv.y0, rv.y1))
            if x1 > x0 and y1 > y0:
                covered += (x1 - x0) * (y1 - y0)
    return min(1.0, covered / area)


def _mask_from_boxes(boxes: Sequence[Sequence[float]], frame, cell_pt, pad_pt=0.0) -> np.ndarray:
    x0, y0, x1, y1 = frame
    w = max(1, int(math.ceil((x1 - x0) / cell_pt)))
    h = max(1, int(math.ceil((y1 - y0) / cell_pt)))
    m = np.zeros((h, w), dtype=np.uint8)
    for b in boxes:
        a = int(math.floor((b[0] - pad_pt - x0) / cell_pt))
        c = int(math.floor((b[1] - pad_pt - y0) / cell_pt))
        d = int(math.ceil((b[2] + pad_pt - x0) / cell_pt))
        e = int(math.ceil((b[3] + pad_pt - y0) / cell_pt))
        a, c = max(0, a), max(0, c)
        d, e = min(w, d), min(h, e)
        if d > a and e > c:
            m[c:e, a:d] = 1
    return m


def _cells_to_pt(cells: int, cell_pt: float) -> float:
    """Ink cells -> approximate stroke length in points (1-cell-wide lines)."""
    return float(cells) * cell_pt


def _shape_signature(mask: np.ndarray, box) -> np.ndarray:
    """Coarse 8x8 normalized occupancy signature of a region — used only to pair
    a removal with an addition (POSITION_CHANGED), never to create an event."""
    x0, y0, x1, y1 = box
    sub = mask[y0:y1, x0:x1]
    if sub.size == 0:
        return np.zeros(64, np.float32)
    r = cv2.resize(sub.astype(np.float32), (8, 8), interpolation=cv2.INTER_AREA)
    n = float(np.linalg.norm(r))
    return (r.flatten() / n) if n > 0 else r.flatten()


def _segments_in_box(segs: np.ndarray, box_pt) -> np.ndarray:
    if len(segs) == 0:
        return np.zeros(0, dtype=np.int64)
    xmin = np.minimum(segs[:, 0], segs[:, 2])
    xmax = np.maximum(segs[:, 0], segs[:, 2])
    ymin = np.minimum(segs[:, 1], segs[:, 3])
    ymax = np.maximum(segs[:, 1], segs[:, 3])
    ok = ~((xmax < box_pt[0]) | (xmin > box_pt[2]) | (ymax < box_pt[1]) | (ymin > box_pt[3]))
    return np.nonzero(ok)[0]


# --------------------------------------------------------------------------
# object layer — addressing only
# --------------------------------------------------------------------------
def build_objects(segs: np.ndarray, long_frac: float = 0.12, gap_pt: float = 2.0) -> dict[str, Any]:
    """Generic graphic objects: connected clusters of short primitives after the
    long 'network' runs are set aside.  No discipline knowledge, no naming."""
    if len(segs) == 0:
        return {"objects": [], "long_runs": 0}
    L = np.hypot(segs[:, 2] - segs[:, 0], segs[:, 3] - segs[:, 1])
    xmin, ymin = float(min(segs[:, 0].min(), segs[:, 2].min())), float(min(segs[:, 1].min(), segs[:, 3].min()))
    xmax, ymax = float(max(segs[:, 0].max(), segs[:, 2].max())), float(max(segs[:, 1].max(), segs[:, 3].max()))
    diag = math.hypot(xmax - xmin, ymax - ymin) or 1.0
    is_long = L > long_frac * diag
    short_idx = np.nonzero(~is_long)[0]
    if len(short_idx) == 0:
        return {"objects": [], "long_runs": int(is_long.sum())}

    # cluster endpoints with a union-find over a grid of gap_pt
    parent = list(range(len(short_idx)))

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    grid: dict[tuple[int, int], list[int]] = {}
    for k, i in enumerate(short_idx):
        for (px, py) in ((segs[i, 0], segs[i, 1]), (segs[i, 2], segs[i, 3])):
            cx, cy = int(px // gap_pt), int(py // gap_pt)
            for ox in (-1, 0, 1):
                for oy in (-1, 0, 1):
                    for other in grid.get((cx + ox, cy + oy), ()):
                        union(k, other)
            grid.setdefault((cx, cy), []).append(k)

    groups: dict[int, list[int]] = {}
    for k, i in enumerate(short_idx):
        groups.setdefault(find(k), []).append(int(i))
    objs = []
    for gi, members in enumerate(groups.values()):
        m = segs[members]
        bx = [float(min(m[:, 0].min(), m[:, 2].min())), float(min(m[:, 1].min(), m[:, 3].min())),
              float(max(m[:, 0].max(), m[:, 2].max())), float(max(m[:, 1].max(), m[:, 3].max()))]
        objs.append({"object_id": f"o{gi+1}", "bbox": [round(v, 2) for v in bx],
                     "n_segments": len(members), "ink_pt": round(float(np.hypot(m[:, 2] - m[:, 0], m[:, 3] - m[:, 1]).sum()), 2)})
    return {"objects": objs, "long_runs": int(is_long.sum())}


def address_region(box_pt, objects: list[dict[str, Any]], texts: list[dict[str, Any]], radius_pt: float):
    """Name a proven change region: overlapping generic objects + nearest labels."""
    hits = []
    for o in objects:
        b = o["bbox"]
        if not (b[2] < box_pt[0] or b[0] > box_pt[2] or b[3] < box_pt[1] or b[1] > box_pt[3]):
            hits.append({"object_id": o["object_id"], "n_segments": o["n_segments"], "bbox": b})
    cx = (box_pt[0] + box_pt[2]) / 2
    cy = (box_pt[1] + box_pt[3]) / 2
    near = []
    for t in texts:
        b = t["bbox"]
        tx = min(max(cx, b[0]), b[2])
        ty = min(max(cy, b[1]), b[3])
        d = math.hypot(cx - tx, cy - ty)
        if d <= radius_pt:
            near.append((d, t["text"]))
    near.sort()
    return {"objects": hits[:6], "nearby_text": [t for _, t in near[:5]]}


# --------------------------------------------------------------------------
def local_diff(
    left_block: Block,
    right_block: Block,
    params: dict[str, Any] | None = None,
    allow_rotation: bool = False,
    resolve_border: bool = True,
    border_probe_pt: float = 24.0,
) -> dict[str, Any]:
    p = dict(DEFAULTS)
    if params:
        p.update(params)
    cell, tol = p["cell_pt"], p["tol_pt"]

    left = extract_ink(left_block)
    right = extract_ink(right_block)
    reg = register(left, right, cell_pt=cell, tol_pt=tol, allow_rotation=allow_rotation)
    t: Transform = reg.pop("_t")
    ls: np.ndarray = reg.pop("_ls")
    rs: np.ndarray = reg.pop("_rs")
    lf = reg.pop("_lf")
    rf = reg.pop("_rf")

    frame = reg["frame"]
    lt = t.apply(ls)
    lft = t.apply_polys(lf) if lf else None
    L = rasterize(lt, None, frame, cell, fills=lft)
    R = rasterize(rs, None, frame, cell, fills=rf or None)
    k = max(1, int(round(tol / cell)))
    ker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * k + 1, 2 * k + 1))
    Ld, Rd = cv2.dilate(L, ker), cv2.dilate(R, ker)
    left_only = (L & ~Rd).astype(np.uint8)
    right_only = (R & ~Ld).astype(np.uint8)
    matched = (L & Rd).astype(np.uint8)

    # text is compared by another pipeline — mask it out of the graphic diff
    ltexts = text_spans(left_block)
    rtexts = text_spans(right_block)
    ltx_boxes = [list(t.apply_pts(np.asarray([[b["bbox"][0], b["bbox"][1]], [b["bbox"][2], b["bbox"][3]]], np.float64)).flatten()) for b in ltexts]
    text_mask = _mask_from_boxes([[min(b[0], b[2]), min(b[1], b[3]), max(b[0], b[2]), max(b[1], b[3])] for b in ltx_boxes], frame, cell, pad_pt=1.0)
    text_mask |= _mask_from_boxes([b["bbox"] for b in rtexts], frame, cell, pad_pt=1.0)

    union = ((left_only | right_only) > 0).astype(np.uint8)
    mk = max(1, int(round(p["merge_pt"] / cell)))
    mker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * mk + 1, 2 * mk + 1))
    grown = cv2.dilate(union, mker)
    n_lab, lab, stats, cent = cv2.connectedComponentsWithStats(grown, connectivity=8)

    x0f, y0f = frame[0], frame[1]
    regions = []
    for i in range(1, n_lab):
        sel = (lab == i)
        lo = int((left_only[sel]).sum())
        ro = int((right_only[sel]).sum())
        if lo + ro == 0:
            continue
        tm = int((text_mask[sel] & ((left_only | right_only)[sel] > 0)).sum())
        x, y, w, h, _ = stats[i]
        box_cells = (x, y, x + w, y + h)
        box_pt = [x0f + x * cell, y0f + y * cell, x0f + (x + w) * cell, y0f + (y + h) * cell]
        ink_pt = _cells_to_pt(lo + ro, cell)
        text_share = tm / max(1, lo + ro)
        regions.append({
            "left_only_cells": lo,
            "right_only_cells": ro,
            "ink_pt": round(ink_pt, 2),
            "left_only_ink_pt": round(_cells_to_pt(lo, cell), 2),
            "right_only_ink_pt": round(_cells_to_pt(ro, cell), 2),
            "bbox": [round(v, 2) for v in box_pt],
            "box_cells": box_cells,
            "text_overlap": round(text_share, 3),
            "_sig_left": _shape_signature(left_only, box_cells),
            "_sig_right": _shape_signature(right_only, box_cells),
        })

    # ---- filters -------------------------------------------------------
    published, suppressed = [], []
    for r in regions:
        if r["ink_pt"] < p["min_region_ink_pt"]:
            r["suppressed_by"] = "BELOW_MIN_INK"
            suppressed.append(r)
        elif r["text_overlap"] >= p["text_overlap_drop"]:
            r["suppressed_by"] = "TEXT_REGION"
            suppressed.append(r)
        else:
            published.append(r)

    # ---- border ambiguity ---------------------------------------------
    lb_t = t.apply(np.asarray([[left["bbox_vis"][0], left["bbox_vis"][1], left["bbox_vis"][2], left["bbox_vis"][3]]], np.float32))[0]
    left_rect_in_frame = [min(lb_t[0], lb_t[2]), min(lb_t[1], lb_t[3]), max(lb_t[0], lb_t[2]), max(lb_t[1], lb_t[3])]
    right_rect = list(right["bbox_vis"])
    common = [max(left_rect_in_frame[0], right_rect[0]), max(left_rect_in_frame[1], right_rect[1]),
              min(left_rect_in_frame[2], right_rect[2]), min(left_rect_in_frame[3], right_rect[3])]
    bm = p["border_pt"]
    keep = []
    for r in published:
        b = r["bbox"]
        outside = (b[2] <= common[0] or b[0] >= common[2] or b[3] <= common[1] or b[1] >= common[3])
        if outside:
            r["suppressed_by"] = "OUTSIDE_COMMON_AREA"
            r["border_ambiguity"] = True
            r["border_resolution"] = "CROP_ARTIFACT"
            suppressed.append(r)
            continue
        touches = (b[0] <= common[0] + bm or b[1] <= common[1] + bm or
                   b[2] >= common[2] - bm or b[3] >= common[3] - bm)
        r["border_ambiguity"] = bool(touches)
        r["border_resolution"] = None
        keep.append(r)
    published = keep

    if resolve_border and any(r["border_ambiguity"] for r in published):
        _resolve_border(published, left_block, right_block, t, p, border_probe_pt)
        keep = []
        for r in published:
            if r.get("border_resolution") == "CROP_ARTIFACT":
                r["suppressed_by"] = "CROP_ARTIFACT"
                suppressed.append(r)
            else:
                keep.append(r)
        published = keep

    # ---- events --------------------------------------------------------
    _pair_moves(published)
    for r in published:
        r["change_type"] = _classify(r, matched, cell, tol, p)

    # ---- object addressing (after the change is proven) ----------------
    robjs = build_objects(rs)
    lobjs = build_objects(lt)
    for r in published:
        src = robjs["objects"] if r["right_only_cells"] >= r["left_only_cells"] else lobjs["objects"]
        txt = rtexts if r["right_only_cells"] >= r["left_only_cells"] else ltexts
        txt_frame = txt if r["right_only_cells"] >= r["left_only_cells"] else [
            {"text": s["text"], "bbox": list(t.apply_pts(np.asarray([[s["bbox"][0], s["bbox"][1]], [s["bbox"][2], s["bbox"][3]]], np.float64)).flatten())} for s in txt]
        r["object_context"] = address_region(r["bbox"], src, txt_frame, radius_pt=max(12.0, 0.02 * math.hypot(frame[2] - frame[0], frame[3] - frame[1])))

    for r in published + suppressed:
        r.pop("_sig_left", None)
        r.pop("_sig_right", None)
        r.pop("box_cells", None)

    total_ink_cells = int(L.sum()) + int(R.sum())
    changed_cells = int(left_only.sum()) + int(right_only.sum())
    published_cells = sum(r["left_only_cells"] + r["right_only_cells"] for r in published)

    stats_out = {
        "left_ink_cells": int(L.sum()),
        "right_ink_cells": int(R.sum()),
        "matched_cells": int(matched.sum()),
        "left_only_cells": int(left_only.sum()),
        "right_only_cells": int(right_only.sum()),
        "changed_ink_fraction": round(changed_cells / max(1, total_ink_cells), 5),
        "published_ink_fraction": round(published_cells / max(1, total_ink_cells), 5),
        "n_regions_raw": len(regions),
        "n_regions_published": len(published),
        "n_regions_suppressed": len(suppressed),
        "left_texts": len(ltexts),
        "right_texts": len(rtexts),
    }
    img_l, img_r = _image_coverage(left_block), _image_coverage(right_block)
    # "text drawn as curves": no text layer at all on one side while the other
    # side has a real one, and the curve side carries enough geometry for that
    # to matter.  The thresholds are deliberately blunt — this only routes.
    curves_l = len(ltexts) == 0 and len(ls) > 2000 and len(rtexts) >= 20
    curves_r = len(rtexts) == 0 and len(rs) > 2000 and len(ltexts) >= 20
    extraction = {
        "left": {k: left[k] for k in ("n_paths_seen", "n_paths_kept", "n_invisible_paths", "segments_dropped_invisible", "n_page_images", "page_rotation")},
        "right": {k: right[k] for k in ("n_paths_seen", "n_paths_kept", "n_invisible_paths", "segments_dropped_invisible", "n_page_images", "page_rotation")},
        "left_segments": int(len(left["segments"])),
        "right_segments": int(len(right["segments"])),
        "image_coverage": [round(img_l, 4), round(img_r, 4)],
        "text_as_curves": [bool(curves_l), bool(curves_r)],
        "flags": {
            "raster_backed_side": bool((img_l > 0.5) != (img_r > 0.5)),
            "text_as_curves_asymmetry": bool(curves_l != curves_r),
        },
    }
    out = {
        "mode": "MODE_1",
        "params": p,
        "left": {"pdf": left_block.pdf, "page_index": left_block.page_index, "block_id": left_block.block_id,
                 "bbox_vis": [round(v, 2) for v in left_block.bbox_vis], "label": left_block.label},
        "right": {"pdf": right_block.pdf, "page_index": right_block.page_index, "block_id": right_block.block_id,
                  "bbox_vis": [round(v, 2) for v in right_block.bbox_vis], "label": right_block.label},
        "registration": reg,
        "extraction": extraction,
        "diff": stats_out,
        "change_regions": sorted(published, key=lambda r: -r["ink_pt"]),
        "suppressed_regions": sorted(suppressed, key=lambda r: -r["ink_pt"])[:50],
    }
    out["route"], out["route_reason"] = route(out)
    if out["route"] == "MODE_1_APPLICABLE" and not out["change_regions"]:
        out["verdict"] = "NO_GRAPHIC_CHANGE"
    elif out["route"] == "MODE_1_APPLICABLE":
        out["verdict"] = "LOCAL_CHANGE"
    else:
        out["verdict"] = out["route"]
    return out


def _classify(r: dict[str, Any], matched: np.ndarray, cell: float, tol: float, p) -> str:
    # a region resolved as a crop artifact never reaches this function: it is
    # suppressed.  What is left is either proven beyond the border or unresolved.
    if r.get("border_ambiguity") and r.get("border_resolution") is None:
        return "BORDER_AMBIGUITY"
    if r.get("moved_with") is not None:
        return "POSITION_CHANGED"
    lo, ro = r["left_only_cells"], r["right_only_cells"]
    if ro == 0 or lo / max(1, ro) > 6:
        return "REMOVED_GRAPHIC"
    if lo == 0 or ro / max(1, lo) > 6:
        return "ADDED_GRAPHIC"
    return "GEOMETRY_CHANGED"


def _pair_moves(regions: list[dict[str, Any]], sim_thr: float = 0.9) -> None:
    """Pair a removal with an addition of the same shape -> POSITION_CHANGED."""
    rem = [r for r in regions if r["right_only_cells"] == 0 and r["left_only_cells"] > 0]
    add = [r for r in regions if r["left_only_cells"] == 0 and r["right_only_cells"] > 0]
    for r in regions:
        r["moved_with"] = None
    used = set()
    for i, a in enumerate(rem):
        best, bs = None, sim_thr
        for j, b in enumerate(add):
            if j in used:
                continue
            if abs(a["left_only_cells"] - b["right_only_cells"]) > 0.25 * max(a["left_only_cells"], b["right_only_cells"]):
                continue
            s = float(np.dot(a["_sig_left"], b["_sig_right"]))
            if s > bs:
                best, bs = j, s
        if best is not None:
            used.add(best)
            a["moved_with"] = add[best]["bbox"]
            add[best]["moved_with"] = a["bbox"]
            a["move_similarity"] = round(bs, 3)
            add[best]["move_similarity"] = round(bs, 3)


def _resolve_border(regions, left_block: Block, right_block: Block, t: Transform, p, probe_pt: float) -> None:
    """Look at the SOURCE PAGE over exactly the change region, on both sides.

    The prepared block contract is untouched: the probe never changes the block
    and never adds geometry to the comparison.  It only answers one question —
    "does this ink exist on the other page at this place, outside that crop?" —
    and annotates the region with the answer.
    """
    cell, tol = p["cell_pt"], p["tol_pt"]
    inv = t.inverse()
    k = max(1, int(round(tol / cell)))
    ker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * k + 1, 2 * k + 1))
    for r in regions:
        if not r["border_ambiguity"]:
            continue
        b = r["bbox"]
        pad = max(4.0, 0.1 * max(b[2] - b[0], b[3] - b[1]))
        rrect = [b[0] - pad, b[1] - pad, b[2] + pad, b[3] + pad]
        corners = np.asarray([[rrect[0], rrect[1]], [rrect[2], rrect[1]],
                              [rrect[2], rrect[3]], [rrect[0], rrect[3]]], dtype=np.float64)
        lc = inv.apply_pts(corners)
        lrect = [float(lc[:, 0].min()), float(lc[:, 1].min()), float(lc[:, 0].max()), float(lc[:, 1].max())]
        try:
            lw = extract_ink(Block(left_block.pdf, left_block.page_index, "probe_l", lrect))
            rw = extract_ink(Block(right_block.pdf, right_block.page_index, "probe_r", rrect))
        except Exception:
            r["border_resolution"] = None
            continue
        lseg = t.apply(lw["segments"])
        lfil = t.apply_polys(lw["fills"]) if lw["fills"] else None
        Lw = rasterize(lseg, None, rrect, cell, fills=lfil)
        Rw = rasterize(rw["segments"], None, rrect, cell, fills=rw["fills"] or None)
        lo = int((Lw & ~cv2.dilate(Rw, ker)).sum())
        ro = int((Rw & ~cv2.dilate(Lw, ker)).sum())
        before = r["left_only_cells"] + r["right_only_cells"]
        after = lo + ro
        r["border_probe"] = {"unmatched_cells_in_block": before,
                             "unmatched_cells_on_page": after,
                             "probe_rect": [round(v, 2) for v in rrect]}
        r["border_resolution"] = "CROP_ARTIFACT" if after <= 0.3 * max(1, before) else "REAL_BEYOND_BORDER"


# --------------------------------------------------------------------------
# routing
# --------------------------------------------------------------------------
GATES = {
    "min_ink_pt": 200.0,           # below this a block has no graphics worth comparing
    "min_sym_cov": 0.80,           # matched ink coverage after registration
    "max_changed_fraction": 0.25,  # more than this is a redesign, not a local edit
    "max_regions": 40,             # a wall of regions is a redesign, not a local edit
    "min_anchor_cov": 0.10,
    "vision_max_ink_pt": 60.0,     # small, uncertain regions get a targeted look
}


def route(out: dict[str, Any], gates: dict[str, float] | None = None) -> tuple[str, str]:
    g = dict(GATES)
    if gates:
        g.update(gates)
    reg = out["registration"]
    d = out["diff"]
    ex = out.get("extraction", {})
    flags = ex.get("flags", {})
    left_ink = reg["left_ink_pt"]
    right_ink = reg["right_ink_pt"]
    if max(left_ink, right_ink) < g["min_ink_pt"]:
        return "NO_GRAPHIC_COMPARISON", f"too little vector graphics ({left_ink:.0f}/{right_ink:.0f} pt)"
    if flags.get("raster_backed_side"):
        return "VISION_REQUIRED", (f"one side is raster-backed (image coverage "
                                   f"{ex['image_coverage'][0]:.2f} vs {ex['image_coverage'][1]:.2f}): "
                                   "there is no vector geometry to compare on that side")
    if flags.get("text_as_curves_asymmetry"):
        return "VISION_REQUIRED", ("text is drawn as curves on one side only: its outlines enter the "
                                   "geometry and cannot be told from graphics deterministically")
    if not reg["success"]:
        return "MODE_2_REQUIRED", f"registration failed: {reg['failure_reason']}"
    if reg["coverage"]["sym_cov"] < g["min_sym_cov"]:
        return "MODE_2_REQUIRED", f"matched ink coverage {reg['coverage']['sym_cov']:.3f} < {g['min_sym_cov']}"
    if d["changed_ink_fraction"] > g["max_changed_fraction"]:
        return "MODE_2_REQUIRED", f"changed ink fraction {d['changed_ink_fraction']:.3f} > {g['max_changed_fraction']}"
    if d["n_regions_published"] > g["max_regions"]:
        return "MODE_2_REQUIRED", f"{d['n_regions_published']} change regions > {g['max_regions']}"
    unresolved = [r for r in out["change_regions"] if r.get("change_type") == "BORDER_AMBIGUITY"]
    small = [r for r in out["change_regions"] if r["ink_pt"] <= g["vision_max_ink_pt"]]
    if unresolved:
        return "VISION_REQUIRED", f"{len(unresolved)} region(s) unresolved at the crop border"
    if small and len(out["change_regions"]) <= 5:
        return "MODE_1_APPLICABLE", f"{len(small)} small region(s) eligible for a targeted look"
    return "MODE_1_APPLICABLE", "registration and local diff within gates"
