# -*- coding: utf-8 -*-
"""`ctr` probe — MINIMAL GraphicBlockDescription v0.3 contract.

Every field emitted here is justified by a measurement of this track; the ablation
driver (ctr_ablate.py) removes each one and reports what breaks.  Blocks are read
ONLY through v03_foundation, objects ONLY through v03_objects, families through
fam_family.  No own extraction.
"""
from __future__ import annotations

import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXP = HERE.parent
ROOT = EXP.parents[1]
ART = EXP / "artifacts"
sys.path.insert(0, str(HERE))

import v03_foundation as F      # noqa: E402
import v03_objects as O         # noqa: E402
import fam_family as FAM        # noqa: E402
import grp_common as GC         # noqa: E402

CONTRACT = "GraphicBlockDescription/0.3"

# --------------------------------------------------------------------------- utils

def tokens(obj) -> int:
    """Same token convention as grp G6' / hyb: len(json)/4."""
    return int(len(json.dumps(obj, ensure_ascii=False)) / 4)


def nbytes(obj) -> int:
    return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))


def polygon_ring_display(pb, ex):
    """The block polygon mapped into DISPLAY points — the same space the objects live in."""
    if pb.shape_type != "polygon" or not pb.polygon_points:
        return None
    fd = ex.frame["clip_display"]
    w_px = max(1e-9, pb.coords_px[2] - pb.coords_px[0])
    h_px = max(1e-9, pb.coords_px[3] - pb.coords_px[1])
    sx = (fd[2] - fd[0]) / w_px
    sy = (fd[3] - fd[1]) / h_px
    return [[round(fd[0] + (q[0] - pb.coords_px[0]) * sx, 2),
             round(fd[1] + (q[1] - pb.coords_px[1]) * sy, 2)] for q in pb.polygon_points]


def point_in_ring(px, py, ring) -> bool:
    c = False
    n = len(ring)
    for i in range(n):
        x0, y0 = ring[i]
        x1, y1 = ring[(i + 1) % n]
        if (y0 > py) != (y1 > py):
            xi = x0 + (py - y0) * (x1 - x0) / (y1 - y0 + 1e-12)
            if px < xi:
                c = not c
    return c


def ink_outside_polygon(ex, ring) -> float:
    if not ring:
        return 0.0
    tot = out = 0.0
    for sg in ex.segments:
        mx = (sg["p0"][0] + sg["p1"][0]) / 2
        my = (sg["p0"][1] + sg["p1"][1]) / 2
        tot += sg["len"]
        if not point_in_ring(mx, my, ring):
            out += sg["len"]
    return round(out / max(1e-9, tot), 4)


def _polygon_area(pts) -> float:
    if not pts or len(pts) < 3:
        return 0.0
    a = 0.0
    for i in range(len(pts)):
        x0, y0 = pts[i][0], pts[i][1]
        x1, y1 = pts[(i + 1) % len(pts)][0], pts[(i + 1) % len(pts)][1]
        a += x0 * y1 - x1 * y0
    return abs(a) / 2.0


def _params_digest(*dicts) -> str:
    payload = json.dumps([{k: v for k, v in sorted(d.items())} for d in dicts],
                         ensure_ascii=False, default=str)
    return hashlib.sha1(payload.encode()).hexdigest()[:12]


def page_text_lines(pdf_path: str, page_index: int) -> int:
    doc = F.open_doc(pdf_path)
    pg = doc[page_index]
    try:
        d = pg.get_text("dict")
    except Exception:
        return -1
    return sum(len(b.get("lines", [])) for b in d.get("blocks", []) if b.get("type") == 0)


DENSITY_BANDS = [(200, "sparse"), (1500, "light"), (5000, "medium"),
                 (15000, "dense"), (50000, "very_dense")]


def density_band(n_seg: int) -> str:
    for thr, name in DENSITY_BANDS:
        if n_seg < thr:
            return name
    return "extreme"


# ----------------------------------------------------------------------- contract

def describe(pb, ex, layer, fam, block_class: str, page_text: int,
             *, with_objects=True, with_desc=True, with_label=True,
             with_family=True, with_border=True) -> dict:
    """Build the v0.3 payload for ONE prepared graphic block."""
    fr = ex.frame
    clip = fr["clip_page"]
    w_pt, h_pt = clip[2] - clip[0], clip[3] - clip[1]

    ring = polygon_ring_display(pb, ex)
    poly_share = None
    if pb.shape_type == "polygon" and pb.polygon_points:
        sx = w_pt / max(1e-9, (pb.coords_px[2] - pb.coords_px[0]))
        sy = h_pt / max(1e-9, (pb.coords_px[3] - pb.coords_px[1]))
        area_poly = _polygon_area(pb.polygon_points) * sx * sy
        poly_share = round(area_poly / max(1e-9, w_pt * h_pt), 4)

    q = ex.quality
    out = {
        "contract": CONTRACT,
        "provenance": {
            "pdf_sha256": ex.provenance["pdf_sha256"],
            "pdf": str(Path(pb.pdf_path).relative_to(ROOT)) if str(pb.pdf_path).startswith(str(ROOT)) else pb.pdf_path,
            "result_json": str(Path(pb.result_json).relative_to(ROOT)) if str(pb.result_json).startswith(str(ROOT)) else pb.result_json,
            "block_id": pb.block_id,
            "page_number": pb.page_number,
            "page_index": pb.page_index,
            "page_index_field": pb.page_index_field,
            "page_index_conflict": pb.page_index_conflict,
            "coords_px": [float(v) for v in pb.coords_px],
            "page_px": [pb.page_px_w, pb.page_px_h],
            "rotation": pb.rotation,
            "rotation_source": pb.rotation_source,
            "shape_type": pb.shape_type,
            "polygon_area_share": poly_share,
            "polygon_pt": ring,
            "extractor": {
                "module": "v03_foundation",
                "drop_invisible": ex.provenance["drop_invisible"],
                "curve_steps": ex.provenance["curve_steps"],
                "params_sha": _params_digest(layer.params),
            },
        },
        "frame": {
            "clip_display_pt": [round(v, 3) for v in fr["clip_display"]],
            "clip_page_pt": [round(v, 3) for v in clip],
            "size_pt": [round(w_pt, 3), round(h_pt, 3)],
            "px_per_pt": [round(1.0 / fr["scale_x"], 4), round(1.0 / fr["scale_y"], 4)],
        },
        "scale": {
            "S": round(layer.S, 4),
            "S_source": layer.scale_source,
            "s_text": round(ex.char_scale["s_text"], 4),
            "s_geom": round(ex.char_scale["s_geom"], 4),
            "n_text_lines": ex.char_scale["n_text"],
            "S_shared": None,           # filled by the pair context, see pair_context()
        },
        "quality": {
            "block_class": block_class,
            "n_seg": ex.inked_segments_count,
            "has_vector": q["has_vector"],
            "raster_coverage": q["raster_coverage"],
            "no_text": q["no_text"],
            "n_curves": q["n_curves"],
            "page_text_lines": page_text,
            "broken_text": q["broken_text"],
            "garbled_ratio": q["garbled_ratio"],
            "invisible_share": q["invisible_share_segments"],
            "border_share": ex.clipped_at_border_flags["share"],
            "ink_outside_polygon_share": ink_outside_polygon(ex, ring),
            "frame_clamped": ex.clipped_at_border_flags["frame_clamped"],
            "route": route_of(block_class, q, ex.inked_segments_count, page_text),
        },
    }

    if with_objects:
        objs = []
        for oi, o in enumerate(layer.objects):
            rec = {
                "oid": o["object_id"],
                "cls": o["cls"],
                "bbox": o["bbox"],
                "ink_pt": o["seg_len"],
            }
            if with_border:
                rec["border"] = bool(any(layer_seg_border(ex, g) for g in o["segments"]))
            if with_desc:
                rec["desc"] = [round(v, 4) for v in o["desc"]["vec"]]
            if with_label:
                rec["label"] = o.get("label")
            if with_family and fam is not None:
                fi = fam.obj_family[oi]
                rec["fam"] = fi if fi >= 0 else None
            objs.append(rec)
        out["objects"] = objs

    if with_family and fam is not None:
        out["families"] = [{"fid": i, "n": len(f["members"]), "ink_pt": round(f["seg_len_sum"], 2)}
                           for i, f in enumerate(fam.families) if len(f["members"]) >= 2]

    return out


def layer_seg_border(ex, gi) -> bool:
    try:
        return bool(ex.segments[gi].get("border"))
    except Exception:
        return False


def route_of(block_class: str, q: dict, n_seg: int, page_text: int) -> str:
    if block_class in ("stamp", "table"):
        return "TABLE_PIPELINE"
    if not q["has_vector"]:
        return "VISION_ONLY"
    if q["raster_coverage"] > 0.5 and n_seg < 20:
        return "VISION_ONLY"
    if q["no_text"] and q["n_curves"] >= 20 and page_text == 0:
        return "VISION_ONLY:text_in_curves"
    if n_seg >= 50000:
        return "VECTOR:tile"
    return "VECTOR"


def pair_context(desc_a: dict, desc_b: dict) -> dict:
    """The 4 pair-level fields.  Each of them is a measured requirement."""
    sa, sb = desc_a["scale"]["S"], desc_b["scale"]["S"]
    fa, fb = desc_a["frame"]["clip_pt"], desc_b["frame"]["clip_pt"]
    inter = [max(fa[0], fb[0]), max(fa[1], fb[1]), min(fa[2], fb[2]), min(fa[3], fb[3])]
    return {
        "same_pdf": desc_a["provenance"]["pdf_sha256"] == desc_b["provenance"]["pdf_sha256"],
        "S_shared": round(max(sa, sb), 4),
        "common_frame_pt": [round(v, 3) for v in inter] if inter[2] > inter[0] and inter[3] > inter[1] else None,
        "transform": {"t": None, "s": None, "rot": None},   # filled by registration (mov M1)
        "comparable_share": None,                            # min(A,B) ink inside common frame
    }
