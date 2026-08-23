# -*- coding: utf-8 -*-
"""VECTOR 0.3 · controlled COUNTERFACTUAL engine (probe `cf`).

    extract2, manifest = apply(extract, objects, cf_id, **params)

``extract``  — ``v03_foundation.BlockExtract`` of a REAL prepared graphic block
               (the only allowed way to read a block);
``objects``  — ``v03_objects.ObjectLayer`` built from that extract (may be ``None``
               for classes A / B / D which never need the object layer);
``cf_id``    — id from ``CF_SPECS`` (classes A, B, C, D of BRIEF_COUNTERFACTUALS.md);
``manifest`` — the exact ground truth: which objects were touched (object_id + bbox in
               PDF points), which primitives changed, delta, seed, expected verdict and
               expected ledger records.  Without it recall cannot be measured.

Nothing outside experiments/stage_comparison_vector_objects_v03_opus/ is written.
Temporary rewritten PDFs (A7 / B5 / D9) go to a scratch dir and are deleted.

Coordinates: display PDF points everywhere (same space the foundation emits).
"""
from __future__ import annotations

import copy
import hashlib
import math
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

import fitz

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

import v03_foundation as F        # noqa: E402
import v03_objects as O           # noqa: E402
import grp_common as G            # noqa: E402  (class-A rewrites live there; reused, not re-written)

SCHEMA = "v03-counterfactual-1"
BASE_SEED = 20260823
SCRATCH = Path(os.environ.get("CF_SCRATCH", tempfile.gettempdir())) / "v03_cf" / str(os.getpid())
DEJAVU = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"


class CFNotApplicable(Exception):
    """The carrier block does not offer what this counterfactual needs."""


# ------------------------------------------------------------------ small helpers

def _seed_for(cf_id: str, key: str, extra: str = "") -> int:
    h = hashlib.sha1(f"{cf_id}|{key}|{extra}".encode()).hexdigest()[:8]
    return (BASE_SEED ^ int(h, 16)) & 0x7FFFFFFF


def _frame_of(ex) -> tuple[float, float, float, float]:
    return tuple(float(v) for v in ex.frame["clip_display"])          # x0,y0,x1,y1


def _block_geom(ex) -> dict:
    x0, y0, x1, y1 = _frame_of(ex)
    w, h = x1 - x0, y1 - y0
    return {"x0": x0, "y0": y0, "x1": x1, "y1": y1, "w": w, "h": h,
            "diag": math.hypot(w, h), "area": max(w * h, 1e-9)}


def _clone(ex, *, segments=None, texts=None, frame=None, images=None, prov=None):
    """A shallow BlockExtract clone with replaced parts (never mutates the original)."""
    new = copy.copy(ex)
    new.segments = segments if segments is not None else [dict(s) for s in ex.segments]
    new.texts = texts if texts is not None else [dict(t) for t in ex.texts]
    new.images = images if images is not None else [dict(i) for i in ex.images]
    new.frame = dict(frame if frame is not None else ex.frame)
    new.quality = dict(ex.quality)
    new.char_scale = dict(ex.char_scale)
    new.provenance = dict(ex.provenance)
    if prov:
        new.provenance.update(prov)
    new.inked_segments_count = len(new.segments)
    return new


def _renumber(segs) -> list[dict]:
    for k, s in enumerate(segs):
        s["i"] = k
    return segs


def _mk_seg(p0, p1, style, *, src=None, path=10 ** 6, op="l", tag=None) -> dict:
    return {"i": -1, "p0": (float(p0[0]), float(p0[1])), "p1": (float(p1[0]), float(p1[1])),
            "len": math.hypot(p1[0] - p0[0], p1[1] - p0[1]),
            "path": path, "op": op, "closed": False,
            "w": style[0], "color": style[1], "fill": style[2],
            "ink_rule": None, "border": False,
            "src": list(src or []), "cf_tag": tag}


def _seg_style(s) -> tuple:
    return (s.get("w"), s.get("color"), s.get("fill"))


def _bbox_of_segs(segs) -> list[float]:
    xs = [p for s in segs for p in (s["p0"][0], s["p1"][0])]
    ys = [p for s in segs for p in (s["p0"][1], s["p1"][1])]
    return [min(xs), min(ys), max(xs), max(ys)]


def _obj_record(obj, geom) -> dict:
    bb = obj["bbox"]
    area = max(bb[2] - bb[0], 1e-9) * max(bb[3] - bb[1], 1e-9)
    return {"object_id": obj["object_id"], "cls": obj["cls"],
            "bbox_pt": [round(v, 3) for v in bb],
            "n_seg": obj["n_seg"], "n_prim": obj["n_prim"],
            "diag_pt": obj["diag"],
            "area_frac_of_block": round(area / geom["area"], 6),
            "len_frac_of_block": None}


def size_bucket(area_frac: float) -> str:
    if area_frac < 0.001:
        return "tiny"          # < 0.1 % of the block area
    if area_frac < 0.01:
        return "small"         # 0.1 - 1 %
    return "large"             # > 1 %


# ------------------------------------------------------------------ renderer
# A deterministic rasteriser OF THE EXTRACT (not of the PDF).  Its only job is to make
# "before" and "after" comparable in the same frame; its fidelity against the
# foundation's render_block is measured separately (cf_selfcheck: renderer_fidelity).

def render_extract(ex, *, frame=None, target_px=1200, out_png=None,
                   draw_text=True, max_segments=200000, force_black=False):
    x0, y0, x1, y1 = frame if frame is not None else _frame_of(ex)
    w, h = max(x1 - x0, 1e-6), max(y1 - y0, 1e-6)
    # NB: no production-style clamp here — a clamp would break the B2 scale check
    rs = max(0.05, min(64.0, target_px / max(w, h)))
    doc = fitz.open()
    page = doc.new_page(width=w * rs, height=h * rs)
    segs = ex.segments[:max_segments]
    groups: dict[tuple, list] = {}
    for s in segs:
        col = (0.0, 0.0, 0.0) if force_black else tuple(s.get("color") or (0.0, 0.0, 0.0))
        wid = max(float(s.get("w") or 0.0), 0.35)
        groups.setdefault((col, round(wid, 2)), []).append(s)
    for (col, wid), items in sorted(groups.items(), key=lambda kv: repr(kv[0])):
        shape = page.new_shape()
        for s in items:
            a = ((s["p0"][0] - x0) * rs, (s["p0"][1] - y0) * rs)
            b = ((s["p1"][0] - x0) * rs, (s["p1"][1] - y0) * rs)
            shape.draw_line(fitz.Point(*a), fitz.Point(*b))
        shape.finish(color=col, width=max(wid * rs, 0.6), closePath=False)
        shape.commit()
    if draw_text and ex.texts:
        try:
            font = fitz.Font(fontfile=DEJAVU)
            tw = fitz.TextWriter(page.rect)
            for t in ex.texts:
                bb = t["bbox"]
                size = max(float(t.get("size") or 0.0) * rs, 1.0)
                d = t.get("dir") or [1, 0]
                pos = fitz.Point((bb[0] - x0) * rs, (bb[3] - y0) * rs)
                if abs(d[1]) > 0.5:                    # vertical text
                    pos = fitz.Point((bb[0] - x0) * rs, (bb[3] - y0) * rs)
                try:
                    tw.append(pos, t["text"], font=font, fontsize=size)
                except Exception:
                    pass
            tw.write_text(page, color=(0, 0, 0))
        except Exception:
            pass
    pix = page.get_pixmap(alpha=False)
    if out_png:
        Path(out_png).parent.mkdir(parents=True, exist_ok=True)
        pix.save(str(out_png))
    return pix


def pix_to_bin(pix, thr=200):
    """Dark-pixel mask (numpy bool array) of a rendered pixmap."""
    import numpy as np
    a = np.frombuffer(pix.samples, dtype=np.uint8)
    a = a.reshape(pix.height, pix.stride)[:, : pix.width * pix.n]
    a = a.reshape(pix.height, pix.width, pix.n)
    if pix.n >= 3:
        v = a[:, :, :3].mean(axis=2)
    else:
        v = a[:, :, 0]
    return v < thr


def raster_diff(pa, pb, thr=200) -> dict:
    """Fraction of pixels whose ink state differs, plus the bbox of the change."""
    import numpy as np
    if pa.width != pb.width or pa.height != pb.height:
        return {"error": "size_mismatch", "a": [pa.width, pa.height], "b": [pb.width, pb.height]}
    A = pix_to_bin(pa, thr)
    B = pix_to_bin(pb, thr)
    D = A ^ B
    diff = int(D.sum())
    inkA = int(A.sum())
    inkB = int(B.sum())
    inter = int((A & B).sum())
    bbox = None
    if diff:
        ys, xs = np.nonzero(D)
        bbox = [int(xs.min()), int(ys.min()), int(xs.max()) + 1, int(ys.max()) + 1]
    tot = A.size
    return {"w": pa.width, "h": pa.height, "px_total": int(tot),
            "diff_px": diff, "diff_frac": diff / tot,
            "ink_a": inkA, "ink_b": inkB,
            "ink_diff_frac_of_a": diff / max(inkA, 1),
            "ink_iou": inter / max(inkA + inkB - inter, 1),
            "diff_bbox_px": bbox}


def ink_iou(pa, pb, thr=200) -> float:
    A = pix_to_bin(pa, thr)
    B = pix_to_bin(pb, thr)
    inter = int((A & B).sum())
    uni = int((A | B).sum())
    return inter / max(uni, 1)


# ---------------- strict circle detector (the loose one turns squares into circles)

def _circles_strict(segs, *, resid_rel=0.02, min_seg=6, max_step_deg=50.0):
    """Closed chains that are REALLY circles.

    ``grp_common._closed_circles`` accepts any closed chain of >=4 segments whose points
    fit a circle — and the four corners of a rectangle fit a circle exactly.  Measured
    consequence: the A4 rewrite silently turned rectangles into circles (picture diff up
    to 3 % of the block).  Here a chain additionally needs >= ``min_seg`` segments and an
    angular step <= ``max_step_deg`` between neighbouring points, which rejects polygons.
    """
    out = []
    for ch in G._geo_chains(segs):
        if len(ch) < min_seg:
            continue
        p0 = segs[ch[0]]["p0"]
        p1 = segs[ch[-1]]["p1"]
        if math.hypot(p0[0] - p1[0], p0[1] - p1[1]) > 1e-2:
            continue
        pts = [tuple(segs[ch[0]]["p0"])] + [tuple(segs[g]["p1"]) for g in ch]
        fit = O._fit_circle(pts)
        if fit is None:
            continue
        cx, cy, r, resid = fit
        if r <= 1e-6 or resid / r > resid_rel:
            continue
        angs = [math.atan2(y - cy, x - cx) for x, y in pts]
        step_ok = True
        for k in range(1, len(angs)):
            d = angs[k] - angs[k - 1]
            while d > math.pi:
                d -= 2 * math.pi
            while d < -math.pi:
                d += 2 * math.pi
            if abs(math.degrees(d)) > max_step_deg:
                step_ok = False
                break
        if not step_ok:
            continue
        out.append((ch, cx, cy, r))
    return out


def _recode_circles_strict(segs, encoder):
    circles = _circles_strict(segs)
    in_ch = {}
    for ci, (ch, *_r) in enumerate(circles):
        for k in ch:
            in_ch[k] = ci
    out, done, touched = [], set(), 0
    for k, s in enumerate(segs):
        ci = in_ch.get(k)
        if ci is None:
            t = dict(s); t["src"] = [s["i"]]; out.append(t); continue
        if ci in done:
            continue
        done.add(ci)
        ch, cx, cy, r = circles[ci]
        touched += len(ch)
        src = [segs[g]["i"] for g in ch]
        style = (segs[ch[0]]["w"], segs[ch[0]]["color"], segs[ch[0]]["fill"])
        a0 = math.atan2(segs[ch[0]]["p0"][1] - cy, segs[ch[0]]["p0"][0] - cx)
        pts = encoder(cx, cy, r, a0)
        for j in range(len(pts) - 1):
            out.append(_mk_seg(pts[j], pts[j + 1], style, src=src, path=segs[ch[0]]["path"],
                               op="c", tag="A4_recoded"))
    _renumber(out)
    return out, touched, len(circles)


def _rw_circle(encoder):
    def f(segs, rng):
        out, touched, n = _recode_circles_strict(segs, encoder)
        if n == 0:
            raise CFNotApplicable("no real circle to re-encode")
        return out
    return f


# ------------------------------------------------------------------ class A

_A_VARIANTS = {
    "A1_path_split": "A1_path_split",
    "A2_path_merge": "A2_path_merge",
    "A3_curve_resample_down": "A3_curve_resample_down",
    "A3_curve_resample_up": "A3_curve_resample_up",
    "A4_circle_to_bezier": "A4_circle_to_bezier",
    "A4b_circle_to_chords5": "A4b_circle_to_chords5",
    "A4c_circle_to_chords24": "A4c_circle_to_chords24",
    "A5_order_shuffle": "A5_order_shuffle",
    "A6_round_0.01": "A6_round_0.01",
    "A6_round_0.1": "A6_round_0.1",
    "A6_round_0.25": "A6_round_0.25",
    "A6_round_0.5": "A6_round_0.5",
    "A8_lineweight": "A8_lineweight",
}


_STRICT_A4 = {
    "A4_circle_to_bezier": lambda: _rw_circle(G._enc_bezier),
    "A4b_circle_to_chords5": lambda: _rw_circle(G._enc_chords(5)),
    "A4c_circle_to_chords24": lambda: _rw_circle(G._enc_chords(24)),
}


def _apply_A(ex, objects, cf_id, key, **params):
    fn = _STRICT_A4[cf_id]() if cf_id in _STRICT_A4 else G.REWRITES[_A_VARIANTS[cf_id]]
    seed = _seed_for(cf_id, key)
    rng = random.Random(seed)
    segs = fn(ex.segments, rng)
    if not segs:
        raise CFNotApplicable("rewrite produced no segments")
    bite = G.rewrite_bite(cf_id, ex.segments, segs)
    if cf_id.startswith(("A3", "A4")) and bite == 0:
        raise CFNotApplicable("no curve/circle chains to re-encode")
    _renumber(segs)
    ex2 = _clone(ex, segments=segs, prov={"cf": cf_id, "cf_seed": seed})
    man = {
        "cf_class": "A", "cf_id": cf_id, "seed": seed, "params": {},
        "touched_objects": [],                       # A touches the whole packaging
        "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                               "segments_touched": bite,
                               "share_touched": round(bite / max(len(ex.segments), 1), 4)},
        "delta": None,
        "expected_verdict": "NO_GRAPHIC_CHANGE",
        "expected_ledger": [],
        # A8 changes stroke WIDTH and COLOUR: the centre lines stay, the raster does not.
        "invariants": {"picture": not cf_id.startswith("A8"),
                       "geometry_exact": cf_id.startswith(("A1", "A2", "A5", "A8")),
                       "text": True},
    }
    return ex2, man


# ------------------------------------------------------------------ class B

def _affine_segments(segs, fn):
    out = []
    for s in segs:
        t = dict(s)
        t["p0"] = fn(s["p0"])
        t["p1"] = fn(s["p1"])
        t["len"] = math.hypot(t["p1"][0] - t["p0"][0], t["p1"][1] - t["p0"][1])
        t["src"] = [s["i"]]
        out.append(t)
    return _renumber(out)


def _affine_texts(texts, fn, scale=1.0):
    out = []
    for t in texts:
        u = dict(t)
        bb = t["bbox"]
        a = fn((bb[0], bb[1]))
        b = fn((bb[2], bb[3]))
        u["bbox"] = [min(a[0], b[0]), min(a[1], b[1]), max(a[0], b[0]), max(a[1], b[1])]
        u["cx"] = (u["bbox"][0] + u["bbox"][2]) / 2
        u["cy"] = (u["bbox"][1] + u["bbox"][3]) / 2
        u["size"] = float(t.get("size") or 0.0) * scale
        out.append(u)
    return out


def _apply_B_translate(ex, key, frac):
    g = _block_geom(ex)
    seed = _seed_for("B1_translate", key, str(frac))
    rng = random.Random(seed)
    ang = rng.uniform(0, 2 * math.pi)
    d = frac * g["diag"]
    dx, dy = d * math.cos(ang), d * math.sin(ang)
    fn = lambda p: (p[0] + dx, p[1] + dy)
    fr = dict(ex.frame)
    fr["clip_display"] = [g["x0"] + dx, g["y0"] + dy, g["x1"] + dx, g["y1"] + dy]
    ex2 = _clone(ex, segments=_affine_segments(ex.segments, fn),
                 texts=_affine_texts(ex.texts, fn), frame=fr,
                 prov={"cf": "B1_translate", "cf_seed": seed})
    man = {"cf_class": "B", "cf_id": "B1_translate", "seed": seed,
           "params": {"frac_of_diag": frac},
           "touched_objects": [], "changed_primitives": {"n_before": len(ex.segments),
                                                          "n_after": len(ex.segments),
                                                          "segments_touched": len(ex.segments)},
           "delta": {"dx_pt": round(dx, 4), "dy_pt": round(dy, 4), "d_pt": round(d, 4)},
           "expected_verdict": "NO_GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "BLOCK_TRANSFORMED", "kind": "translate",
                                "dx_pt": round(dx, 4), "dy_pt": round(dy, 4)}],
           "invariants": {"picture_after_compensation": True, "geometry_exact": False, "text": True},
           "compensation": {"kind": "translate", "dx_pt": dx, "dy_pt": dy}}
    return ex2, man


def _apply_B_scale(ex, key, k):
    g = _block_geom(ex)
    cx, cy = (g["x0"] + g["x1"]) / 2, (g["y0"] + g["y1"]) / 2
    fn = lambda p: (cx + (p[0] - cx) * k, cy + (p[1] - cy) * k)
    fr = dict(ex.frame)
    fr["clip_display"] = [cx + (g["x0"] - cx) * k, cy + (g["y0"] - cy) * k,
                          cx + (g["x1"] - cx) * k, cy + (g["y1"] - cy) * k]
    seed = _seed_for("B2_scale", key, str(k))
    segs = _affine_segments(ex.segments, fn)
    for t in segs:                      # a similarity transform scales stroke width too,
        if t.get("w"):                  # otherwise the rendered picture is not invariant
            t["w"] = round(float(t["w"]) * k, 4)
    ex2 = _clone(ex, segments=segs,
                 texts=_affine_texts(ex.texts, fn, scale=k), frame=fr,
                 prov={"cf": "B2_scale", "cf_seed": seed})
    man = {"cf_class": "B", "cf_id": "B2_scale", "seed": seed, "params": {"k": k},
           "touched_objects": [], "changed_primitives": {"n_before": len(ex.segments),
                                                          "n_after": len(ex.segments),
                                                          "segments_touched": len(ex.segments)},
           "delta": {"scale": k},
           "expected_verdict": "NO_GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "BLOCK_TRANSFORMED", "kind": "scale", "k": k}],
           "invariants": {"picture_after_compensation": True, "geometry_exact": False, "text": True},
           "compensation": {"kind": "scale", "k": k, "cx": cx, "cy": cy}}
    return ex2, man


def _reextract(ex, coords_px=None, pdf=None, page_index=None, page_px=None):
    p = ex.provenance
    return F.extract_block(pdf or p["pdf"],
                           p["page_index"] if page_index is None else page_index,
                           coords_px or p["coords_px"],
                           (page_px or p["page_px"])[0], (page_px or p["page_px"])[1])


def _apply_B_cropjitter(ex, key, frac):
    p = ex.provenance
    x1, y1, x2, y2 = p["coords_px"]
    w, h = x2 - x1, y2 - y1
    seed = _seed_for("B3_crop_jitter", key, str(frac))
    rng = random.Random(seed)
    dx = rng.choice([-1, 1]) * frac * w
    dy = rng.choice([-1, 1]) * frac * h
    new = [x1 + dx, y1 + dy, x2 + dx, y2 + dy]
    ex2 = _reextract(ex, coords_px=new)
    ex2.provenance.update({"cf": "B3_crop_jitter", "cf_seed": seed})
    man = {"cf_class": "B", "cf_id": "B3_crop_jitter", "seed": seed,
           "params": {"frac_of_side": frac},
           "touched_objects": [],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(ex2.segments),
                                  "segments_touched": 0},
           "delta": {"dx_px": round(dx, 3), "dy_px": round(dy, 3)},
           "expected_verdict": "NO_GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "BLOCK_TRANSFORMED", "kind": "crop_jitter",
                                "dx_px": round(dx, 3), "dy_px": round(dy, 3)}],
           "invariants": {"picture_on_intersection": True, "geometry_exact": False, "text": True},
           "compensation": {"kind": "frame_intersection"}}
    return ex2, man


def _apply_B_aspect(ex, key, frac):
    p = ex.provenance
    x1, y1, x2, y2 = p["coords_px"]
    w, h = x2 - x1, y2 - y1
    new = [x1, y1, x2 + w * frac, y2 - h * frac * 0.5]
    seed = _seed_for("B4_aspect", key, str(frac))
    ex2 = _reextract(ex, coords_px=new)
    ex2.provenance.update({"cf": "B4_aspect", "cf_seed": seed})
    man = {"cf_class": "B", "cf_id": "B4_aspect", "seed": seed, "params": {"frac": frac},
           "touched_objects": [],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(ex2.segments),
                                  "segments_touched": 0},
           "delta": {"dw_px": round(w * frac, 3), "dh_px": round(-h * frac * 0.5, 3)},
           "expected_verdict": "NO_GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "BLOCK_TRANSFORMED", "kind": "aspect"}],
           "invariants": {"picture_on_intersection": True, "geometry_exact": False, "text": True},
           "compensation": {"kind": "frame_intersection"}}
    return ex2, man


# --- page-level rewrites (real PDF): A7 and B5 and D9 ------------------------------

def _single_page_pdf(pdf_path, page_index, out_path) -> str:
    src = fitz.open(pdf_path)
    dst = fitz.open()
    dst.insert_pdf(src, from_page=page_index, to_page=page_index, annots=False)
    dst.save(str(out_path))
    dst.close()
    return str(out_path)


def _run(cmd) -> tuple[int, str]:
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    return r.returncode, (r.stderr or "")[-400:]


def page_rewrite(ex, mode: str, workdir: Path) -> dict:
    """Rewrite the carrier's PAGE with a real tool; returns paths + page geometry."""
    workdir.mkdir(parents=True, exist_ok=True)
    p = ex.provenance
    one = workdir / "page.pdf"
    _single_page_pdf(p["pdf"], p["page_index"], one)
    out = workdir / f"page_{mode}.pdf"
    if mode == "gs":
        rc, err = _run(["gs", "-q", "-dNOPAUSE", "-dBATCH", "-sDEVICE=pdfwrite",
                        "-dPDFSETTINGS=/prepress", f"-sOutputFile={out}", str(one)])
    elif mode == "cairo":
        rc, err = _run(["pdftocairo", "-pdf", str(one), str(out)])
    elif mode == "gs_nofonts":
        # -dNoOutputFonts converts every glyph to vector outlines: the real D9 case
        rc, err = _run(["gs", "-q", "-dNOPAUSE", "-dBATCH", "-dNoOutputFonts", "-sDEVICE=pdfwrite",
                        "-dPDFSETTINGS=/prepress", f"-sOutputFile={out}", str(one)])
    elif mode == "rotate90" or mode == "rotate270":
        d = fitz.open(str(one))
        pg = d[0]
        add = 90 if mode == "rotate90" else 270
        pg.set_rotation((pg.rotation + add) % 360)
        d.save(str(out))
        d.close()
        rc, err = 0, ""
    else:
        raise ValueError(mode)
    if rc != 0 or not out.exists():
        raise CFNotApplicable(f"page rewrite {mode} failed rc={rc} {err}")
    a = fitz.open(str(one))[0]
    b = fitz.open(str(out))[0]
    return {"src_pdf": str(one), "out_pdf": str(out), "mode": mode,
            "src_rect": [a.rect.x0, a.rect.y0, a.rect.x1, a.rect.y1],
            "out_rect": [b.rect.x0, b.rect.y0, b.rect.x1, b.rect.y1],
            "src_rot": a.rotation, "out_rot": b.rotation,
            "rect_equal": (abs(a.rect.width - b.rect.width) < 0.5 and
                           abs(a.rect.height - b.rect.height) < 0.5)}


def _apply_A7(ex, key, tool, workdir):
    info = page_rewrite(ex, tool, workdir)
    p = ex.provenance
    ex2 = F.extract_block(info["out_pdf"], 0, p["coords_px"], p["page_px"][0], p["page_px"][1])
    seed = _seed_for("A7_reexport", key, tool)
    ex2.provenance.update({"cf": f"A7_reexport_{tool}", "cf_seed": seed})
    man = {"cf_class": "A", "cf_id": f"A7_reexport_{tool}", "seed": seed,
           "params": {"tool": tool, "page_rect_preserved": info["rect_equal"],
                      "rotation_before": info["src_rot"], "rotation_after": info["out_rot"]},
           "touched_objects": [],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(ex2.segments),
                                  "segments_touched": None},
           "delta": None,
           "expected_verdict": "NO_GRAPHIC_CHANGE",
           "expected_ledger": [],
           "invariants": {"picture": True, "geometry_exact": False, "text": None},
           "page_rewrite": info}
    return ex2, man


def _apply_B5(ex, key, add):
    workdir = SCRATCH / f"{key}_B5_{add}"
    info = page_rewrite(ex, f"rotate{add}", workdir)
    p = ex.provenance
    # map the block's px box into the rotated page's px system
    src = fitz.open(info["src_pdf"])[0]
    dst = fitz.open(info["out_pdf"])[0]
    W, H = float(p["page_px"][0]), float(p["page_px"][1])
    sx, sy = src.rect.width / W, src.rect.height / H
    x1, y1, x2, y2 = p["coords_px"]
    disp = fitz.Rect(x1 * sx, y1 * sy, x2 * sx, y2 * sy)
    M = src.derotation_matrix * dst.rotation_matrix        # old display -> new display
    r = fitz.Rect(disp) * M
    r.normalize()
    W2, H2 = H, W
    sx2, sy2 = dst.rect.width / W2, dst.rect.height / H2
    new_px = [r.x0 / sx2, r.y0 / sy2, r.x1 / sx2, r.y1 / sy2]
    ex2 = F.extract_block(info["out_pdf"], 0, new_px, W2, H2)
    seed = _seed_for("B5_rotate_page", key, str(add))
    ex2.provenance.update({"cf": f"B5_rotate_page_{add}", "cf_seed": seed})
    man = {"cf_class": "B", "cf_id": f"B5_rotate_page_{add}", "seed": seed,
           "params": {"add_deg": add, "rotation_before": info["src_rot"],
                      "rotation_after": info["out_rot"], "coords_px_after": [round(v, 2) for v in new_px],
                      "page_px_after": [W2, H2]},
           "touched_objects": [],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(ex2.segments),
                                  "segments_touched": 0},
           "delta": {"rotate_deg": add},
           "expected_verdict": "NO_GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "BLOCK_TRANSFORMED", "kind": "page_rotate", "deg": add}],
           "invariants": {"picture_after_compensation": True, "geometry_exact": False, "text": True},
           "compensation": {"kind": "rotate", "deg": add,
                            "matrix": [M.a, M.b, M.c, M.d, M.e, M.f]},
           "page_rewrite": info}
    return ex2, man


# ------------------------------------------------------------------ class C helpers

def _pick_objects(objects, geom, *, rng, want_bucket=None, min_seg=2, max_seg=4000,
                  classes=None, n=1, exclude=()):
    cands = []
    for k, o in enumerate(objects.objects):
        if o["object_id"] in exclude:
            continue
        if o["n_seg"] < min_seg or o["n_seg"] > max_seg:
            continue
        if classes and o["cls"] not in classes:
            continue
        bb = o["bbox"]
        af = max(bb[2] - bb[0], 1e-9) * max(bb[3] - bb[1], 1e-9) / geom["area"]
        if want_bucket and size_bucket(af) != want_bucket:
            continue
        if o["diag"] <= 0:
            continue
        cands.append((k, af))
    if not cands:
        raise CFNotApplicable(f"no object matches bucket={want_bucket} classes={classes}")
    cands.sort(key=lambda t: (objects.objects[t[0]]["cy"], objects.objects[t[0]]["cx"]))
    rng.shuffle(cands)
    return cands[:n]


def _segments_of(objects, ex, idx) -> list[int]:
    return list(objects.objects[idx]["segments"])


def _ink_grid(ex, geom, nx=48, ny=48):
    grid = [[0] * nx for _ in range(ny)]
    for s in ex.segments:
        for p in (s["p0"], s["p1"], ((s["p0"][0] + s["p1"][0]) / 2, (s["p0"][1] + s["p1"][1]) / 2)):
            gx = int((p[0] - geom["x0"]) / geom["w"] * nx)
            gy = int((p[1] - geom["y0"]) / geom["h"] * ny)
            if 0 <= gx < nx and 0 <= gy < ny:
                grid[gy][gx] += 1
    for t in ex.texts:
        bb = t["bbox"]
        for gx in range(max(0, int((bb[0] - geom["x0"]) / geom["w"] * nx)),
                        min(nx, int((bb[2] - geom["x0"]) / geom["w"] * nx) + 1)):
            for gy in range(max(0, int((bb[1] - geom["y0"]) / geom["h"] * ny)),
                            min(ny, int((bb[3] - geom["y0"]) / geom["h"] * ny) + 1)):
                grid[gy][gx] += 1
    return grid, nx, ny


def _free_spot(ex, geom, obj_bbox, rng):
    """Find an empty rectangle inside the block able to host the object's bbox."""
    grid, nx, ny = _ink_grid(ex, geom)
    ow = obj_bbox[2] - obj_bbox[0]
    oh = obj_bbox[3] - obj_bbox[1]
    cw = max(1, int(math.ceil(ow / geom["w"] * nx)) + 1)
    ch = max(1, int(math.ceil(oh / geom["h"] * ny)) + 1)
    spots = []
    for gy in range(0, ny - ch):
        for gx in range(0, nx - cw):
            s = 0
            for yy in range(gy, gy + ch):
                row = grid[yy]
                for xx in range(gx, gx + cw):
                    s += row[xx]
                    if s:
                        break
                if s:
                    break
            if s == 0:
                spots.append((gx, gy))
    if not spots:
        raise CFNotApplicable("no free space to place a duplicate")
    gx, gy = spots[rng.randrange(len(spots))]
    return (geom["x0"] + (gx + 0.5) * geom["w"] / nx - obj_bbox[0],
            geom["y0"] + (gy + 0.5) * geom["h"] / ny - obj_bbox[1])


def _apply_C1(ex, objects, key, bucket):
    geom = _block_geom(ex)
    rng = random.Random(_seed_for("C1_remove_object", key, bucket or ""))
    (idx, af), = _pick_objects(objects, geom, rng=rng, want_bucket=bucket, n=1)
    obj = objects.objects[idx]
    drop = set(_segments_of(objects, ex, idx))
    segs = [dict(s) for k, s in enumerate(ex.segments) if k not in drop]
    for s in segs:
        s["src"] = [s["i"]]
    _renumber(segs)
    ex2 = _clone(ex, segments=segs, prov={"cf": "C1_remove_object"})
    rec = _obj_record(obj, geom)
    man = {"cf_class": "C", "cf_id": "C1_remove_object", "seed": _seed_for("C1_remove_object", key, bucket or ""),
           "params": {"size_bucket": bucket or size_bucket(af)},
           "touched_objects": [rec],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                  "removed_segment_ix": sorted(drop),
                                  "segments_touched": len(drop)},
           "delta": None,
           "expected_verdict": "GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "REMOVED_OBJECT", "object_id": obj["object_id"],
                                "bbox_pt": rec["bbox_pt"]}],
           "change_bbox_pt": rec["bbox_pt"],
           "invariants": {"picture": False, "text": True, "local": True}}
    return ex2, man


def _apply_C2(ex, objects, key, bucket):
    geom = _block_geom(ex)
    seed = _seed_for("C2_add_object", key, bucket or "")
    rng = random.Random(seed)
    (idx, af), = _pick_objects(objects, geom, rng=rng, want_bucket=bucket, n=1, max_seg=800)
    obj = objects.objects[idx]
    dx, dy = _free_spot(ex, geom, obj["bbox"], rng)
    src_ix = _segments_of(objects, ex, idx)
    segs = [dict(s) for s in ex.segments]
    for s in segs:
        s["src"] = [s["i"]]
    added = []
    for k in src_ix:
        s = ex.segments[k]
        t = _mk_seg((s["p0"][0] + dx, s["p0"][1] + dy), (s["p1"][0] + dx, s["p1"][1] + dy),
                    _seg_style(s), src=[k], op=s.get("op", "l"), tag="C2_added")
        segs.append(t)
        added.append(t)
    _renumber(segs)
    nb = [obj["bbox"][0] + dx, obj["bbox"][1] + dy, obj["bbox"][2] + dx, obj["bbox"][3] + dy]
    rec = _obj_record(obj, geom)
    ex2 = _clone(ex, segments=segs, prov={"cf": "C2_add_object"})
    man = {"cf_class": "C", "cf_id": "C2_add_object", "seed": seed,
           "params": {"size_bucket": bucket or size_bucket(af),
                      "placed_at_pt": [round(dx, 3), round(dy, 3)]},
           "touched_objects": [rec],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                  "added_segment_ix": list(range(len(ex.segments), len(segs))),
                                  "segments_touched": len(added)},
           "delta": {"dx_pt": round(dx, 3), "dy_pt": round(dy, 3)},
           "expected_verdict": "GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "ADDED_OBJECT", "copy_of": obj["object_id"],
                                "bbox_pt": [round(v, 3) for v in nb]}],
           "change_bbox_pt": [round(v, 3) for v in nb],
           "invariants": {"picture": False, "text": True, "local": True}}
    return ex2, man


def _apply_C3(ex, objects, key, bucket, frac):
    geom = _block_geom(ex)
    seed = _seed_for("C3_move_object", key, f"{bucket}")   # same object for every delta
    rng = random.Random(seed)
    (idx, af), = _pick_objects(objects, geom, rng=rng, want_bucket=bucket, n=1)
    obj = objects.objects[idx]
    d = frac * geom["diag"]
    ang0 = random.Random(seed + 1).uniform(0, 2 * math.pi)
    bb = obj["bbox"]
    dx = dy = None
    for k in range(8):                     # the same object must survive the whole delta curve
        ang = ang0 + k * math.pi / 4
        cx_, cy_ = d * math.cos(ang), d * math.sin(ang)
        if (geom["x0"] <= bb[0] + cx_ and bb[2] + cx_ <= geom["x1"] and
                geom["y0"] <= bb[1] + cy_ and bb[3] + cy_ <= geom["y1"]):
            dx, dy = cx_, cy_
            break
    if dx is None:
        raise CFNotApplicable("moved object would leave the block frame in any of 8 directions")
    move = set(_segments_of(objects, ex, idx))
    segs = []
    for k, s in enumerate(ex.segments):
        t = dict(s)
        t["src"] = [k]
        if k in move:
            t["p0"] = (s["p0"][0] + dx, s["p0"][1] + dy)
            t["p1"] = (s["p1"][0] + dx, s["p1"][1] + dy)
            t["cf_tag"] = "C3_moved"
        segs.append(t)
    _renumber(segs)
    ex2 = _clone(ex, segments=segs, prov={"cf": "C3_move_object"})
    rec = _obj_record(obj, geom)
    nb = [bb[0] + dx, bb[1] + dy, bb[2] + dx, bb[3] + dy]
    union = [min(bb[0], nb[0]), min(bb[1], nb[1]), max(bb[2], nb[2]), max(bb[3], nb[3])]
    man = {"cf_class": "C", "cf_id": "C3_move_object", "seed": seed,
           "params": {"size_bucket": bucket or size_bucket(af), "frac_of_diag": frac},
           "touched_objects": [rec],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                  "moved_segment_ix": sorted(move),
                                  "segments_touched": len(move)},
           "delta": {"dx_pt": round(dx, 4), "dy_pt": round(dy, 4), "d_pt": round(d, 4),
                     "d_over_S": round(d / max(objects.S, 1e-9), 3)},
           "expected_verdict": "GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "MOVED_OBJECT", "object_id": obj["object_id"],
                                "bbox_pt": rec["bbox_pt"], "bbox_after_pt": [round(v, 3) for v in nb],
                                "dx_pt": round(dx, 4), "dy_pt": round(dy, 4)}],
           "change_bbox_pt": [round(v, 3) for v in union],
           "invariants": {"picture": False, "text": True, "local": True}}
    return ex2, man


def _swap_pair(ex, objects, geom, rng, *, like: bool):
    objs = objects.objects
    cands = []
    for i in range(len(objs)):
        a = objs[i]
        if a["n_seg"] < 3 or a["n_seg"] > 400 or a["diag"] <= 0:
            continue
        cands.append(i)
    rng.shuffle(cands)
    for ii in range(len(cands)):
        for jj in range(ii + 1, len(cands)):
            a, b = objs[cands[ii]], objs[cands[jj]]
            if like and (a["n_seg"] != b["n_seg"] or a["n_prim"] != b["n_prim"]):
                continue
            if (not like) and abs(a["n_seg"] - b["n_seg"]) < 3:
                continue
            dd = O.descriptor_distance(a["desc"], b["desc"])
            if like and dd < 0.05:            # visually identical -> swap invisible
                continue
            gap = math.hypot(a["cx"] - b["cx"], a["cy"] - b["cy"])
            if gap < max(a["diag"], b["diag"]):
                continue
            return cands[ii], cands[jj], dd
    raise CFNotApplicable("no swappable object pair")


def _apply_swap(ex, objects, key, cf_id, like):
    geom = _block_geom(ex)
    seed = _seed_for(cf_id, key)
    rng = random.Random(seed)
    i, j, dd = _swap_pair(ex, objects, geom, rng, like=like)
    a, b = objects.objects[i], objects.objects[j]
    dax = b["cx"] - a["cx"]; day = b["cy"] - a["cy"]
    sa = set(_segments_of(objects, ex, i)); sb = set(_segments_of(objects, ex, j))
    segs = []
    for k, s in enumerate(ex.segments):
        t = dict(s); t["src"] = [k]
        if k in sa:
            t["p0"] = (s["p0"][0] + dax, s["p0"][1] + day)
            t["p1"] = (s["p1"][0] + dax, s["p1"][1] + day)
            t["cf_tag"] = "swap_a"
        elif k in sb:
            t["p0"] = (s["p0"][0] - dax, s["p0"][1] - day)
            t["p1"] = (s["p1"][0] - dax, s["p1"][1] - day)
            t["cf_tag"] = "swap_b"
        segs.append(t)
    _renumber(segs)
    ex2 = _clone(ex, segments=segs, prov={"cf": cf_id})
    ra, rb = _obj_record(a, geom), _obj_record(b, geom)
    aa, bb_ = a["bbox"], b["bbox"]
    a_after = [aa[0] + dax, aa[1] + day, aa[2] + dax, aa[3] + day]
    b_after = [bb_[0] - dax, bb_[1] - day, bb_[2] - dax, bb_[3] - day]
    reg_a = [min(aa[0], a_after[0], bb_[0], b_after[0]), min(aa[1], a_after[1], bb_[1], b_after[1]),
             max(aa[2], a_after[2], bb_[2], b_after[2]), max(aa[3], a_after[3], bb_[3], b_after[3])]
    regions = [[round(v, 3) for v in [min(aa[0], b_after[0]), min(aa[1], b_after[1]),
                                      max(aa[2], b_after[2]), max(aa[3], b_after[3])]],
               [round(v, 3) for v in [min(bb_[0], a_after[0]), min(bb_[1], a_after[1]),
                                      max(bb_[2], a_after[2]), max(bb_[3], a_after[3])]]]
    union = reg_a
    man = {"cf_class": "C", "cf_id": cf_id, "seed": seed,
           "params": {"like": like, "descriptor_distance": round(dd, 5),
                      "n_seg_a": a["n_seg"], "n_seg_b": b["n_seg"]},
           "touched_objects": [ra, rb],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                  "moved_segment_ix": sorted(sa | sb),
                                  "segments_touched": len(sa) + len(sb)},
           "delta": {"dx_pt": round(dax, 4), "dy_pt": round(day, 4)},
           "expected_verdict": "GRAPHIC_CHANGE",
           "expected_ledger": [
               {"type": "MOVED_OBJECT", "object_id": a["object_id"], "bbox_pt": ra["bbox_pt"]},
               {"type": "MOVED_OBJECT", "object_id": b["object_id"], "bbox_pt": rb["bbox_pt"]}],
           "counters_invariant": True,
           "change_bbox_pt": [round(v, 3) for v in union],
           "change_regions_pt": regions,
           "invariants": {"picture": False, "text": True, "local": True,
                          "object_count": True, "segment_count": True}}
    return ex2, man


def _apply_C6(ex, objects, key, bucket, k=1.35):
    geom = _block_geom(ex)
    seed = _seed_for("C6_reshape_object", key, bucket or "")
    rng = random.Random(seed)
    (idx, af), = _pick_objects(objects, geom, rng=rng, want_bucket=bucket, n=1, min_seg=3)
    obj = objects.objects[idx]
    ix = set(_segments_of(objects, ex, idx))
    cx, cy = obj["cx"], obj["cy"]
    segs = []
    for m, s in enumerate(ex.segments):
        t = dict(s); t["src"] = [m]
        if m in ix:                                  # anisotropic stretch: same count, new shape
            t["p0"] = (cx + (s["p0"][0] - cx) * k, cy + (s["p0"][1] - cy) / k)
            t["p1"] = (cx + (s["p1"][0] - cx) * k, cy + (s["p1"][1] - cy) / k)
            t["len"] = math.hypot(t["p1"][0] - t["p0"][0], t["p1"][1] - t["p0"][1])
            t["cf_tag"] = "C6_reshaped"
        segs.append(t)
    _renumber(segs)
    ex2 = _clone(ex, segments=segs, prov={"cf": "C6_reshape_object"})
    rec = _obj_record(obj, geom)
    bb = obj["bbox"]
    nb = [cx + (bb[0] - cx) * k, cy + (bb[1] - cy) / k, cx + (bb[2] - cx) * k, cy + (bb[3] - cy) / k]
    union = [min(bb[0], nb[0]), min(bb[1], nb[1]), max(bb[2], nb[2]), max(bb[3], nb[3])]
    man = {"cf_class": "C", "cf_id": "C6_reshape_object", "seed": seed,
           "params": {"size_bucket": bucket or size_bucket(af), "kx": k, "ky": round(1 / k, 4)},
           "touched_objects": [rec],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                  "moved_segment_ix": sorted(ix), "segments_touched": len(ix)},
           "delta": {"scale_x": k, "scale_y": round(1 / k, 4)},
           "expected_verdict": "GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "RESHAPED_OBJECT", "object_id": obj["object_id"],
                                "bbox_pt": rec["bbox_pt"]}],
           "counters_invariant": True,
           "change_bbox_pt": [round(v, 3) for v in union],
           "invariants": {"picture": False, "text": True, "local": True, "segment_count": True}}
    return ex2, man


def _node_key(p, tol):
    return (round(p[0] / tol), round(p[1] / tol))


def _find_bridge(ex, ix, tol):
    """Segment whose removal disconnects the object (classic bridge search)."""
    nodes: dict[tuple, int] = {}
    edges = []
    for k in ix:
        s = ex.segments[k]
        a = _node_key(s["p0"], tol); b = _node_key(s["p1"], tol)
        for q in (a, b):
            nodes.setdefault(q, len(nodes))
        edges.append((nodes[a], nodes[b], k))
    n = len(nodes)
    adj: dict[int, list] = {i: [] for i in range(n)}
    for ei, (u, v, k) in enumerate(edges):
        adj[u].append((v, ei)); adj[v].append((u, ei))
    disc = [-1] * n; low = [0] * n; bridges = []
    timer = [0]
    import sys as _s
    _s.setrecursionlimit(100000)

    def dfs(u, pe):
        disc[u] = low[u] = timer[0]; timer[0] += 1
        for v, ei in adj[u]:
            if ei == pe:
                continue
            if disc[v] == -1:
                dfs(v, ei)
                low[u] = min(low[u], low[v])
                if low[v] > disc[u]:
                    bridges.append(ei)
            else:
                low[u] = min(low[u], disc[v])
    for u in range(n):
        if disc[u] == -1:
            dfs(u, -1)
    # a bridge whose both sides carry >= 2 segments is a real split
    good = []
    for ei in bridges:
        u, v, k = edges[ei]
        if len(adj[u]) >= 2 and len(adj[v]) >= 2:
            good.append(k)
    return good


def _apply_C7(ex, objects, key):
    geom = _block_geom(ex)
    seed = _seed_for("C7_split_object", key)
    rng = random.Random(seed)
    S = objects.S
    tol = max(0.05 * S, 0.02)
    cands = _pick_objects(objects, geom, rng=rng, min_seg=6, max_seg=200, n=40)
    for idx, af in cands:
        ix = _segments_of(objects, ex, idx)
        good = _find_bridge(ex, ix, tol)
        if not good:
            continue
        k = good[rng.randrange(len(good))]
        obj = objects.objects[idx]
        segs = [dict(s) for m, s in enumerate(ex.segments) if m != k]
        for s in segs:
            s["src"] = [s["i"]]
        _renumber(segs)
        ex2 = _clone(ex, segments=segs, prov={"cf": "C7_split_object"})
        rec = _obj_record(obj, geom)
        cut = ex.segments[k]
        man = {"cf_class": "C", "cf_id": "C7_split_object", "seed": seed,
               "params": {"size_bucket": size_bucket(af), "cut_len_pt": round(cut["len"], 3)},
               "touched_objects": [rec],
               "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                      "removed_segment_ix": [k], "segments_touched": 1},
               "delta": None,
               "expected_verdict": "GRAPHIC_CHANGE",
               "expected_ledger": [{"type": "SPLIT_OBJECT", "object_id": obj["object_id"],
                                    "bbox_pt": rec["bbox_pt"]}],
               "change_bbox_pt": [round(v, 3) for v in _bbox_of_segs([cut])],
               "invariants": {"picture": False, "text": True, "local": True}}
        return ex2, man
    raise CFNotApplicable("no object with a removable bridge")


def _nearest_endpoints(a_segs, b_segs):
    best = None
    for s in a_segs:
        for p in (s["p0"], s["p1"]):
            for t in b_segs:
                for q in (t["p0"], t["p1"]):
                    d = math.hypot(p[0] - q[0], p[1] - q[1])
                    if best is None or d < best[0]:
                        best = (d, p, q, s)
    return best


def _apply_C8(ex, objects, key):
    geom = _block_geom(ex)
    seed = _seed_for("C8_merge_objects", key)
    rng = random.Random(seed)
    S = objects.S
    objs = objects.objects
    idxs = [k for k, o in enumerate(objs) if 2 <= o["n_seg"] <= 200]
    rng.shuffle(idxs)
    for ii in range(min(len(idxs), 60)):
        for jj in range(ii + 1, min(len(idxs), 60)):
            a, b = objs[idxs[ii]], objs[idxs[jj]]
            gap = O._bbox_gap(a["bbox"], b["bbox"])
            if not (1.0 * S <= gap <= 8.0 * S):
                continue
            A = [ex.segments[m] for m in a["segments"][:200]]
            B = [ex.segments[m] for m in b["segments"][:200]]
            d, p, q, s = _nearest_endpoints(A, B)
            if d < 0.5 * S:
                continue
            segs = [dict(x) for x in ex.segments]
            for x in segs:
                x["src"] = [x["i"]]
            link = _mk_seg(p, q, _seg_style(s), src=[], tag="C8_link")
            segs.append(link)
            _renumber(segs)
            ex2 = _clone(ex, segments=segs, prov={"cf": "C8_merge_objects"})
            ra, rb = _obj_record(a, geom), _obj_record(b, geom)
            man = {"cf_class": "C", "cf_id": "C8_merge_objects", "seed": seed,
                   "params": {"gap_pt": round(gap, 3), "link_len_pt": round(d, 3),
                              "link_over_S": round(d / max(S, 1e-9), 3)},
                   "touched_objects": [ra, rb],
                   "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                          "added_segment_ix": [len(segs) - 1], "segments_touched": 1},
                   "delta": {"link_len_pt": round(d, 3)},
                   "expected_verdict": "GRAPHIC_CHANGE",
                   "expected_ledger": [{"type": "MERGED_OBJECTS",
                                        "object_ids": [a["object_id"], b["object_id"]],
                                        "bbox_pt": [round(v, 3) for v in _bbox_of_segs([link])]}],
                   "change_bbox_pt": [round(v, 3) for v in _bbox_of_segs([link])],
                   "invariants": {"picture": False, "text": True, "local": True}}
            return ex2, man
    raise CFNotApplicable("no object pair with a 1..8*S gap")


def _apply_C9(ex, objects, key):
    geom = _block_geom(ex)
    seed = _seed_for("C9_add_branch", key)
    rng = random.Random(seed)
    S = objects.S
    lin = [k for k, o in enumerate(objects.objects) if o["cls"] == "linear" and o["n_seg"] >= 1]
    if not lin:
        raise CFNotApplicable("no linear object to branch from")
    rng.shuffle(lin)
    for idx in lin[:40]:
        o = objects.objects[idx]
        segs_ix = o["segments"]
        longest = max((ex.segments[m] for m in segs_ix), key=lambda s: s["len"])
        if longest["len"] < 6 * S:
            continue
        s = longest
        ux = (s["p1"][0] - s["p0"][0]) / s["len"]
        uy = (s["p1"][1] - s["p0"][1]) / s["len"]
        base = (s["p0"][0] + ux * s["len"] * 0.4, s["p0"][1] + uy * s["len"] * 0.4)
        L = 6 * S
        for sgn in (1, -1):
            tip = (base[0] - uy * L * sgn, base[1] + ux * L * sgn)
            if not (geom["x0"] <= tip[0] <= geom["x1"] and geom["y0"] <= tip[1] <= geom["y1"]):
                continue
            segs = [dict(x) for x in ex.segments]
            for x in segs:
                x["src"] = [x["i"]]
            br = _mk_seg(base, tip, _seg_style(s), src=[], tag="C9_branch")
            segs.append(br)
            _renumber(segs)
            ex2 = _clone(ex, segments=segs, prov={"cf": "C9_add_branch"})
            rec = _obj_record(o, geom)
            man = {"cf_class": "C", "cf_id": "C9_add_branch", "seed": seed,
                   "params": {"branch_len_pt": round(L, 3), "branch_over_S": 6.0},
                   "touched_objects": [rec],
                   "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                          "added_segment_ix": [len(segs) - 1], "segments_touched": 1},
                   "delta": {"branch_len_pt": round(L, 3)},
                   "expected_verdict": "GRAPHIC_CHANGE",
                   "expected_ledger": [{"type": "ADDED_BRANCH", "object_id": o["object_id"],
                                        "bbox_pt": [round(v, 3) for v in _bbox_of_segs([br])]}],
                   "change_bbox_pt": [round(v, 3) for v in _bbox_of_segs([br])],
                   "invariants": {"picture": False, "text": True, "local": True}}
            return ex2, man
    raise CFNotApplicable("no long enough linear run for a branch")


def _apply_C10(ex, objects, key):
    """Close a gap ('opening') between two collinear runs drawn in the same style.

    An opening in a wall usually SPLITS the wall into two objects, so the search is over
    all long segments of the block, not inside one object; the ledger names the objects
    that own the two sides.
    """
    geom = _block_geom(ex)
    seed = _seed_for("C10_remove_opening", key)
    rng = random.Random(seed)
    S = objects.S
    seg2obj: dict[int, int] = {}
    for oi, o in enumerate(objects.objects):
        for m in o["segments"]:
            seg2obj[m] = oi
    long_ix = [i for i, s in enumerate(ex.segments) if s["len"] >= 3 * S]
    rng.shuffle(long_ix)
    long_ix = long_ix[:1200]
    # bucket by direction and offset so the scan stays near-linear
    buckets: dict[tuple, list[int]] = {}
    for i in long_ix:
        s = ex.segments[i]
        ux = (s["p1"][0] - s["p0"][0]) / max(s["len"], 1e-9)
        uy = (s["p1"][1] - s["p0"][1]) / max(s["len"], 1e-9)
        if ux < 0 or (abs(ux) < 1e-9 and uy < 0):
            ux, uy = -ux, -uy
        ang = math.degrees(math.atan2(uy, ux))
        rho = -uy * s["p0"][0] + ux * s["p0"][1]
        buckets.setdefault((round(ang / 2.0), round(rho / max(0.4 * S, 0.05))), []).append(i)
    keys = sorted(buckets, key=lambda k: -len(buckets[k]))
    for kb in keys:
        ix = buckets[kb]
        if len(ix) < 2:
            continue
        for a_i in ix:
            A = ex.segments[a_i]
            ax = (A["p1"][0] - A["p0"][0]) / max(A["len"], 1e-9)
            ay = (A["p1"][1] - A["p0"][1]) / max(A["len"], 1e-9)
            for b_i in ix:
                if b_i <= a_i:
                    continue
                B = ex.segments[b_i]
                if _seg_style(A) != _seg_style(B):
                    continue
                bx = (B["p1"][0] - B["p0"][0]) / max(B["len"], 1e-9)
                by = (B["p1"][1] - B["p0"][1]) / max(B["len"], 1e-9)
                if abs(ax * bx + ay * by) < 0.999:
                    continue
                best = None
                for p in (A["p0"], A["p1"]):
                    for q in (B["p0"], B["p1"]):
                        dd = math.hypot(p[0] - q[0], p[1] - q[1])
                        if best is None or dd < best[0]:
                            best = (dd, p, q)
                d, p, q = best
                if not (1.0 * S <= d <= 14 * S):
                    continue
                cross = abs((q[0] - p[0]) * ay - (q[1] - p[1]) * ax)
                if cross > 0.35 * S:
                    continue
                busy = False
                for s in ex.segments:
                    mx = (s["p0"][0] + s["p1"][0]) / 2
                    my = (s["p0"][1] + s["p1"][1]) / 2
                    t = ((mx - p[0]) * (q[0] - p[0]) + (my - p[1]) * (q[1] - p[1])) / max(d * d, 1e-9)
                    if 0.15 < t < 0.85:
                        px = p[0] + t * (q[0] - p[0]); py = p[1] + t * (q[1] - p[1])
                        if math.hypot(mx - px, my - py) < 0.3 * S:
                            busy = True
                            break
                if busy:
                    continue
                segs = [dict(x) for x in ex.segments]
                for x in segs:
                    x["src"] = [x["i"]]
                fill = _mk_seg(p, q, _seg_style(A), src=[], tag="C10_closed_opening")
                segs.append(fill)
                _renumber(segs)
                ex2 = _clone(ex, segments=segs, prov={"cf": "C10_remove_opening"})
                recs = []
                for si in (a_i, b_i):
                    oi = seg2obj.get(si)
                    if oi is not None:
                        recs.append(_obj_record(objects.objects[oi], geom))
                man = {"cf_class": "C", "cf_id": "C10_remove_opening", "seed": seed,
                       "params": {"opening_pt": round(d, 3), "opening_over_S": round(d / S, 2),
                                  "same_object": seg2obj.get(a_i) == seg2obj.get(b_i)},
                       "touched_objects": recs,
                       "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                              "added_segment_ix": [len(segs) - 1],
                                              "segments_touched": 1},
                       "delta": {"opening_len_pt": round(d, 3)},
                       "expected_verdict": "GRAPHIC_CHANGE",
                       "expected_ledger": [{"type": "REMOVED_OPENING",
                                            "object_ids": [r["object_id"] for r in recs],
                                            "bbox_pt": [round(v, 3) for v in _bbox_of_segs([fill])]}],
                       "change_bbox_pt": [round(v, 3) for v in _bbox_of_segs([fill])],
                       "invariants": {"picture": False, "text": True, "local": True}}
                return ex2, man
    raise CFNotApplicable("no collinear gap of 1..14*S between same-style long runs")


# ------------------------------------------------------------------ class D

_NUM = re.compile(r"\d+")
_LABEL = re.compile(r"([A-ZА-Я]{1,3})[- ]?(\d{1,3})")


def _bump_digits(s: str, add: int = 1) -> str:
    return _NUM.sub(lambda m: str(int(m.group()) + add), s)


def _pick_texts(ex, rng, k, pred=None):
    ix = [i for i, t in enumerate(ex.texts) if (pred(t) if pred else True)]
    if not ix:
        raise CFNotApplicable("no text line matches")
    rng.shuffle(ix)
    return ix[:k]


def _text_cf(ex, key, cf_id, mutate, pred=None, k=3, params=None, move=None,
             expected="NO_GRAPHIC_CHANGE"):
    seed = _seed_for(cf_id, key)
    rng = random.Random(seed)
    ix = _pick_texts(ex, rng, k, pred)
    texts = [dict(t) for t in ex.texts]
    touched = []
    for i in ix:
        before = texts[i]["text"]
        if mutate:
            texts[i]["text"] = mutate(before)
        if move:
            dx, dy = move
            bb = texts[i]["bbox"]
            texts[i]["bbox"] = [bb[0] + dx, bb[1] + dy, bb[2] + dx, bb[3] + dy]
            texts[i]["cx"] += dx
            texts[i]["cy"] += dy
        touched.append({"text_ix": i, "before": before, "after": texts[i]["text"],
                        "bbox_pt": [round(v, 3) for v in ex.texts[i]["bbox"]]})
    if mutate and all(t["before"] == t["after"] for t in touched):
        raise CFNotApplicable("mutation changed nothing")
    ex2 = _clone(ex, texts=texts, prov={"cf": cf_id})
    man = {"cf_class": "D", "cf_id": cf_id, "seed": seed, "params": params or {},
           "touched_objects": [], "touched_texts": touched,
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(ex.segments),
                                  "segments_touched": 0},
           "delta": {"dx_pt": move[0], "dy_pt": move[1]} if move else None,
           "expected_verdict": expected,
           "expected_ledger": [],
           "invariants": {"geometry_exact": True, "picture_geometry": True, "text": False}}
    return ex2, man


def _apply_D(ex, objects, cf_id, key, **params):
    S = float(ex.char_scale.get("S") or 1.0)
    if cf_id == "D1_text_edit":
        return _text_cf(ex, key, cf_id,
                        lambda s: (s[::-1] if len(s) > 3 else s + "X"),
                        pred=lambda t: len(t["text"]) >= 3, k=3)
    if cf_id == "D2_text_move":
        return _text_cf(ex, key, cf_id, None, k=3, move=(1.5 * S, 0.0),
                        params={"dx_over_S": 1.5})
    if cf_id == "D3_label_rename":
        return _text_cf(ex, key, cf_id,
                        lambda s: _LABEL.sub(lambda m: f"{m.group(1)}{int(m.group(2)) + 1}", s),
                        pred=lambda t: bool(_LABEL.search(t["text"])), k=10)
    if cf_id == "D4_table_values":
        return _text_cf(ex, key, cf_id, lambda s: _bump_digits(s, 7),
                        pred=lambda t: bool(_NUM.search(t["text"])), k=5,
                        params={"scope": "numeric cells"})
    if cf_id == "D5_table_row_text":
        seed = _seed_for(cf_id, key)
        rng = random.Random(seed)
        if not ex.texts:
            raise CFNotApplicable("no text")
        rows: dict[int, list[int]] = {}
        for i, t in enumerate(ex.texts):
            rows.setdefault(int(round(t["cy"] / max(0.8 * S, 0.5))), []).append(i)
        big = [r for r, v in rows.items() if len(v) >= 3]
        if not big:
            raise CFNotApplicable("no table row with >=3 cells")
        r = big[rng.randrange(len(big))]
        keep = set(rows[r])
        return _text_cf(ex, key, cf_id, lambda s: "ЗАМЕНА " + _bump_digits(s, 3),
                        pred=lambda t: id(t) in {id(ex.texts[i]) for i in keep},
                        k=len(keep), params={"row_cells": len(keep)})
    if cf_id in ("D6_dim_value_only", "D7_dim_geometry"):
        return _apply_dim(ex, objects, cf_id, key)
    if cf_id == "D8_font_swap":
        seed = _seed_for(cf_id, key)
        rng = random.Random(seed)
        texts = [dict(t) for t in ex.texts]
        if not texts:
            raise CFNotApplicable("no text")
        for t in texts:
            t["font"] = "GOSTtypeA-Italic" if t.get("font") != "GOSTtypeA-Italic" else "ISOCPEUR"
        ex2 = _clone(ex, texts=texts, prov={"cf": cf_id})
        man = {"cf_class": "D", "cf_id": cf_id, "seed": seed, "params": {"n_lines": len(texts)},
               "touched_objects": [], "touched_texts": [],
               "changed_primitives": {"n_before": len(ex.segments), "n_after": len(ex.segments),
                                      "segments_touched": 0},
               "delta": None, "expected_verdict": "NO_GRAPHIC_CHANGE", "expected_ledger": [],
               "invariants": {"geometry_exact": True, "picture_geometry": True, "text": True}}
        return ex2, man
    raise ValueError(cf_id)


def _apply_D9(ex, key):
    workdir = SCRATCH / f"{key}_D9"
    info = page_rewrite(ex, "gs_nofonts", workdir)
    p = ex.provenance
    ex2 = F.extract_block(info["out_pdf"], 0, p["coords_px"], p["page_px"][0], p["page_px"][1])
    seed = _seed_for("D9_text_to_curves", key)
    ex2.provenance.update({"cf": "D9_text_to_curves", "cf_seed": seed})
    man = {"cf_class": "D", "cf_id": "D9_text_to_curves", "seed": seed,
           "params": {"tool": "gs -dNoOutputFonts", "page_rect_preserved": info["rect_equal"]},
           "touched_objects": [], "touched_texts": [],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(ex2.segments),
                                  "segments_touched": None,
                                  "text_lines_before": len(ex.texts), "text_lines_after": len(ex2.texts)},
           "delta": None,
           "expected_verdict": "NO_GRAPHIC_CHANGE",
           "expected_ledger": [],
           "invariants": {"geometry_exact": False, "picture": True, "text": False},
           "page_rewrite": info}
    return ex2, man


def _dim_candidates(ex, S):
    """(text, segment) pairs that look like 'number over a dimension line'."""
    out = []
    for ti, t in enumerate(ex.texts):
        s = t["text"].strip()
        if not re.fullmatch(r"\d{2,5}", s):
            continue
        val = int(s)
        best = None
        for si, sg in enumerate(ex.segments):
            if sg["len"] < 3 * S:
                continue
            dx = sg["p1"][0] - sg["p0"][0]
            dy = sg["p1"][1] - sg["p0"][1]
            ux, uy = dx / sg["len"], dy / sg["len"]
            # text direction roughly along the segment
            d = t.get("dir") or [1, 0]
            if abs(ux * d[0] + uy * d[1]) < 0.9:
                continue
            mx = (sg["p0"][0] + sg["p1"][0]) / 2
            my = (sg["p0"][1] + sg["p1"][1]) / 2
            dist = math.hypot(mx - t["cx"], my - t["cy"])
            if dist > 4.0 * S:
                continue
            if best is None or dist < best[0]:
                best = (dist, si)
        if best:
            out.append({"text_ix": ti, "seg_ix": best[1], "value": val, "dist": best[0]})
    return out


def _apply_dim(ex, objects, cf_id, key):
    S = float(ex.char_scale.get("S") or 1.0)
    seed = _seed_for(cf_id, key)
    rng = random.Random(seed)
    cands = _dim_candidates(ex, S)
    if not cands:
        raise CFNotApplicable("no dimension-like (number over a line) candidate")
    c = cands[rng.randrange(len(cands))]
    ti, si, val = c["text_ix"], c["seg_ix"], c["value"]
    sg = ex.segments[si]
    if cf_id == "D6_dim_value_only":
        texts = [dict(t) for t in ex.texts]
        newval = val + max(1, int(val * 0.1))
        texts[ti]["text"] = str(newval)
        ex2 = _clone(ex, texts=texts, prov={"cf": cf_id})
        man = {"cf_class": "D", "cf_id": cf_id, "seed": seed,
               "params": {"value_before": val, "value_after": newval,
                          "dim_line_len_pt": round(sg["len"], 3)},
               "touched_objects": [], "touched_texts": [{"text_ix": ti, "before": str(val),
                                                          "after": str(newval),
                                                          "bbox_pt": [round(v, 3) for v in ex.texts[ti]["bbox"]]}],
               "changed_primitives": {"n_before": len(ex.segments), "n_after": len(ex.segments),
                                      "segments_touched": 0},
               "delta": None, "expected_verdict": "NO_GRAPHIC_CHANGE", "expected_ledger": [],
               "invariants": {"geometry_exact": True, "picture_geometry": True, "text": False}}
        return ex2, man
    # D7: rebuild the chain 1500 -> 300 + 1000 + 200 (geometry AND numbers change)
    parts = [0.2, 0.667, 0.133]
    vals = [max(1, int(round(val * f))) for f in parts]
    vals[-1] = max(1, val - vals[0] - vals[1])
    ux = (sg["p1"][0] - sg["p0"][0]) / max(sg["len"], 1e-9)
    uy = (sg["p1"][1] - sg["p0"][1]) / max(sg["len"], 1e-9)
    nx, ny = -uy, ux
    tick = 1.2 * S
    segs = [dict(s) for s in ex.segments]
    for s in segs:
        s["src"] = [s["i"]]
    added = []
    cuts = [parts[0], parts[0] + parts[1]]
    for f in cuts:
        px = sg["p0"][0] + ux * sg["len"] * f
        py = sg["p0"][1] + uy * sg["len"] * f
        a = (px - nx * tick / 2, py - ny * tick / 2)
        b = (px + nx * tick / 2, py + ny * tick / 2)
        t = _mk_seg(a, b, _seg_style(sg), src=[si], tag="D7_tick")
        segs.append(t)
        added.append(t)
    _renumber(segs)
    texts = [dict(t) for t in ex.texts]
    base = texts[ti]
    bb = base["bbox"]
    centers = [parts[0] / 2, parts[0] + parts[1] / 2, parts[0] + parts[1] + parts[2] / 2]
    new_texts = []
    for j, (v, f) in enumerate(zip(vals, centers)):
        u = dict(base)
        px = sg["p0"][0] + ux * sg["len"] * f
        py = sg["p0"][1] + uy * sg["len"] * f
        w = bb[2] - bb[0]; h = bb[3] - bb[1]
        off = (base["cx"] - (sg["p0"][0] + sg["p1"][0]) / 2, base["cy"] - (sg["p0"][1] + sg["p1"][1]) / 2)
        cx, cy = px + off[0], py + off[1]
        u["text"] = str(v)
        u["bbox"] = [cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2]
        u["cx"], u["cy"] = cx, cy
        new_texts.append(u)
    texts = [t for i, t in enumerate(texts) if i != ti] + new_texts
    ex2 = _clone(ex, segments=segs, texts=texts, prov={"cf": cf_id})
    man = {"cf_class": "D", "cf_id": "D7_dim_geometry", "seed": seed,
           "params": {"value_before": val, "values_after": vals,
                      "dim_line_len_pt": round(sg["len"], 3), "tick_len_pt": round(tick, 3)},
           "touched_objects": [],
           "touched_texts": [{"text_ix": ti, "before": str(val), "after": "+".join(map(str, vals)),
                              "bbox_pt": [round(v, 3) for v in bb]}],
           "changed_primitives": {"n_before": len(ex.segments), "n_after": len(segs),
                                  "added_segment_ix": [len(segs) - 2, len(segs) - 1],
                                  "segments_touched": 2},
           "delta": {"n_ticks_added": 2},
           "expected_verdict": "GRAPHIC_CHANGE",
           "expected_ledger": [{"type": "DIMENSION_CHAIN_CHANGED",
                                "bbox_pt": [round(v, 3) for v in _bbox_of_segs(added)]}],
           "change_bbox_pt": [round(v, 3) for v in _bbox_of_segs(added)],
           "invariants": {"geometry_exact": False, "picture": False, "text": False, "local": True}}
    return ex2, man


# ------------------------------------------------------------------ registry / dispatch

CF_SPECS: dict[str, dict] = {}
for _id in _A_VARIANTS:
    CF_SPECS[_id] = {"cls": "A", "needs_objects": False, "expected": "NO_GRAPHIC_CHANGE"}
CF_SPECS["A7_reexport_gs"] = {"cls": "A", "needs_objects": False, "expected": "NO_GRAPHIC_CHANGE",
                              "page_level": True}
CF_SPECS["A7_reexport_cairo"] = {"cls": "A", "needs_objects": False, "expected": "NO_GRAPHIC_CHANGE",
                                 "page_level": True}
for _id, _var in (("B1_translate", [0.005, 0.02, 0.10]), ("B2_scale", [0.95, 1.05, 1.2]),
                  ("B3_crop_jitter", [0.0025, 0.005, 0.02, 0.05, 0.10]),
                  ("B4_aspect", [0.05, 0.14])):
    CF_SPECS[_id] = {"cls": "B", "needs_objects": False, "expected": "NO_GRAPHIC_CHANGE",
                     "variants": _var}
CF_SPECS["B5_rotate_page"] = {"cls": "B", "needs_objects": False, "expected": "NO_GRAPHIC_CHANGE",
                              "variants": [90, 270], "page_level": True}
for _id in ("C1_remove_object", "C2_add_object", "C3_move_object", "C4_swap_objects",
            "C5_swap_unlike", "C6_reshape_object", "C7_split_object", "C8_merge_objects",
            "C9_add_branch", "C10_remove_opening"):
    CF_SPECS[_id] = {"cls": "C", "needs_objects": True, "expected": "GRAPHIC_CHANGE"}
for _id in ("D1_text_edit", "D2_text_move", "D3_label_rename", "D4_table_values",
            "D5_table_row_text", "D6_dim_value_only", "D8_font_swap", "D9_text_to_curves"):
    CF_SPECS[_id] = {"cls": "D", "needs_objects": False, "expected": "NO_GRAPHIC_CHANGE"}
CF_SPECS["D7_dim_geometry"] = {"cls": "D", "needs_objects": False, "expected": "GRAPHIC_CHANGE"}


def apply(extract, objects, cf_id: str, *, key: Optional[str] = None, **params):
    """Apply one counterfactual.  Returns (extract2, manifest)."""
    if cf_id not in CF_SPECS:
        raise ValueError(f"unknown cf_id {cf_id!r}")
    key = key or hashlib.sha1(
        f"{extract.provenance['pdf']}|{extract.provenance['page_index']}|"
        f"{extract.provenance['coords_px']}".encode()).hexdigest()[:12]
    spec = CF_SPECS[cf_id]
    if spec["needs_objects"] and objects is None:
        raise ValueError(f"{cf_id} needs the object layer")
    if cf_id in _A_VARIANTS:
        ex2, man = _apply_A(extract, objects, cf_id, key, **params)
    elif cf_id.startswith("A7_reexport"):
        tool = cf_id.split("_")[-1]
        ex2, man = _apply_A7(extract, key, tool, SCRATCH / f"{key}_A7_{tool}")
    elif cf_id == "B1_translate":
        ex2, man = _apply_B_translate(extract, key, params["frac"])
    elif cf_id == "B2_scale":
        ex2, man = _apply_B_scale(extract, key, params["k"])
    elif cf_id == "B3_crop_jitter":
        ex2, man = _apply_B_cropjitter(extract, key, params["frac"])
    elif cf_id == "B4_aspect":
        ex2, man = _apply_B_aspect(extract, key, params["frac"])
    elif cf_id == "B5_rotate_page":
        ex2, man = _apply_B5(extract, key, params.get("add", 90))
    elif cf_id == "C1_remove_object":
        ex2, man = _apply_C1(extract, objects, key, params.get("bucket"))
    elif cf_id == "C2_add_object":
        ex2, man = _apply_C2(extract, objects, key, params.get("bucket"))
    elif cf_id == "C3_move_object":
        ex2, man = _apply_C3(extract, objects, key, params.get("bucket"), params.get("frac", 0.01))
    elif cf_id == "C4_swap_objects":
        ex2, man = _apply_swap(extract, objects, key, cf_id, like=True)
    elif cf_id == "C5_swap_unlike":
        ex2, man = _apply_swap(extract, objects, key, cf_id, like=False)
    elif cf_id == "C6_reshape_object":
        ex2, man = _apply_C6(extract, objects, key, params.get("bucket"))
    elif cf_id == "C7_split_object":
        ex2, man = _apply_C7(extract, objects, key)
    elif cf_id == "C8_merge_objects":
        ex2, man = _apply_C8(extract, objects, key)
    elif cf_id == "C9_add_branch":
        ex2, man = _apply_C9(extract, objects, key)
    elif cf_id == "C10_remove_opening":
        ex2, man = _apply_C10(extract, objects, key)
    elif cf_id == "D9_text_to_curves":
        ex2, man = _apply_D9(extract, key)
    elif cf_id.startswith("D"):
        ex2, man = _apply_D(extract, objects, cf_id, key, **params)
    else:
        raise ValueError(cf_id)
    man["schema"] = SCHEMA
    man["carrier_key"] = key
    man["block_frame_pt"] = [round(v, 3) for v in _frame_of(extract)]
    man["block_frame_pt_after"] = [round(v, 3) for v in _frame_of(ex2)]
    man["S_pt"] = round(float(objects.S), 4) if objects is not None else None
    man["n_text_before"] = len(extract.texts)
    man["n_text_after"] = len(ex2.texts)
    man["provenance"] = {"pdf_sha256": extract.provenance.get("pdf_sha256"),
                         "page_index": extract.provenance.get("page_index"),
                         "coords_px": extract.provenance.get("coords_px")}
    return ex2, man


def cleanup_scratch():
    shutil.rmtree(SCRATCH, ignore_errors=True)
