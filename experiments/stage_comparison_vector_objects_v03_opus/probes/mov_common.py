# -*- coding: utf-8 -*-
"""Shared plumbing for the `mov` probe.  Blocks are read only through v03_foundation."""
from __future__ import annotations
import json, math, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXP = HERE.parent
ART = EXP / "artifacts"
ROOT = EXP.parents[1]
sys.path.insert(0, str(HERE))

import v03_foundation as F          # noqa: E402
import v03_objects as O             # noqa: E402
import mov_align as A               # noqa: E402
import grp_match as GM              # noqa: E402


class FakeExtract:
    """Minimal BlockExtract stand-in for a segment list (same shape build_objects wants)."""
    def __init__(self, segments, texts, frame=None, provenance=None):
        self.segments = segments
        self.texts = texts
        self.frame = frame or {}
        self.provenance = provenance or {}


def layer_of(ex, **params):
    return O.build_objects(ex, **params)


def frame_of(ex):
    f = ex.frame if isinstance(ex.frame, dict) else {}
    c = f.get("clip_display")
    if c:
        return [float(v) for v in c]
    xs = [q for s in ex.segments for q in (s["p0"][0], s["p1"][0])]
    ys = [q for s in ex.segments for q in (s["p0"][1], s["p1"][1])]
    return [min(xs), min(ys), max(xs), max(ys)] if xs else [0, 0, 1, 1]


def shared_scale(exA, exB):
    """G2-2b of the `grp` probe: a SHARED characteristic scale, measured to be strictly
    better (median 1:1 churn 0.937 -> 0.988, 0 pairs worse).  Same rule here."""
    sa = O.build_objects(exA).S
    sb = O.build_objects(exB).S
    return max(sa, sb), sa, sb


def apply_sim(ex, T):
    """Return a copy of ``ex`` with every coordinate mapped through the similarity T.

    Needed because the object layer is NOT invariant to where the drawing sits on the
    page.  Measured in mov_m1b_phase.py on 16 real blocks: translating a block by as
    little as 0.01 * S (about 0.09 pt) already rewrites a median 3.0 % of the object
    partition and moves 11 of 16 blocks off their original partition; from 0.1 * S on the
    median churn is 0.09-0.18 with a maximum of 0.89.  It is NOT a clean grid-phase
    effect (a shift of exactly one S does not restore the partition) -- it is general
    instability.  The object COUNT hides it: n_obj stays within 2 % throughout.
    Grouping both sides in ONE frame is what removes it from the comparison.
    """
    segs = []
    for k, s in enumerate(ex.segments):
        t = dict(s)
        t["p0"] = T(s["p0"])
        t["p1"] = T(s["p1"])
        t["len"] = math.hypot(t["p1"][0] - t["p0"][0], t["p1"][1] - t["p0"][1])
        t["src"] = list(s.get("src") or [s.get("i", k)])
        segs.append(t)
    for i, t in enumerate(segs):
        t["i"] = i
    texts = []
    for t in ex.texts:
        u = dict(t)
        bb = t["bbox"]
        a = T((bb[0], bb[1])); b = T((bb[2], bb[3]))
        u["bbox"] = [min(a[0], b[0]), min(a[1], b[1]), max(a[0], b[0]), max(a[1], b[1])]
        u["cx"] = (u["bbox"][0] + u["bbox"][2]) / 2
        u["cy"] = (u["bbox"][1] + u["bbox"][3]) / 2
        u["size"] = float(t.get("size") or 0.0) * T.s
        texts.append(u)
    fr = dict(ex.frame) if isinstance(ex.frame, dict) else {}
    f = frame_of(ex)
    a = T((f[0], f[1])); b = T((f[2], f[3]))
    fr["clip_display"] = [min(a[0], b[0]), min(a[1], b[1]), max(a[0], b[0]), max(a[1], b[1])]
    return FakeExtract(segs, texts, fr, dict(getattr(ex, "provenance", {}) or {}))


invert = A.sim_inverse


def refine_ink(exA, exB, T, *, tol0=1.2, rounds=3, max_seg=6000):
    """Sharpen the object-anchor transform on segment ENDPOINT correspondences (ICP).

    Still a closed-form fit on point pairs, not a picture search: correspondences are
    exact (both endpoints of a segment within tol), so the fit is over-determined and
    a moved object simply drops out of it.  Needed because the object layer regroups
    under sub-point misalignment (mov_phase.json), which biases the anchor-only estimate
    through the very centroids it uses.
    """
    sa = exA.segments
    sb = exB.segments
    if not sa or not sb:
        return T, {"refined": False, "reason": "empty"}
    step = max(1, len(sa) // max_seg)
    sub = sa[::step]
    info = {"refined": True, "n_sub": len(sub)}
    tol = tol0
    for _ in range(rounds):
        eidx = GM.build_endpoint_index(sb)
        pa, pb = [], []
        for s in sub:
            a0 = T(s["p0"]); a1 = T(s["p1"])
            bi = GM.query_endpoints(eidx, a0[0], a0[1], a1[0], a1[1], tol)
            if bi is None:
                continue
            b = sb[bi]
            # orient: pair the endpoints that are actually closest
            if (math.hypot(b["p0"][0] - a0[0], b["p0"][1] - a0[1])
                    <= math.hypot(b["p1"][0] - a0[0], b["p1"][1] - a0[1])):
                pa.extend([s["p0"], s["p1"]]); pb.extend([b["p0"], b["p1"]])
            else:
                pa.extend([s["p0"], s["p1"]]); pb.extend([b["p1"], b["p0"]])
        if len(pa) < 6:
            info["n_pairs"] = len(pa) // 2
            break
        T2 = A.fit_similarity(pa, pb, T.theta)
        if T2 is None:
            break
        T = T2
        info["n_pairs"] = len(pa) // 2
        tol = max(0.15, tol / 3.0)
    info["transform"] = T.as_dict()
    return T, info


def align_pair(exA, exB, *, share_S=True, two_pass=True, refine=True, layer_params=None, **align_params):
    """Estimate the global transform, then (two_pass) regroup B in A's frame and match."""
    lp = dict(layer_params or {})
    if share_S:
        S, sa, sb = shared_scale(exA, exB)
        lp["S_override"] = S
    LA = layer_of(exA, **lp)
    LB = layer_of(exB, **lp)
    rep = A.align(LA, exA.segments, frame_of(exA), LB, exB.segments, frame_of(exB),
                  **align_params)
    rep["S_a"], rep["S_b"] = round(LA.S, 4), round(LB.S, 4)
    rep["pass"] = 1
    if not two_pass or rep.get("status") == "ALIGNMENT_UNAVAILABLE":
        return rep, LA, LB
    T = A.Sim(rep["transform"]["s"], rep["transform"]["theta"],
              rep["transform"]["tx"], rep["transform"]["ty"])
    if refine:
        T, rinfo = refine_ink(exA, exB, T)
        rep["refine"] = rinfo
    exB2 = apply_sim(exB, invert(T))
    LB2 = layer_of(exB2, **lp)
    rep2 = A.align(LA, exA.segments, frame_of(exA), LB2, exB2.segments, frame_of(exB2),
                   **align_params)
    rep2["S_a"], rep2["S_b"] = rep["S_a"], rep["S_b"]
    rep2["pass"] = 2
    rep2["global_transform"] = T.as_dict()
    rep2["transform_anchors_only"] = rep["transform"]
    rep2["refine"] = rep.get("refine")
    rep2["pass1"] = {k: rep.get(k) for k in ("transform", "counts", "residual", "status",
                                             "estimate", "verdict")}
    # the ledger's BLOCK_TRANSFORMED entry belongs to pass 1 (pass 2 works pre-compensated)
    led = [l for l in rep2.get("ledger", []) if l["type"] != "BLOCK_TRANSFORMED"]
    Tg = T
    if not Tg.is_trivial(A.DEFAULTS["t_eps_pt"], A.DEFAULTS["s_eps"]):
        kind = []
        if Tg.theta:
            kind.append("rotate")
        if abs(Tg.s - 1.0) > A.DEFAULTS["s_eps"]:
            kind.append("scale")
        if math.hypot(Tg.tx, Tg.ty) > A.DEFAULTS["t_eps_pt"]:
            kind.append("translate")
        led.insert(0, {"type": "BLOCK_TRANSFORMED", "kind": "+".join(kind), **Tg.as_dict()})
    rep2["ledger"] = led
    rep2["verdict"] = ("GRAPHIC_CHANGE" if any(l["type"] != "BLOCK_TRANSFORMED" for l in led)
                       else ("BLOCK_TRANSFORMED" if led else "NO_GRAPHIC_CHANGE"))
    return rep2, LA, LB2


def extract_side(pdf, page_index, coords_px, page_px_w, page_px_h):
    return F.extract_block(str(pdf), int(page_index), coords_px, page_px_w, page_px_h)


def jdump(obj, name):
    p = ART / name
    p.parent.mkdir(parents=True, exist_ok=True)
    json.dump(obj, open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return str(p)


# ------------------------------------------------------------------ the full comparison

MIN_INK = 1.0e-4      # ink-length share below which a cluster is not worth a ledger line


def compare(exA, exB, *, modes=("strict",), ink=True, max_seg_ink=120000, **kw):
    """One prepared block against another: global transform + object + ink evidence."""
    rep, LA, LB = align_pair(exA, exB, **kw)
    out = {k: v for k, v in rep.items() if k != "_rows"}
    if rep.get("status") == "ALIGNMENT_UNAVAILABLE":
        out["verdict"] = "UNKNOWN"
        return out, rep, LA, LB
    g = rep.get("global_transform") or rep["transform"]
    T = A.Sim(g["s"], g["theta"], g["tx"], g["ty"])
    S = max(rep["S_a"], rep["S_b"])
    if ink:
        out["ink"] = {}
        for m in modes:
            out["ink"][m] = A.ink_changes(exA.segments, exB.segments, T, S,
                                          rep["frame_intersection"], A.DEFAULTS,
                                          mode=m, max_seg=max_seg_ink)
    # unified ledger: the ink evidence decides, the object layer names the object
    led = [l for l in rep.get("ledger", []) if l["type"] == "BLOCK_TRANSFORMED"]
    ik = (out.get("ink") or {}).get(modes[0]) if ink else None
    if ik and "skipped" not in ik:
        for c in ik["clusters_a"]:
            if c["ink_len"] <= 0:
                continue
            if not c["inside_intersection"] and not c["moved"]:
                led.append({"type": "BORDER_UNCERTAIN", "side": "a", "bbox_pt": c["bbox"],
                            "ink_len": c["ink_len"]})
                continue
            if c["moved"]:
                led.append({"type": "MOVED_INK", "bbox_pt": c["bbox"], "ink_len": c["ink_len"],
                            **c["moved"]})
            else:
                led.append({"type": "REMOVED_INK", "bbox_pt": c["bbox"], "ink_len": c["ink_len"]})
        for c in ik["clusters_b"]:
            if c["moved_from"] is not None:
                continue
            if not c["inside_intersection"]:
                led.append({"type": "BORDER_UNCERTAIN", "side": "b", "bbox_pt": c["bbox"],
                            "ink_len": c["ink_len"]})
            else:
                led.append({"type": "ADDED_INK", "bbox_pt": c["bbox"], "ink_len": c["ink_len"]})
    out["ledger_unified"] = led
    real = [l for l in led if l["type"] in ("MOVED_INK", "REMOVED_INK", "ADDED_INK")]
    if real:
        out["verdict"] = "GRAPHIC_CHANGE"
    elif any(l["type"] == "BLOCK_TRANSFORMED" for l in led):
        out["verdict"] = "BLOCK_TRANSFORMED"
    else:
        out["verdict"] = "NO_GRAPHIC_CHANGE"
    return out, rep, LA, LB
