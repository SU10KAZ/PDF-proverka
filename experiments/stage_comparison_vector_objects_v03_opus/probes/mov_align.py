# -*- coding: utf-8 -*-
"""VECTOR 0.3 · probe `mov` — global block alignment vs. a single moved object (§13).

    T, report = align(layer_a, segs_a, frame_a, layer_b, segs_b, frame_b)

The question this file answers is NOT "do the two pictures overlap" but
"is the difference a rigid re-placement of the WHOLE block (not a project change)
or a displacement of ONE object relative to the others (possibly a project change)".

Design constraints, each of them measured elsewhere in this track:
  * everything in **PDF points** (v0.2: tolerances in fractions of the block lose 11.7 %);
  * the transform is estimated from OBJECT anchors, never from the block bbox —
    `mine` M4 measured that version bboxes differ by >2 % of width on 47 % of pairs,
    so the bbox itself is a moving target;
  * 4-6 parameters only: translation + ONE isotropic scale + rotation constrained to
    multiples of 90 deg.  A free-rotation estimate is computed alongside purely to
    report how often the constraint is violated;
  * closed form (Umeyama with the rotation fixed) inside a RANSAC/IRLS loop —
    never a picture fit;
  * the crop frames of the two sides are different windows onto the same drawing
    (`mine` M5: on 36.5 % of pairs with a residual, ALL big difference components sit
    on the crop border).  Anything outside the INTERSECTION of the two frames is
    reported as `border_uncertain` and is never called moved / added / removed.

Nothing outside experiments/stage_comparison_vector_objects_v03_opus/ is touched.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

import numpy as np

# ------------------------------------------------------------------ parameters

DEFAULTS: dict[str, Any] = {
    "anchor_max": 150,        # anchors used to ESTIMATE the transform (largest objects)
    "anchor_min_seg": 2,
    "knn": 6,                 # descriptor neighbours per anchor
    "pos_seed_r": 0.10,       # positional candidate radius, fraction of block diagonal
    "pos_seed_k": 4,
    "ransac_iters": 400,
    "ransac_seed": 20260823,
    "tol_coarse_pt": 3.0,     # RANSAC inlier tolerance, PDF points
    "tol_floor_pt": 0.20,     # IRLS never shrinks below this
    "irls_rounds": 4,
    "min_anchor_pairs": 3,
    "min_inlier_ratio": 0.30, # below this: ALIGNMENT_UNAVAILABLE (no consensus)
    "ambig_ratio": 0.60,      # second consensus this strong -> AMBIGUOUS
    "scale_lo": 0.40,         # a similarity outside this band is not a block transform
    "scale_hi": 2.50,
    "dir_tol_deg": 3.0,       # 2-point sample: direction consistency
    "match_r_S": 3.0,         # positional partner search radius, units of S
    "desc_max": 0.60,         # L1 descriptor distance accepted for a partner
    "len_ratio": 1.6,         # ink-length compatibility for a partner
    "move_k_sigma": 6.0,      # moved if residual > k * robust sigma ...
    "move_floor_pt": 0.35,    # ... and > this many PDF points
    "search_far_frac": 0.45,  # far search radius for a displaced object, frac of diag
    "far_len_ratio": 1.15,
    "far_desc_max": 0.25,
    "border_pad_frac": 0.02,
    "t_eps_pt": 0.30,         # |t| below this -> transform is trivial
    "s_eps": 0.002,
}

SCHEMA = "v03-mov-align-1"

# ------------------------------------------------------------------ descriptor rotation

# 6 direction bins of 30 deg (mod 180): a rotation by 90 deg shifts by 3 bins.
_ANG_PERM = {0: [0, 1, 2, 3, 4, 5], 90: [3, 4, 5, 0, 1, 2],
             180: [0, 1, 2, 3, 4, 5], 270: [3, 4, 5, 0, 1, 2]}


def _grid_perm(theta: int) -> list[int]:
    """4x4 occupancy grid permutation for a rotation by theta, SRC index -> DST index.

    Derived from ``rot_apply`` itself, so it is the same rotation the coordinates get:
    a cell centre (u, v) in the object's own normalised bbox goes to (1 - v, u) under
    90 deg, i.e. cell (gx, gy) -> (3 - gy, gx).
    """
    perm = [0] * 16
    for gy in range(4):
        for gx in range(4):
            x, y = gx, gy
            for _ in range(theta // 90):
                x, y = 3 - y, x
            perm[gy * 4 + gx] = y * 4 + x      # src -> dst
    return perm


_GRID_PERM = {t: _grid_perm(t) for t in (0, 90, 180, 270)}


def rotate_desc(vec: Sequence[float], theta: int) -> list[float]:
    """Descriptor of the same shape seen after a rotation by theta (exact permutation)."""
    theta %= 360
    out = [0.0] * len(vec)
    out[0] = vec[0] if theta in (0, 180) else 1.0 - vec[0]
    out[1] = vec[1]
    ap = _ANG_PERM[theta]
    for i in range(6):
        out[2 + ap[i]] = vec[2 + i]
    gp = _GRID_PERM[theta]
    for i in range(16):
        out[8 + gp[i]] = vec[8 + i]
    if len(vec) > 24:
        out[24] = vec[24]
    return out


# ------------------------------------------------------------------ geometry helpers

def rot_apply(theta: int, x: float, y: float) -> tuple[float, float]:
    theta %= 360
    if theta == 0:
        return x, y
    if theta == 90:
        return -y, x
    if theta == 180:
        return -x, -y
    return y, -x


@dataclass
class Sim:
    """s * R_theta * p + t   (4 parameters when theta is fixed, 6 counting theta and reflection)."""
    s: float = 1.0
    theta: int = 0
    tx: float = 0.0
    ty: float = 0.0

    def __call__(self, p) -> tuple[float, float]:
        x, y = rot_apply(self.theta, p[0], p[1])
        return (self.s * x + self.tx, self.s * y + self.ty)

    def as_dict(self) -> dict:
        return {"s": round(self.s, 6), "theta": self.theta,
                "tx": round(self.tx, 4), "ty": round(self.ty, 4)}

    def is_trivial(self, t_eps: float, s_eps: float) -> bool:
        return (self.theta == 0 and abs(self.s - 1.0) <= s_eps
                and math.hypot(self.tx, self.ty) <= t_eps)


def sim_inverse(T: Sim) -> Sim:
    """Exact inverse of s*R_theta + t (theta is a multiple of 90)."""
    s = 1.0 / T.s
    th = (-T.theta) % 360
    x, y = rot_apply(th, -T.tx, -T.ty)
    return Sim(s, th, s * x, s * y)


def fit_similarity(pa, pb, theta: int) -> Optional[Sim]:
    """Closed form least squares for s and t with the rotation FIXED (Umeyama, R given)."""
    n = len(pa)
    if n == 0:
        return None
    ax = np.empty(n); ay = np.empty(n)
    for i, p in enumerate(pa):
        ax[i], ay[i] = rot_apply(theta, p[0], p[1])
    bx = np.array([q[0] for q in pb]); by = np.array([q[1] for q in pb])
    max_, may = ax.mean(), ay.mean()
    mbx, mby = bx.mean(), by.mean()
    dax, day = ax - max_, ay - may
    dbx, dby = bx - mbx, by - mby
    den = float((dax * dax + day * day).sum())
    if den <= 1e-12:
        s = 1.0
    else:
        s = float((dax * dbx + day * dby).sum() / den)
    if not (1e-6 < s < 1e6):
        s = 1.0
    return Sim(s, theta, mbx - s * max_, mby - s * may)


def fit_similarity_free(pa, pb) -> Optional[dict]:
    """Full similarity with FREE rotation (diagnostic only: how far from k*90 is reality)."""
    n = len(pa)
    if n < 2:
        return None
    A = np.array(pa, dtype=float); B = np.array(pb, dtype=float)
    ma, mb = A.mean(0), B.mean(0)
    A0, B0 = A - ma, B - mb
    num = float((A0[:, 0] * B0[:, 1] - A0[:, 1] * B0[:, 0]).sum())
    den = float((A0[:, 0] * B0[:, 0] + A0[:, 1] * B0[:, 1]).sum())
    if abs(num) < 1e-12 and abs(den) < 1e-12:
        return None
    ang = math.degrees(math.atan2(num, den))
    var = float((A0 * A0).sum())
    s = math.hypot(num, den) / var if var > 1e-12 else 1.0
    return {"theta_free_deg": round(ang, 4), "s_free": round(s, 6)}


# ------------------------------------------------------------------ object features

def object_points(layer, segs) -> list[dict]:
    """Ink-weighted centroid per object (more stable than the bbox centre under clipping)."""
    out = []
    for oi, o in enumerate(layer.objects):
        sx = sy = w = 0.0
        for gi in o["segments"]:
            s = segs[gi]
            L = s["len"]
            sx += L * (s["p0"][0] + s["p1"][0]) / 2
            sy += L * (s["p0"][1] + s["p1"][1]) / 2
            w += L
        if w <= 0:
            cx, cy = o["cx"], o["cy"]
        else:
            cx, cy = sx / w, sy / w
        out.append({"oi": oi, "cx": cx, "cy": cy, "len": o["seg_len"], "n_seg": o["n_seg"],
                    "diag": o["diag"], "cls": o["cls"], "bbox": o["bbox"],
                    "vec": o["desc"]["vec"], "object_id": o.get("object_id")})
    return out


def _desc_matrix(pts, theta=0):
    if not pts:
        return np.zeros((0, 25))
    if theta == 0:
        return np.array([p["vec"] for p in pts], dtype=float)
    return np.array([rotate_desc(p["vec"], theta) for p in pts], dtype=float)


class _Grid:
    def __init__(self, pts, cell):
        self.cell = max(cell, 1e-6)
        self.g: dict[tuple[int, int], list[int]] = {}
        for i, p in enumerate(pts):
            k = (int(p["cx"] // self.cell), int(p["cy"] // self.cell))
            self.g.setdefault(k, []).append(i)

    def near(self, x, y, r):
        c = self.cell
        n = int(r // c) + 1
        gx, gy = int(x // c), int(y // c)
        out = []
        for i in range(gx - n, gx + n + 1):
            for j in range(gy - n, gy + n + 1):
                out.extend(self.g.get((i, j), ()))
        return out


# ------------------------------------------------------------------ the estimator

def _anchors(pts, p):
    cand = [q for q in pts if q["cls"] != "stray" and q["n_seg"] >= p["anchor_min_seg"]]
    if not cand:
        cand = list(pts)
    cand.sort(key=lambda q: (-q["len"], q["cx"], q["cy"]))
    return cand[: p["anchor_max"]]


def _candidates(A, B, p, diag_ref):
    """(ai, bi, theta) candidate correspondences from descriptor kNN + positional seeding."""
    out: set[tuple[int, int, int]] = set()
    if not A or not B:
        return []
    MB = _desc_matrix(B)
    lenB = np.array([q["len"] for q in B])
    for theta in (0, 90, 180, 270):
        MA = _desc_matrix(A, theta)
        D = np.abs(MA[:, None, :] - MB[None, :, :]).sum(-1)
        lenA = np.array([q["len"] for q in A])
        ratio = np.maximum(lenA[:, None] / np.maximum(lenB[None, :], 1e-9),
                           lenB[None, :] / np.maximum(lenA[:, None], 1e-9))
        D = D + np.where(ratio > 4.0, 10.0, 0.0)     # gross ink-length mismatch
        k = min(p["knn"], len(B))
        idx = np.argpartition(D, k - 1, axis=1)[:, :k]
        for ai in range(len(A)):
            for bi in idx[ai]:
                out.add((ai, int(bi), theta))
    # positional seeding: the two crops usually already overlap
    gb = _Grid(B, max(diag_ref * 0.05, 1.0))
    r = p["pos_seed_r"] * diag_ref
    for ai, a in enumerate(A):
        near = [(math.hypot(B[bi]["cx"] - a["cx"], B[bi]["cy"] - a["cy"]), bi)
                for bi in gb.near(a["cx"], a["cy"], r)]
        near.sort()
        for _d, bi in near[: p["pos_seed_k"]]:
            out.add((ai, bi, 0))
    return sorted(out)


def _score(T, cands, A, B, tol):
    hit: dict[int, float] = {}
    inl = []
    t2 = tol * tol
    for (ai, bi, th) in cands:
        if th != T.theta:
            continue
        x, y = T((A[ai]["cx"], A[ai]["cy"]))
        d2 = (x - B[bi]["cx"]) ** 2 + (y - B[bi]["cy"]) ** 2
        if d2 <= t2:
            if ai not in hit or d2 < hit[ai]:
                hit[ai] = d2
                inl.append((ai, bi, d2))
    best: dict[int, tuple[int, float]] = {}
    for ai, bi, d2 in inl:
        if ai not in best or d2 < best[ai][1]:
            best[ai] = (bi, d2)
    used_b = set()
    pairs = []
    for ai, (bi, d2) in sorted(best.items(), key=lambda kv: kv[1][1]):
        if bi in used_b:
            continue
        used_b.add(bi)
        pairs.append((ai, bi))
    return pairs


def estimate_transform(A, B, p, diag_ref):
    """RANSAC over 2-point samples + closed-form IRLS refinement.  Returns (Sim, info)."""
    info: dict[str, Any] = {"n_anchor_a": len(A), "n_anchor_b": len(B)}
    if len(A) < p["min_anchor_pairs"] or len(B) < p["min_anchor_pairs"]:
        info["reason"] = "too_few_anchors"
        return None, info
    cands = _candidates(A, B, p, diag_ref)
    info["n_candidates"] = len(cands)
    by_theta: dict[int, list] = {}
    for c in cands:
        by_theta.setdefault(c[2], []).append(c)
    rng = random.Random(p["ransac_seed"])
    hyp: list[tuple[int, Sim]] = []
    # deterministic seed hypotheses first
    for th in (0,):
        hyp.append((th, Sim(1.0, th, 0.0, 0.0)))
    for th, cl in by_theta.items():
        if len(cl) < 2:
            continue
        n_it = p["ransac_iters"]
        for _ in range(n_it):
            (a1, b1, _), (a2, b2, _) = rng.sample(cl, 2)
            if a1 == a2 or b1 == b2:
                continue
            ax, ay = rot_apply(th, A[a2]["cx"] - A[a1]["cx"], A[a2]["cy"] - A[a1]["cy"])
            bx, by = B[b2]["cx"] - B[b1]["cx"], B[b2]["cy"] - B[b1]["cy"]
            la, lb = math.hypot(ax, ay), math.hypot(bx, by)
            if la < 1e-6 or lb < 1e-6:
                continue
            s = lb / la
            if not (p["scale_lo"] <= s <= p["scale_hi"]):
                continue
            da = math.degrees(math.atan2(by, bx) - math.atan2(ay, ax))
            da = (da + 180.0) % 360.0 - 180.0
            if abs(da) > p["dir_tol_deg"]:
                continue
            ax1, ay1 = rot_apply(th, A[a1]["cx"], A[a1]["cy"])
            hyp.append((th, Sim(s, th, B[b1]["cx"] - s * ax1, B[b1]["cy"] - s * ay1)))
    best = None
    for th, T in hyp:
        pairs = _score(T, cands, A, B, p["tol_coarse_pt"])
        if best is None or len(pairs) > len(best[1]):
            best = (T, pairs)
    if best is None or len(best[1]) < p["min_anchor_pairs"]:
        info["reason"] = "no_consensus"
        info["best_inliers"] = 0 if best is None else len(best[1])
        return None, info
    T, pairs = best
    tol = p["tol_coarse_pt"]
    for _ in range(p["irls_rounds"]):
        if len(pairs) < 2:
            break
        T2 = fit_similarity([(A[a]["cx"], A[a]["cy"]) for a, _ in pairs],
                            [(B[b]["cx"], B[b]["cy"]) for _, b in pairs], T.theta)
        if T2 is None or not (p["scale_lo"] <= T2.s <= p["scale_hi"]):
            break
        res = []
        for a, b in pairs:
            x, y = T2((A[a]["cx"], A[a]["cy"]))
            res.append(math.hypot(x - B[b]["cx"], y - B[b]["cy"]))
        med = float(np.median(res)) if res else 0.0
        mad = float(np.median([abs(r - med) for r in res])) if res else 0.0
        tol = max(p["tol_floor_pt"], min(tol, 3.0 * (med + 1.4826 * mad) + p["tol_floor_pt"]))
        T = T2
        pairs = _score(T, cands, A, B, tol)
        if len(pairs) < p["min_anchor_pairs"]:
            break
    info["inliers"] = len(pairs)
    info["inlier_ratio"] = round(len(pairs) / max(1, min(len(A), len(B))), 4)
    info["tol_final_pt"] = round(tol, 4)
    free = fit_similarity_free([(A[a]["cx"], A[a]["cy"]) for a, _ in pairs],
                               [(B[b]["cx"], B[b]["cy"]) for _, b in pairs])
    if free:
        info.update(free)
    # second consensus (repeated motif / 1 -> N)
    rest = [c for c in cands if c[0] not in {a for a, _ in pairs}]
    second = 0
    if len(rest) >= 4:
        b2 = None
        for th, T2 in hyp:
            pr = _score(T2, rest, A, B, max(tol, p["tol_coarse_pt"]))
            if b2 is None or len(pr) > len(b2):
                b2 = pr
        if b2 is not None:
            second = len(b2)
    info["second_consensus"] = second
    info["ambiguous"] = bool(second >= p["ambig_ratio"] * len(pairs) and second >= 4)
    if info["inlier_ratio"] < p["min_inlier_ratio"]:
        info["reason"] = "low_inlier_ratio"
        return None, info
    return T, info


# ------------------------------------------------------------------ per-object report

def _frame_intersection(fa, fb, T):
    """A-frame mapped through T, intersected with the B frame.  All in B coordinates."""
    c = [T((fa[0], fa[1])), T((fa[2], fa[1])), T((fa[2], fa[3])), T((fa[0], fa[3]))]
    xs = [q[0] for q in c]; ys = [q[1] for q in c]
    ax0, ay0, ax1, ay1 = min(xs), min(ys), max(xs), max(ys)
    return (max(ax0, fb[0]), max(ay0, fb[1]), min(ax1, fb[2]), min(ay1, fb[3]))


def _inside(bb, box, pad):
    return (bb[0] >= box[0] + pad and bb[1] >= box[1] + pad
            and bb[2] <= box[2] - pad and bb[3] <= box[3] - pad)


def _margin(bb, box):
    """Signed distance from a bbox to the nearest edge of `box` (negative = sticks out).
    Recorded per cluster so a border-pad sweep needs no re-matching."""
    return min(bb[0] - box[0], bb[1] - box[1], box[2] - bb[2], box[3] - bb[3])


def match_objects(PA, PB, T, p, S, diag_ref, inter):
    """Object partnership: a global greedy assignment, then a far search for the rest.

    A one-shot nearest-neighbour is not enough: in dense CAD the nearest free object to
    an A object is often the partner of its neighbour, and a first-come assignment then
    reports both as REMOVED + ADDED.  Measured on B1_translate (where the truth is
    "nothing changed"): one-shot gave 20 removed + 20 added of 75 objects, the greedy
    assignment below gives 0.
    """
    r_near = max(p["match_r_S"] * S, 1.0)
    gb = _Grid(PB, max(r_near, 1.0))
    cand: list[tuple[float, int, int, float, float]] = []
    tpos = []
    for ai, a in enumerate(PA):
        x, y = T((a["cx"], a["cy"]))
        tpos.append((x, y))
        av = rotate_desc(a["vec"], T.theta)
        local = []
        for bi in gb.near(x, y, r_near):
            b = PB[bi]
            d = math.hypot(b["cx"] - x, b["cy"] - y)
            if d > r_near:
                continue
            lr = max(a["len"] / max(b["len"], 1e-9), b["len"] / max(a["len"], 1e-9))
            if lr > p["len_ratio"]:
                continue
            dd = sum(abs(u - v) for u, v in zip(av, b["vec"]))
            if dd > p["desc_max"]:
                continue
            local.append((d + 4.0 * dd * S + 0.5 * S * (lr - 1.0), ai, bi, d, dd))
        local.sort()
        cand.extend(local[:6])
    cand.sort()
    a_taken: dict[int, tuple] = {}
    b_taken: dict[int, int] = {}
    for sc, ai, bi, d, dd in cand:
        if ai in a_taken or bi in b_taken:
            continue
        a_taken[ai] = (bi, d, dd)
        b_taken[bi] = ai
    rows = []
    for ai, a in enumerate(PA):
        m = a_taken.get(ai)
        rows.append({"ai": ai, "cx": a["cx"], "cy": a["cy"], "len": a["len"],
                     "n_seg": a["n_seg"], "cls": a["cls"], "bbox": a["bbox"],
                     "object_id": a["object_id"], "tx": tpos[ai][0], "ty": tpos[ai][1],
                     "bi": None if m is None else m[0],
                     "res": None if m is None else m[1],
                     "desc_d": None if m is None else m[2], "far": False})
    # far search: an object that really travelled is not in its old neighbourhood at all
    freeB = [bi for bi in range(len(PB)) if bi not in b_taken]
    if freeB:
        far_r = p["search_far_frac"] * diag_ref
        gfar = _Grid([PB[b] for b in freeB], max(far_r / 6.0, 2.0))
        pend = []
        for r in rows:
            if r["bi"] is not None:
                continue
            a = PA[r["ai"]]
            av = rotate_desc(a["vec"], T.theta)
            x, y = r["tx"], r["ty"]
            for j in gfar.near(x, y, far_r):
                bi = freeB[j]
                b = PB[bi]
                d = math.hypot(b["cx"] - x, b["cy"] - y)
                if d > far_r:
                    continue
                lr = max(a["len"] / max(b["len"], 1e-9), b["len"] / max(a["len"], 1e-9))
                if lr > p["far_len_ratio"]:
                    continue
                dd = sum(abs(u - v) for u, v in zip(av, b["vec"]))
                if dd > p["far_desc_max"]:
                    continue
                pend.append((dd + d / max(diag_ref, 1e-9), r["ai"], bi, d, dd))
        pend.sort()
        for sc, ai, bi, d, dd in pend:
            if ai in a_taken or bi in b_taken:
                continue
            a_taken[ai] = (bi, d, dd)
            b_taken[bi] = ai
            rr = rows[ai]
            rr["bi"], rr["res"], rr["desc_d"], rr["far"] = bi, d, dd, True
    pad = p["border_pad_frac"] * diag_ref / math.sqrt(2)
    for r in rows:
        bb = r["bbox"]
        tb = list(T((bb[0], bb[1]))) + list(T((bb[2], bb[3])))
        tb = [min(tb[0], tb[2]), min(tb[1], tb[3]), max(tb[0], tb[2]), max(tb[1], tb[3])]
        r["inside_intersection"] = _inside(tb, inter, pad)
        r["t_bbox"] = tb
    return rows, b_taken


def align(layer_a, segs_a, frame_a, layer_b, segs_b, frame_b, **params):
    """Main entry point.  Returns a report dict (never raises on ordinary failure)."""
    p = dict(DEFAULTS); p.update(params or {})
    PA = object_points(layer_a, segs_a)
    PB = object_points(layer_b, segs_b)
    fa = list(frame_a); fb = list(frame_b)
    diag_ref = max(math.hypot(fa[2] - fa[0], fa[3] - fa[1]),
                   math.hypot(fb[2] - fb[0], fb[3] - fb[1]), 1.0)
    S = max(getattr(layer_a, "S", 1.0), getattr(layer_b, "S", 1.0))
    rep: dict[str, Any] = {"schema": SCHEMA, "n_obj_a": len(PA), "n_obj_b": len(PB),
                           "S_pt": round(S, 4), "diag_ref_pt": round(diag_ref, 3)}
    if not PA or not PB:
        rep["status"] = "ALIGNMENT_UNAVAILABLE"
        rep["reason"] = "empty_object_layer"
        return rep
    A = _anchors(PA, p); B = _anchors(PB, p)
    T, info = estimate_transform(A, B, p, diag_ref)
    rep["estimate"] = info
    if T is None:
        rep["status"] = "ALIGNMENT_UNAVAILABLE"
        rep["reason"] = info.get("reason", "no_consensus")
        return rep
    rep["transform"] = T.as_dict()
    inter = _frame_intersection(fa, fb, T)
    rep["frame_intersection"] = [round(v, 3) for v in inter]
    inter_area = max(0.0, inter[2] - inter[0]) * max(0.0, inter[3] - inter[1])
    fb_area = max(1e-9, (fb[2] - fb[0]) * (fb[3] - fb[1]))
    rep["frame_overlap_share"] = round(inter_area / fb_area, 4)
    rows, taken = match_objects(PA, PB, T, p, S, diag_ref, inter)
    matched = [r for r in rows if r["bi"] is not None]
    interior = [r for r in matched if r["inside_intersection"]]
    res = [r["res"] for r in interior] or [r["res"] for r in matched]
    if res:
        med = float(np.median(res))
        mad = float(np.median([abs(v - med) for v in res]))
        sigma = 1.4826 * mad
    else:
        med = mad = sigma = 0.0
    thr = max(p["move_k_sigma"] * sigma, p["move_floor_pt"])
    rep["residual"] = {"n_matched": len(matched), "n_interior": len(interior),
                       "median_pt": round(med, 4), "mad_pt": round(mad, 4),
                       "sigma_pt": round(sigma, 4), "p90_pt": round(float(np.percentile(res, 90)), 4) if res else None,
                       "max_pt": round(max(res), 4) if res else None,
                       "move_threshold_pt": round(thr, 4)}
    ledger = []
    moved, border_uncertain, removed, added = [], [], [], []
    for r in rows:
        if not r["inside_intersection"]:
            border_uncertain.append(r)
            continue
        if r["bi"] is None:
            removed.append(r)
            continue
        if r["res"] is not None and r["res"] > thr:
            moved.append(r)
    for bi, b in enumerate(PB):
        if bi in taken:
            continue
        bb = b["bbox"]
        if _inside(bb, inter, p["border_pad_frac"] * diag_ref / math.sqrt(2)):
            added.append({"bi": bi, "cx": b["cx"], "cy": b["cy"], "len": b["len"],
                          "n_seg": b["n_seg"], "object_id": b["object_id"], "bbox": bb})
        else:
            border_uncertain.append({"ai": None, "bi": bi, "side": "b", "bbox": bb,
                                     "len": b["len"], "n_seg": b["n_seg"]})
    for r in sorted(moved, key=lambda r: -r["len"]):
        x, y = T((r["cx"], r["cy"]))
        b = PB[r["bi"]]
        ledger.append({"type": "MOVED_OBJECT", "object_id": r["object_id"],
                       "bbox_pt": [round(v, 3) for v in r["bbox"]],
                       "dx_pt": round(b["cx"] - x, 4), "dy_pt": round(b["cy"] - y, 4),
                       "d_pt": round(r["res"], 4), "ink_len": round(r["len"], 2),
                       "far": bool(r.get("far"))})
    for r in sorted(removed, key=lambda r: -r["len"]):
        ledger.append({"type": "REMOVED_OBJECT", "object_id": r["object_id"],
                       "bbox_pt": [round(v, 3) for v in r["bbox"]], "ink_len": round(r["len"], 2)})
    for r in sorted(added, key=lambda r: -r["len"]):
        ledger.append({"type": "ADDED_OBJECT", "object_id": r["object_id"],
                       "bbox_pt": [round(v, 3) for v in r["bbox"]], "ink_len": round(r["len"], 2)})
    trivial = T.is_trivial(p["t_eps_pt"], p["s_eps"])
    if not trivial:
        kind = []
        if T.theta:
            kind.append("rotate")
        if abs(T.s - 1.0) > p["s_eps"]:
            kind.append("scale")
        if math.hypot(T.tx, T.ty) > p["t_eps_pt"]:
            kind.append("translate")
        ledger.insert(0, {"type": "BLOCK_TRANSFORMED", "kind": "+".join(kind),
                          **T.as_dict()})
    rep["ledger"] = ledger
    rep["counts"] = {"moved": len(moved), "removed": len(removed), "added": len(added),
                     "border_uncertain": len(border_uncertain),
                     "matched": len(matched)}
    rep["border_uncertain_ink"] = round(sum(r.get("len", 0.0) for r in border_uncertain), 2)
    if info.get("ambiguous"):
        rep["status"] = "ALIGNMENT_AMBIGUOUS"
    else:
        rep["status"] = "ALIGNED"
    rep["verdict"] = ("GRAPHIC_CHANGE" if (moved or removed or added)
                      else ("BLOCK_TRANSFORMED" if not trivial else "NO_GRAPHIC_CHANGE"))
    rep["_rows"] = rows
    return rep


# ------------------------------------------------------------------ ink-level changes
# Object-level bookkeeping alone cannot answer "which object moved".  Measured on real
# pairs in this probe (mov_prose.json -> oi_*): of 61 pairs where the INK evidence is
# silent, the object bookkeeping still claims changes on 23 (p90 = 38 objects, max 294) —
# the grouping rebuilds itself from the smallest frame shift, the ink does not.  Ink-level
# evidence does not depend on grouping at all, so it is computed separately and reported
# alongside; the object layer only supplies the NAME of what changed.

import grp_match as _GM     # noqa: E402  (endpoint / parallel matchers, PDF points)


def _cluster_segments(segs, radius):
    """Union-find clustering by spatial proximity (grid, O(n))."""
    n = len(segs)
    if n == 0:
        return []
    cell = max(radius, 1e-6)
    grid: dict[tuple[int, int], int] = {}
    uf = list(range(n))

    def find(a):
        while uf[a] != a:
            uf[a] = uf[uf[a]]
            a = uf[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            uf[rb] = ra

    for i, s in enumerate(segs):
        L = max(s["len"], 1e-9)
        k = int(L / (cell / 2.0)) + 1
        for t in range(k + 1):
            u = t / k
            x = s["p0"][0] + u * (s["p1"][0] - s["p0"][0])
            y = s["p0"][1] + u * (s["p1"][1] - s["p0"][1])
            gx, gy = int(math.floor(x / cell)), int(math.floor(y / cell))
            for dx in (-1, 0, 1):
                for dy in (-1, 0, 1):
                    j = grid.get((gx + dx, gy + dy))
                    if j is not None:
                        union(i, j)
            grid.setdefault((gx, gy), i)
    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)
    return list(groups.values())


def _seg_bbox(segs, idx):
    xs = [q for i in idx for q in (segs[i]["p0"][0], segs[i]["p1"][0])]
    ys = [q for i in idx for q in (segs[i]["p0"][1], segs[i]["p1"][1])]
    return [min(xs), min(ys), max(xs), max(ys)]


def _ink_centroid(segs, idx):
    sx = sy = w = 0.0
    for i in idx:
        s = segs[i]
        L = s["len"]
        sx += L * (s["p0"][0] + s["p1"][0]) / 2
        sy += L * (s["p0"][1] + s["p1"][1]) / 2
        w += L
    return (sx / w, sy / w) if w > 0 else (0.0, 0.0)


def _unmatched(src, dst_eidx, dst_pidx, tol, mode="strict"):
    """Segments of `src` with no partner in `dst`.

    ``strict``   — both endpoints must coincide (exact decomposition).  This is the
                   right test when the two sides come from the same encoder, and it is
                   the only test that sees a segment sliding ALONG its own line.
    ``tolerant`` — falls back to nearest-parallel point matching, which is needed when
                   the two sides really are packaged differently (`mine` M11: the same
                   drawing as 5 845 vs 12 921 segments) but is blind to a slide along
                   the line and to a shift smaller than the tolerance.
    """
    out = []
    for s in src:
        if _GM.query_endpoints(dst_eidx, s["p0"][0], s["p0"][1], s["p1"][0], s["p1"][1], tol) is not None:
            continue
        if mode == "strict":
            out.append(s)
            continue
        ang = math.degrees(math.atan2(s["p1"][1] - s["p0"][1], s["p1"][0] - s["p0"][0])) % 180.0
        ok = True
        for u in (0.15, 0.5, 0.85):
            x = s["p0"][0] + u * (s["p1"][0] - s["p0"][0])
            y = s["p0"][1] + u * (s["p1"][1] - s["p0"][1])
            if _GM.query(dst_pidx, x, y, ang, tol) is None:
                ok = False
                break
        if not ok:
            out.append(s)
    return out


def ink_changes(segs_a, segs_b, T, S, inter, p, tol=None, max_seg=120000, mode="strict",
                border_pad_pt=0.0):
    """Grouping-independent change evidence: lost ink, new ink, and translated ink.

    ``border_pad_pt`` widens the crop-border guard; each cluster also carries
    ``border_seg`` — True when it contains a segment the FRAME ITSELF cut
    (``v03_foundation`` sets ``border`` on every Liang-Barsky clipped segment).  That
    flag is provenance, not a heuristic: such ink is only partly in the crop, so its
    endpoints are an artefact of where the crop was drawn, not of the drawing.
    """
    tol = tol or max(0.5, 0.05 * S)
    A2 = [{"i": i, "p0": T(s["p0"]), "p1": T(s["p1"]), "len": s["len"] * T.s,
           "bd": bool(s.get("border"))}
          for i, s in enumerate(segs_a)]
    B = [{"i": i, "p0": tuple(s["p0"]), "p1": tuple(s["p1"]), "len": s["len"],
          "bd": bool(s.get("border"))}
         for i, s in enumerate(segs_b)]
    if len(A2) > max_seg or len(B) > max_seg:
        return {"skipped": "too_many_segments", "n_a": len(A2), "n_b": len(B)}
    eB = _GM.build_endpoint_index(B); pB = _GM.build_index(B)
    eA = _GM.build_endpoint_index(A2); pA = _GM.build_index(A2)
    lostA = _unmatched(A2, eB, pB, tol, mode)
    newB = _unmatched(B, eA, pA, tol, mode)
    tot_a = sum(s["len"] for s in A2) or 1.0
    tot_b = sum(s["len"] for s in B) or 1.0
    radius = max(2.0 * S, 2.0)
    ca = _cluster_segments(lostA, radius)
    cb = _cluster_segments(newB, radius)
    pad = 0.0
    Ti = sim_inverse(T)
    A_orig = [{"p0": tuple(s["p0"]), "p1": tuple(s["p1"]), "len": s["len"]} for s in segs_a]
    clus_a = []
    for g in ca:
        idx = [lostA[i]["i"] for i in g]
        bb = _seg_bbox(A2, idx)                     # B frame: used for the border test
        clus_a.append({"idx": idx, "bbox_native": bb, "bbox": _seg_bbox(A_orig, idx),
                       "len": sum(A2[i]["len"] for i in idx),
                       "c": _ink_centroid(A2, idx), "n": len(idx),
                       "inside": _inside(bb, inter, pad),
                       "inside_pad": _inside(bb, inter, border_pad_pt),
                       "margin": _margin(bb, inter),
                       "border_seg": any(A2[i]["bd"] for i in idx)})
    clus_b = []
    for g in cb:
        idx = [newB[i]["i"] for i in g]
        bb = _seg_bbox(B, idx)
        cc = [Ti((bb[0], bb[1])), Ti((bb[2], bb[3]))]
        bba = [min(cc[0][0], cc[1][0]), min(cc[0][1], cc[1][1]),
               max(cc[0][0], cc[1][0]), max(cc[0][1], cc[1][1])]
        clus_b.append({"idx": idx, "bbox_native": bb, "bbox": bba,
                       "len": sum(B[i]["len"] for i in idx),
                       "c": _ink_centroid(B, idx), "n": len(idx),
                       "inside": _inside(bb, inter, pad),
                       "inside_pad": _inside(bb, inter, border_pad_pt),
                       "margin": _margin(bb, inter),
                       "border_seg": any(B[i]["bd"] for i in idx)})
    clus_a.sort(key=lambda c: -c["len"])
    clus_b.sort(key=lambda c: -c["len"])
    # translated-copy test: does an A cluster land on a B cluster under one shift?
    used_b = set()
    for ka, a in enumerate(clus_a[:40]):
        best = None
        for kb, b in enumerate(clus_b[:40]):
            if kb in used_b:
                continue
            lr = max(a["len"] / max(b["len"], 1e-9), b["len"] / max(a["len"], 1e-9))
            if lr > 1.25:
                continue
            dx, dy = b["c"][0] - a["c"][0], b["c"][1] - a["c"][1]
            hit = tot = 0.0
            for i in a["idx"]:
                s = A2[i]
                tot += s["len"]
                if _GM.query_endpoints(eB, s["p0"][0] + dx, s["p0"][1] + dy,
                                       s["p1"][0] + dx, s["p1"][1] + dy, tol) is not None:
                    hit += s["len"]
            sh = hit / max(tot, 1e-9)
            if sh >= 0.90 and (best is None or sh > best[0]):
                best = (sh, kb, dx, dy)
        if best:
            sh, kb, dx, dy = best
            used_b.add(kb)
            # refit the shift on the matched endpoint pairs (the centroid estimate is
            # biased when part of the cluster's ink overlaps ink that did not move)
            ddx = []; ddy = []
            for i in a["idx"]:
                s_ = A2[i]
                bi = _GM.query_endpoints(eB, s_["p0"][0] + dx, s_["p0"][1] + dy,
                                         s_["p1"][0] + dx, s_["p1"][1] + dy, tol)
                if bi is None:
                    continue
                b_ = B[bi]
                if (math.hypot(b_["p0"][0] - s_["p0"][0] - dx, b_["p0"][1] - s_["p0"][1] - dy)
                        <= math.hypot(b_["p1"][0] - s_["p0"][0] - dx, b_["p1"][1] - s_["p0"][1] - dy)):
                    ddx += [b_["p0"][0] - s_["p0"][0], b_["p1"][0] - s_["p1"][0]]
                    ddy += [b_["p0"][1] - s_["p0"][1], b_["p1"][1] - s_["p1"][1]]
                else:
                    ddx += [b_["p1"][0] - s_["p0"][0], b_["p0"][0] - s_["p1"][0]]
                    ddy += [b_["p1"][1] - s_["p0"][1], b_["p0"][1] - s_["p1"][1]]
            if len(ddx) >= 2:
                ddx.sort(); ddy.sort()
                dx = ddx[len(ddx) // 2]
                dy = ddy[len(ddy) // 2]
            ax_, ay_ = rot_apply((-T.theta) % 360, dx / T.s, dy / T.s)
            a["moved"] = {"dx_pt": round(ax_, 4), "dy_pt": round(ay_, 4),
                          "d_pt": round(math.hypot(ax_, ay_), 4), "match_share": round(sh, 4),
                          "dx_native_pt": round(dx, 4), "dy_native_pt": round(dy, 4),
                          "partner_bbox": [round(v, 3) for v in clus_b[kb]["bbox"]]}
            clus_b[kb]["moved_from"] = ka
    def _pack(cl, side):
        return [{"side": side, "bbox": [round(v, 3) for v in c["bbox"]],
                 "bbox_native": [round(v, 3) for v in c["bbox_native"]],
                 "ink_len": round(c["len"], 3), "n_seg": c["n"],
                 "inside_intersection": c["inside"],
                 "inside_pad": c["inside_pad"], "border_seg": c["border_seg"],
                 "margin_pt": round(c["margin"], 3),
                 "moved": c.get("moved"), "moved_from": c.get("moved_from")}
                for c in cl]
    # --- rule v2: a cluster only counts as a finding when it is inside the padded
    # intersection AND contains no ink the frame itself cut.  Measured against v1 in
    # mov_borderfix.py (M4b): v1 reports the sheet-frame line as MOVED_OBJECT.
    def _ok2(c):
        return c["inside_pad"] and not c["border_seg"]
    moved_len_v2 = sum(c["len"] for c in clus_a if c.get("moved") and _ok2(c))
    lost_len_v2 = sum(c["len"] for c in clus_a if not c.get("moved") and _ok2(c))
    new_len_v2 = sum(c["len"] for c in clus_b if c.get("moved_from") is None and _ok2(c))
    border_len_a_v2 = sum(c["len"] for c in clus_a if not _ok2(c))
    border_len_b_v2 = sum(c["len"] for c in clus_b if c.get("moved_from") is None and not _ok2(c))
    moved_len = sum(c["len"] for c in clus_a if c.get("moved"))
    lost_len = sum(c["len"] for c in clus_a if not c.get("moved") and c["inside"])
    new_len = sum(c["len"] for c in clus_b if c.get("moved_from") is None and c["inside"])
    border_a = sum(c["len"] for c in clus_a if not c.get("moved") and not c["inside"])
    border_b = sum(c["len"] for c in clus_b if c.get("moved_from") is None and not c["inside"])
    return {
        "mode": mode, "tol_pt": round(tol, 3), "cluster_radius_pt": round(radius, 3),
        "unmatched_ink_share_a": round(sum(s["len"] for s in lostA) / tot_a, 6),
        "unmatched_ink_share_b": round(sum(s["len"] for s in newB) / tot_b, 6),
        "moved_ink_share_a": round(moved_len / tot_a, 6),
        "lost_ink_share_a": round(lost_len / tot_a, 6),
        "new_ink_share_b": round(new_len / tot_b, 6),
        "border_ink_share_a": round(border_a / tot_a, 6),
        "border_ink_share_b": round(border_b / tot_b, 6),
        "n_border_clusters": (sum(1 for c in clus_a if not c.get("moved") and not c["inside"])
                              + sum(1 for c in clus_b if c.get("moved_from") is None and not c["inside"])),
        "n_lost_clusters": sum(1 for c in clus_a if not c.get("moved") and c["inside"]),
        "n_new_clusters": sum(1 for c in clus_b if c.get("moved_from") is None and c["inside"]),
        "n_clusters_a": len(clus_a), "n_clusters_b": len(clus_b),
        "n_moved_clusters": sum(1 for c in clus_a if c.get("moved")),
        "v2": {
            "moved_ink_share_a": round(moved_len_v2 / tot_a, 6),
            "lost_ink_share_a": round(lost_len_v2 / tot_a, 6),
            "new_ink_share_b": round(new_len_v2 / tot_b, 6),
            "border_ink_share_a": round(border_len_a_v2 / tot_a, 6),
            "border_ink_share_b": round(border_len_b_v2 / tot_b, 6),
            "n_moved_clusters": sum(1 for c in clus_a if c.get("moved") and _ok2(c)),
            "n_lost_clusters": sum(1 for c in clus_a if not c.get("moved") and _ok2(c)),
            "n_new_clusters": sum(1 for c in clus_b if c.get("moved_from") is None and _ok2(c)),
            "n_border_clusters": (sum(1 for c in clus_a if not _ok2(c))
                                  + sum(1 for c in clus_b if c.get("moved_from") is None
                                        and not _ok2(c))),
            "border_pad_pt": round(border_pad_pt, 3),
        },
        "clusters_a": _pack(clus_a[:25], "a"), "clusters_b": _pack(clus_b[:25], "b"),
    }
