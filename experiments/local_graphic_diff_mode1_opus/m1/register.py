"""MODE 1 registration — deterministic alignment of two prepared blocks.

Allowed transform family: uniform scale + translation (+ an optional rotation
that has to *earn* its place).  No free affine warp: an affine that can squeeze
one axis can hide a real geometry change, which is exactly what MODE 1 must
never do.

Estimation is a two-stage classic:

1. **feature voting** — segments are described by (length, orientation) after
   collinear chains are merged, so a line split into three pieces by one CAD
   exporter still matches the same line written as one piece by another.  Every
   pair of same-descriptor segments votes a translation; the Hough peak is the
   coarse offset;
2. **least squares on inliers** — a 3-parameter similarity (s, tx, ty) fitted to
   the correspondences that survived the vote.

Two cheap hypotheses are always evaluated alongside (bbox-anchored identity and
bbox-fit), plus a raster phase-correlation fallback for the case where the two
sides are packed so differently that no descriptor matches.  The hypothesis with
the best matched-ink coverage wins, and everything about it is recorded.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from typing import Any

import numpy as np

from .core import ink_length, rasterize


@dataclass
class Transform:
    scale: float
    theta_deg: float
    tx: float
    ty: float

    def apply(self, segs: np.ndarray) -> np.ndarray:
        if len(segs) == 0:
            return segs
        c = math.cos(math.radians(self.theta_deg)) * self.scale
        s = math.sin(math.radians(self.theta_deg)) * self.scale
        out = np.empty_like(segs)
        out[:, 0] = c * segs[:, 0] - s * segs[:, 1] + self.tx
        out[:, 1] = s * segs[:, 0] + c * segs[:, 1] + self.ty
        out[:, 2] = c * segs[:, 2] - s * segs[:, 3] + self.tx
        out[:, 3] = s * segs[:, 2] + c * segs[:, 3] + self.ty
        return out

    def apply_pts(self, pts: np.ndarray) -> np.ndarray:
        c = math.cos(math.radians(self.theta_deg)) * self.scale
        s = math.sin(math.radians(self.theta_deg)) * self.scale
        out = np.empty_like(pts)
        out[:, 0] = c * pts[:, 0] - s * pts[:, 1] + self.tx
        out[:, 1] = s * pts[:, 0] + c * pts[:, 1] + self.ty
        return out

    def apply_polys(self, groups):
        out = []
        for g in groups:
            if isinstance(g, dict):
                out.append({"polys": [self.apply_pts(np.asarray(p, dtype=np.float64)).astype(np.float32) for p in g["polys"]],
                            "even_odd": g.get("even_odd", True),
                            "clips": [[self.apply_pts(np.asarray(p, dtype=np.float64)).astype(np.float32) for p in c]
                                      for c in (g.get("clips") or [])] or None})
            else:
                out.append(self.apply_pts(np.asarray(g, dtype=np.float64)).astype(np.float32))
        return out

    def inverse(self) -> "Transform":
        s = 1.0 / self.scale
        th = -self.theta_deg
        c = math.cos(math.radians(th)) * s
        sn = math.sin(math.radians(th)) * s
        return Transform(scale=s, theta_deg=th,
                         tx=-(c * self.tx - sn * self.ty),
                         ty=-(sn * self.tx + c * self.ty))

    def as_dict(self) -> dict[str, float]:
        return {k: round(float(v), 5) for k, v in asdict(self).items()}


# --------------------------------------------------------------------------
def merge_chains(segs: np.ndarray, path_id: np.ndarray, angle_eps_deg: float = 1.0) -> np.ndarray:
    """Merge consecutive collinear segments of one path into single segments.

    Kills the "same line, different packing" descriptor mismatch.
    """
    if len(segs) == 0:
        return segs
    out = []
    n = len(segs)
    i = 0
    while i < n:
        x0, y0, x1, y1 = segs[i]
        pid = path_id[i]
        ang = math.degrees(math.atan2(y1 - y0, x1 - x0))
        j = i + 1
        while j < n and path_id[j] == pid:
            a0, b0, a1, b1 = segs[j]
            if abs(a0 - x1) > 1e-4 or abs(b0 - y1) > 1e-4:
                break
            ang2 = math.degrees(math.atan2(b1 - b0, a1 - a0))
            d = abs((ang2 - ang + 180.0) % 360.0 - 180.0)
            if d > angle_eps_deg:
                break
            x1, y1 = a1, b1
            j += 1
        out.append((x0, y0, x1, y1))
        i = j
    return np.asarray(out, dtype=np.float32)


def descriptors(segs: np.ndarray, min_len: float, len_q: float = 0.25, ang_q: float = 1.0):
    if len(segs) == 0:
        return {}, np.zeros((0, 2), dtype=np.float32)
    dx = segs[:, 2] - segs[:, 0]
    dy = segs[:, 3] - segs[:, 1]
    L = np.hypot(dx, dy)
    keep = L >= min_len
    idx = np.nonzero(keep)[0]
    ang = np.degrees(np.arctan2(dy[idx], dx[idx])) % 180.0
    mid = np.stack([(segs[idx, 0] + segs[idx, 2]) / 2.0, (segs[idx, 1] + segs[idx, 3]) / 2.0], axis=1)
    keys: dict[tuple[int, int], list[int]] = {}
    lq = np.round(L[idx] / len_q).astype(np.int64)
    aq = np.round(ang / ang_q).astype(np.int64) % int(round(180.0 / ang_q))
    for k in range(len(idx)):
        keys.setdefault((int(lq[k]), int(aq[k])), []).append(k)
    return keys, mid.astype(np.float32)


def _vote_translation(left_segs, right_segs, min_len, bin_pt=2.0, max_per_key=6, max_votes=400_000):
    kl, ml = descriptors(left_segs, min_len)
    kr, mr = descriptors(right_segs, min_len)
    if not kl or not kr:
        return None, 0
    votes_x: list[float] = []
    votes_y: list[float] = []
    common = [k for k in kl.keys() if k in kr]
    common.sort(key=lambda k: len(kl[k]) * len(kr[k]))
    total = 0
    for k in common:
        li, ri = kl[k][:max_per_key], kr[k][:max_per_key]
        for a in li:
            for b in ri:
                votes_x.append(float(mr[b, 0] - ml[a, 0]))
                votes_y.append(float(mr[b, 1] - ml[a, 1]))
                total += 1
        if total > max_votes:
            break
    if not votes_x:
        return None, 0
    vx = np.asarray(votes_x)
    vy = np.asarray(votes_y)
    bx = np.round(vx / bin_pt).astype(np.int64)
    by = np.round(vy / bin_pt).astype(np.int64)
    keys, counts = np.unique(np.stack([bx, by], axis=1), axis=0, return_counts=True)
    best = int(np.argmax(counts))
    cx, cy = keys[best]
    sel = (np.abs(bx - cx) <= 1) & (np.abs(by - cy) <= 1)
    return (float(np.median(vx[sel])), float(np.median(vy[sel]))), int(sel.sum())


def _correspondences(left_segs, right_segs, t: Transform, min_len: float, tol: float, max_keys: int = 20000):
    """Nearest same-descriptor midpoint correspondences under transform t."""
    from scipy.spatial import cKDTree

    kl, ml = descriptors(left_segs, min_len)
    kr, mr = descriptors(right_segs, min_len)
    if not kl or not kr or len(mr) == 0:
        return np.zeros((0, 2), np.float32), np.zeros((0, 2), np.float32)
    mlt = t.apply_pts(ml.astype(np.float64))
    tree = cKDTree(mr)
    src, dst = [], []
    used = set()
    done = 0
    for key, lidx in kl.items():
        if done >= max_keys:
            break
        ridx = kr.get(key)
        if not ridx:
            continue
        rset = set(ridx)
        for a in lidx:
            cand = tree.query_ball_point(mlt[a], tol)
            best, bestd = None, 1e18
            for b in cand:
                if b not in rset or b in used:
                    continue
                d = float(np.hypot(mr[b, 0] - mlt[a, 0], mr[b, 1] - mlt[a, 1]))
                if d < bestd:
                    best, bestd = b, d
            done += 1
            if best is not None:
                used.add(best)
                src.append(ml[a])
                dst.append(mr[best])
    return np.asarray(src, np.float32).reshape(-1, 2), np.asarray(dst, np.float32).reshape(-1, 2)


def _fit_similarity(src: np.ndarray, dst: np.ndarray, allow_rotation: bool) -> Transform | None:
    if len(src) < 3:
        return None
    mu_s = src.mean(axis=0)
    mu_d = dst.mean(axis=0)
    a = src - mu_s
    b = dst - mu_d
    var = float((a ** 2).sum())
    if var < 1e-9:
        return None
    if allow_rotation:
        num_c = float((a[:, 0] * b[:, 0] + a[:, 1] * b[:, 1]).sum())
        num_s = float((a[:, 0] * b[:, 1] - a[:, 1] * b[:, 0]).sum())
        theta = math.degrees(math.atan2(num_s, num_c))
        scale = math.hypot(num_c, num_s) / var
    else:
        theta = 0.0
        scale = float((a * b).sum() / var)
    if not (0.2 < scale < 5.0):
        return None
    c = math.cos(math.radians(theta)) * scale
    s = math.sin(math.radians(theta)) * scale
    tx = mu_d[0] - (c * mu_s[0] - s * mu_s[1])
    ty = mu_d[1] - (s * mu_s[0] + c * mu_s[1])
    return Transform(scale=scale, theta_deg=theta, tx=float(tx), ty=float(ty))


# --------------------------------------------------------------------------
_R_CACHE: dict[tuple, tuple] = {}


def coverage(left_segs, right_segs, t: Transform, frame, cell_pt, tol_pt,
             left_fills=None, right_fills=None, cache_key=None) -> dict[str, float]:
    """Matched ink coverage of the two sides in the common physical frame."""
    import cv2

    lt = t.apply(left_segs)
    L = rasterize(lt, None, frame, cell_pt, fills=t.apply_polys(left_fills) if left_fills else None)
    k = max(1, int(round(tol_pt / cell_pt)))
    ker = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * k + 1, 2 * k + 1))
    hit = _R_CACHE.get(cache_key) if cache_key else None
    if hit is None:
        R = rasterize(right_segs, None, frame, cell_pt, fills=right_fills)
        Rd = cv2.dilate(R, ker)
        if cache_key:
            _R_CACHE.clear()
            _R_CACHE[cache_key] = (R, Rd)
    else:
        R, Rd = hit
    Ld = cv2.dilate(L, ker)
    nl, nr = int(L.sum()), int(R.sum())
    ml = int((L & Rd).sum())
    mr = int((R & Ld).sum())
    return {
        "left_ink_cells": nl,
        "right_ink_cells": nr,
        "left_matched": ml,
        "right_matched": mr,
        "left_cov": ml / nl if nl else 0.0,
        "right_cov": mr / nr if nr else 0.0,
        "sym_cov": (ml + mr) / (nl + nr) if (nl + nr) else 0.0,
    }


def _residual(src, dst, t: Transform) -> dict[str, float]:
    if len(src) == 0:
        return {"n": 0, "median": None, "p90": None}
    p = t.apply_pts(src.astype(np.float64))
    d = np.hypot(p[:, 0] - dst[:, 0], p[:, 1] - dst[:, 1])
    return {"n": int(len(d)), "median": float(np.median(d)), "p90": float(np.percentile(d, 90))}


def register(
    left: dict[str, Any],
    right: dict[str, Any],
    cell_pt: float = 0.6,
    tol_pt: float = 1.2,
    allow_rotation: bool = False,
    min_len_frac: float = 0.004,
) -> dict[str, Any]:
    """Estimate the placement transform LEFT -> RIGHT."""
    ls = merge_chains(left["segments"], left["path_id"])
    rs = merge_chains(right["segments"], right["path_id"])
    lf = left.get("fills") or []
    rf = right.get("fills") or []
    lb, rb = left["bbox_vis"], right["bbox_vis"]
    lw, lh = lb[2] - lb[0], lb[3] - lb[1]
    rw, rh = rb[2] - rb[0], rb[3] - rb[1]
    diag = math.hypot(rw, rh)
    min_len = max(1.5, min_len_frac * diag)

    frame = [rb[0], rb[1], rb[2], rb[3]]
    hyps: list[tuple[str, Transform]] = []

    # H0 — bbox-anchored identity (same scale, corner to corner)
    hyps.append(("bbox_anchor", Transform(1.0, 0.0, rb[0] - lb[0], rb[1] - lb[1])))
    # H1 — bbox fit (uniform scale from the size ratio, centres aligned)
    s = min(rw / lw, rh / lh) if lw > 0 and lh > 0 else 1.0
    cxl, cyl = (lb[0] + lb[2]) / 2, (lb[1] + lb[3]) / 2
    cxr, cyr = (rb[0] + rb[2]) / 2, (rb[1] + rb[3]) / 2
    hyps.append(("bbox_fit", Transform(s, 0.0, cxr - s * cxl, cyr - s * cyl)))

    # H2 — feature voting at scale 1 in the bbox-anchored frame
    anchor = Transform(1.0, 0.0, rb[0] - lb[0], rb[1] - lb[1])
    ls_anchor = anchor.apply(ls)
    vote, nvotes = _vote_translation(ls_anchor, rs, min_len)
    if vote is not None:
        hyps.append(("vote", Transform(1.0, 0.0, anchor.tx + vote[0], anchor.ty + vote[1])))

    # H3 — feature voting after bbox-fit scaling (different scale between sides)
    if abs(s - 1.0) > 0.02:
        pre = hyps[1][1]
        vote2, _ = _vote_translation(pre.apply(ls), rs, min_len * s)
        if vote2 is not None:
            hyps.append(("vote_scaled", Transform(pre.scale, 0.0, pre.tx + vote2[0], pre.ty + vote2[1])))

    # H4 — raster phase correlation fallback (packing-independent)
    try:
        import cv2

        A = rasterize(anchor.apply(ls), None, frame, cell_pt).astype(np.float32)
        B = rasterize(rs, None, frame, cell_pt).astype(np.float32)
        if A.sum() > 20 and B.sum() > 20:
            win = cv2.createHanningWindow((A.shape[1], A.shape[0]), cv2.CV_32F)
            (dx, dy), resp = cv2.phaseCorrelate(A * win, B * win)
            hyps.append(("phase_corr", Transform(1.0, 0.0, anchor.tx + dx * cell_pt, anchor.ty + dy * cell_pt)))
    except Exception:
        pass

    if allow_rotation:
        for deg in (90.0, 180.0, 270.0):
            th = math.radians(deg)
            c, sn = math.cos(th), math.sin(th)
            tx = cxr - (c * cxl - sn * cyl)
            ty = cyr - (sn * cxl + c * cyl)
            base = Transform(1.0, deg, tx, ty)
            v, _ = _vote_translation(base.apply(ls), rs, min_len)
            if v is not None:
                hyps.append((f"rot{int(deg)}", Transform(1.0, deg, base.tx + v[0], base.ty + v[1])))
            else:
                hyps.append((f"rot{int(deg)}", base))

    # evaluate + refine
    ck = (id(rs), tuple(round(v, 3) for v in frame), cell_pt, tol_pt)
    best = None
    trace = []
    for name, t in hyps:
        cov = coverage(ls, rs, t, frame, cell_pt, tol_pt, lf, rf, cache_key=ck)
        src, dst = _correspondences(ls, rs, t, min_len, tol_pt * 2.5)
        ref = _fit_similarity(src, dst, allow_rotation=allow_rotation and name.startswith("rot") is False)
        cand = [(name, t, cov)]
        if ref is not None and abs(ref.scale - t.scale) < 0.15 and abs(ref.theta_deg - t.theta_deg) < 3.0:
            cov2 = coverage(ls, rs, ref, frame, cell_pt, tol_pt, lf, rf, cache_key=ck)
            cand.append((name + "+lsq", ref, cov2))
        for nm, tt, cv_ in cand:
            trace.append({"hypothesis": nm, "transform": tt.as_dict(), "sym_cov": round(cv_["sym_cov"], 4)})
            if best is None or cv_["sym_cov"] > best[2]["sym_cov"] + 1e-6:
                best = (nm, tt, cv_)

    name, t, cov = best
    src, dst = _correspondences(ls, rs, t, min_len, tol_pt * 2.5)
    res = _residual(src, dst, t)
    anchors_left = int(len(descriptors(ls, min_len)[1]))
    anchors_right = int(len(descriptors(rs, min_len)[1]))
    anchor_cov = res["n"] / max(1, min(anchors_left, anchors_right))

    ok = cov["sym_cov"] >= 0.5 and (res["median"] is None or res["median"] <= tol_pt * 2)
    reason = None
    if not ok:
        if cov["sym_cov"] < 0.5:
            reason = "LOW_MATCHED_INK"
        else:
            reason = "HIGH_RESIDUAL"
    if min(len(ls), len(rs)) < 5:
        ok, reason = False, "TOO_FEW_PRIMITIVES"

    conf = min(1.0, cov["sym_cov"]) * (1.0 if res["median"] is None else max(0.0, 1.0 - res["median"] / (tol_pt * 3)))
    return {
        "method": name,
        "transform": t.as_dict(),
        "frame": [round(v, 3) for v in frame],
        "cell_pt": cell_pt,
        "tol_pt": tol_pt,
        "coverage": {k: (round(v, 5) if isinstance(v, float) else v) for k, v in cov.items()},
        "residual": {k: (round(v, 4) if isinstance(v, float) else v) for k, v in res.items()},
        "anchors": {
            "left_candidates": anchors_left,
            "right_candidates": anchors_right,
            "matched": res["n"],
            "coverage": round(anchor_cov, 4),
            "min_len_pt": round(min_len, 3),
            "votes": nvotes,
        },
        "success": bool(ok),
        "failure_reason": reason,
        "confidence": round(float(conf), 4),
        "hypotheses": trace,
        "left_segments_merged": int(len(ls)),
        "right_segments_merged": int(len(rs)),
        "left_ink_pt": round(ink_length(ls), 2),
        "right_ink_pt": round(ink_length(rs), 2),
        "_t": t,
        "_ls": ls,
        "_rs": rs,
        "_lf": lf,
        "_rf": rf,
    }
